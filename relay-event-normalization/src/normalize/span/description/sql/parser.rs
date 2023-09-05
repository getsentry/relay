//! Logic for parsing SQL queries and manipulating the resulting Abstract Syntax Tree.
use std::ops::ControlFlow;

use itertools::Itertools;
use sqlparser::ast::{
    Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    UnaryOperator, Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::{Dialect, GenericDialect};

/// Derive the SQL dialect from `db_system` (the value obtained from `span.data.system`)
/// and try to parse the query into an AST.
pub fn parse_query(
    db_system: Option<&str>,
    query: &str,
) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
    // See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/database.md#notes-and-well-known-identifiers-for-dbsystem
    //     https://docs.rs/sqlparser/latest/sqlparser/dialect/fn.dialect_from_str.html
    let dialect = db_system
        .and_then(sqlparser::dialect::dialect_from_str)
        .unwrap_or_else(|| Box::new(GenericDialect {}));
    let dialect = DialectWithParameters(dialect);

    sqlparser::parser::Parser::parse_sql(&dialect, query)
}

/// Tries to parse a series of SQL queries into an AST and normalize it.
pub fn normalize_parsed_queries(db_system: Option<&str>, string: &str) -> Result<String, ()> {
    let mut parsed = parse_query(db_system, string).map_err(|_| ())?;
    parsed.visit(&mut NormalizeVisitor);

    Ok(parsed
        .iter()
        .map(|statement| statement.to_string())
        .join("; "))
}

/// A visitor that normalizes the SQL AST in-place.
///
/// Used for span description normalization.
pub struct NormalizeVisitor;

impl NormalizeVisitor {
    /// Placeholder for string and numerical literals.
    fn placeholder() -> Value {
        Value::Number("%s".into(), false)
    }

    /// Placeholder for a list of items that has been scrubbed.
    fn ellipsis() -> Ident {
        Ident::new("..")
    }

    /// Check if a selected item can be erased into `..`. Currently we only collapse simple
    /// columns (`col1` or `col1 AS foo`).
    fn is_collapsible(item: &SelectItem) -> bool {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            }
            SelectItem::QualifiedWildcard(_, _) => todo!(),
            SelectItem::Wildcard(_) => todo!(),
            // _ => false,
        }
    }

    /// Helper function to collapse a sequence of selected items into `..`.
    fn collapse_items(collapse: &mut Vec<SelectItem>, output: &mut Vec<SelectItem>) {
        match collapse.len() {
            0 => {}
            1 => {
                output.append(collapse);
            }
            _ => {
                output.push(SelectItem::UnnamedExpr(Expr::Identifier(Self::ellipsis())));
            }
        }
    }

    /// Normalizes `SELECT ...` queries.
    fn transform_query(query: &mut Query) {
        if let SetExpr::Select(select) = &mut *query.body {
            Self::transform_select(&mut *select);
            Self::transform_from(&mut select.from);
        }
    }

    fn transform_select(select: &mut Select) {
        // Track collapsible selected items (e.g. `SELECT col1, col2`).
        let mut collapse = vec![];

        // Iterate over selected item.
        for mut item in std::mem::take(&mut select.projection) {
            // Normalize aliases.
            if let SelectItem::ExprWithAlias { ref mut alias, .. } = &mut item {
                alias.quote_style = None;
            }
            if Self::is_collapsible(&item) {
                collapse.push(item);
            } else {
                Self::collapse_items(&mut collapse, &mut select.projection);
                collapse.clear();
                select.projection.push(item);
            }
        }
        Self::collapse_items(&mut collapse, &mut select.projection);
    }

    fn transform_from(from: &mut [TableWithJoins]) {
        // Iterate "FROM".
        for from in from {
            match &mut from.relation {
                // Recurse into subqueries.
                TableFactor::Derived { subquery, .. } => {
                    Self::transform_query(subquery);
                }
                // Strip quotes from identifiers in table names.
                TableFactor::Table { name, .. } => {
                    for ident in &mut name.0 {
                        ident.quote_style = None;
                    }
                }
                _ => (),
            }
        }
    }

    fn simplify_compound_identifier(parts: &mut Vec<Ident>) {
        if let Some(mut last) = parts.pop() {
            last.quote_style = None;
            *parts = vec![last];
        }
    }
}

impl VisitorMut for NormalizeVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            // Simple values like numbers and strings are replaced by a placeholder:
            Expr::Value(x) => *x = Self::placeholder(),
            // Casts are omitted for simplification.
            Expr::Cast { expr: inner, .. } => {
                *expr = *inner.clone(); // clone is unfortunate here.
            }
            // `IN (val1, val2, val3)` is replaced by `IN (%s)`.
            Expr::InList { list, .. } => *list = vec![Expr::Value(Self::placeholder())],
            // `"table"."col"` is replaced by `col`.
            Expr::CompoundIdentifier(parts) => {
                Self::simplify_compound_identifier(parts);
            }
            // `"col"` is replaced by `col`.
            Expr::Identifier(ident) => {
                ident.quote_style = None;
            }
            // Recurse into subqueries.
            Expr::Subquery(query) => Self::transform_query(query),
            // Remove negative sign, e.g. `-100` becomes `%s`.
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => {
                if let Expr::Value(_) = **inner {
                    *expr = Expr::Value(Self::placeholder())
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(query) => {
                Self::transform_query(query);
            }
            // `INSERT INTO col1, col2 VALUES (val1, val2)` becomes `INSERT INTO .. VALUES (%s)`.
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                Self::simplify_compound_identifier(&mut table_name.0);
                *columns = vec![Self::ellipsis()];
                if let SetExpr::Values(v) = &mut *source.body {
                    v.rows = vec![vec![Expr::Value(Self::placeholder())]]
                }
            }
            // `UPDATE "foo"."bar"` becomes `UPDATE bar`.
            Statement::Update { assignments, .. } => {
                for assignment in assignments.iter_mut() {
                    Self::simplify_compound_identifier(&mut assignment.id);
                }
            }
            // `SAVEPOINT foo` becomes `SAVEPOINT %s`.
            Statement::Savepoint { name } => {
                name.quote_style = None;
                name.value = "%s".into()
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// An extension of an SQL dialect that accepts `?`, `%s`, `:c0` as valid input.
#[derive(Debug)]
struct DialectWithParameters(Box<dyn Dialect>);

impl DialectWithParameters {
    const PARAMETERS: &'static str = "?%:";
}

impl Dialect for DialectWithParameters {
    fn is_identifier_start(&self, ch: char) -> bool {
        Self::PARAMETERS.contains(ch) || self.0.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.0.is_identifier_part(ch)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.0.is_delimited_identifier_start(ch)
    }

    fn is_proper_identifier_inside_quotes(
        &self,
        chars: std::iter::Peekable<std::str::Chars<'_>>,
    ) -> bool {
        self.0.is_proper_identifier_inside_quotes(chars)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        self.0.supports_filter_during_aggregation()
    }

    fn supports_within_after_array_aggregation(&self) -> bool {
        self.0.supports_within_after_array_aggregation()
    }

    fn supports_group_by_expr(&self) -> bool {
        self.0.supports_group_by_expr()
    }

    fn supports_substring_from_for_expr(&self) -> bool {
        self.0.supports_substring_from_for_expr()
    }

    fn parse_prefix(
        &self,
        parser: &mut sqlparser::parser::Parser,
    ) -> Option<Result<Expr, sqlparser::parser::ParserError>> {
        self.0.parse_prefix(parser)
    }

    fn parse_infix(
        &self,
        parser: &mut sqlparser::parser::Parser,
        expr: &Expr,
        precedence: u8,
    ) -> Option<Result<Expr, sqlparser::parser::ParserError>> {
        self.0.parse_infix(parser, expr, precedence)
    }

    fn get_next_precedence(
        &self,
        parser: &sqlparser::parser::Parser,
    ) -> Option<Result<u8, sqlparser::parser::ParserError>> {
        self.0.get_next_precedence(parser)
    }

    fn parse_statement(
        &self,
        parser: &mut sqlparser::parser::Parser,
    ) -> Option<Result<Statement, sqlparser::parser::ParserError>> {
        self.0.parse_statement(parser)
    }
}
