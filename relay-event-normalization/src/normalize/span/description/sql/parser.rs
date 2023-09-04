//! Logic for parsing SQL queries and manipulating the resulting Abstract Syntax Tree.
use std::ops::ControlFlow;

use itertools::Itertools;
use sqlparser::ast::{self, Expr, Ident, Statement, Value, VisitMut, VisitorMut}; // TODO: no self
use sqlparser::dialect::{Dialect, GenericDialect};

/// Used to replace placeholders (parameters) by dummy values such that the query can be parsed.
// static SQL_PLACEHOLDER_REGEX: Lazy<Regex> =
//     Lazy::new(|| Regex::new(r"(?:\?+|\$\d+|%(?:\(\w+\))?s|:\w+)").unwrap());

/// Derive the SQL dialect from `span.system` and try to parse the query into an AST.
pub fn parse_query(
    db_system: Option<&str>,
    query: &str,
) -> Result<Vec<ast::Statement>, sqlparser::parser::ParserError> {
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
    fn placeholder() -> Value {
        Value::Number("%s".into(), false)
    }

    fn transform_query(query: &mut ast::Query) {
        if let ast::SetExpr::Select(select) = &mut *query.body {
            let mut collapse = vec![];

            // Iterate projection (the thing that's being selected).
            for mut item in std::mem::take(&mut select.projection) {
                if match &mut item {
                    ast::SelectItem::UnnamedExpr(expr) => {
                        matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                    }
                    ast::SelectItem::ExprWithAlias { expr, alias } => {
                        alias.quote_style = None;
                        matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                    }
                    _ => false,
                } {
                    collapse.push(item);
                } else {
                    match collapse.len() {
                        0 => {}
                        1 => {
                            select.projection.append(&mut collapse);
                        }
                        _ => select
                            .projection
                            .push(ast::SelectItem::UnnamedExpr(Expr::Value(Value::Number(
                                "..".into(),
                                false,
                            )))),
                    }
                    collapse.clear();
                    select.projection.push(item);
                }
            }
            match collapse.len() {
                0 => {}
                1 => {
                    select.projection.append(&mut collapse);
                }
                _ => select
                    .projection
                    .push(ast::SelectItem::UnnamedExpr(Expr::Value(Value::Number(
                        "..".into(),
                        false,
                    )))),
            }

            // Iterate "FROM"
            for from in select.from.iter_mut() {
                match &mut from.relation {
                    ast::TableFactor::Derived { subquery, .. } => {
                        Self::transform_query(subquery);
                    }
                    ast::TableFactor::Table { name, .. } => {
                        for ident in &mut name.0 {
                            ident.quote_style = None;
                        }
                    }
                    _ => (),
                }
            }
        }
    }
}

impl VisitorMut for NormalizeVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::UnaryOp { op: _, expr: inner } => {
                if matches!(inner.as_ref(), Expr::Value(_)) {
                    *expr = Expr::Value(Self::placeholder());
                }
            }
            Expr::Value(x) => *x = Self::placeholder(),
            Expr::Cast { expr: inner, .. } => {
                // Discard casts.
                *expr = *inner.clone(); // TODO: without clone?
            }
            Expr::InList { list, .. } => *list = vec![Expr::Value(Self::placeholder())],
            Expr::CompoundIdentifier(parts) => {
                if let Some(mut last) = parts.pop() {
                    last.quote_style = None;
                    *parts = vec![last];
                }
            }
            Expr::Identifier(ident) => {
                ident.quote_style = None;
            }
            Expr::Subquery(query) => Self::transform_query(query),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    // fn pre_visit_statement(&mut self, statement: &mut ast::Statement) -> ControlFlow<Self::Break> {
    //     match statement {};
    //     ControlFlow::Continue(())
    // }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            ast::Statement::Query(query) => {
                Self::transform_query(query);
            }
            Statement::Insert {
                columns, source, ..
            } => {
                *columns = vec![Ident::new("..")];
                if let ast::SetExpr::Values(v) = &mut *source.body {
                    v.rows = vec![vec![Expr::Value(Self::placeholder())]]
                }
            }
            Statement::Savepoint { name } => {
                name.quote_style = None;
                name.value = "%s".into()
            } // TODO: placeholder constant
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
