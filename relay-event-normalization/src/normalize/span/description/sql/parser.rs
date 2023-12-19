//! Logic for parsing SQL queries and manipulating the resulting Abstract Syntax Tree.
use std::borrow::Cow;
use std::ops::ControlFlow;

use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast::{
    AlterTableOperation, Assignment, CloseCursor, ColumnDef, Expr, Ident, ObjectName, Query,
    Select, SelectItem, SetExpr, Statement, TableAlias, TableConstraint, TableFactor,
    UnaryOperator, Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::{Dialect, GenericDialect};

/// Keeps track of the maximum depth of an SQL expression that the [`NormalizeVisitor`] encounters.
///
/// This is used to prevent the serialization of the AST from crashing.
/// Note that the expression depth does not fully cover the depth of complex SQL statements,
/// because not everything in SQL is an expression.
const MAX_EXPRESSION_DEPTH: usize = 64;

/// Regex used to scrub hex IDs and multi-digit numbers from table names and other identifiers.
static TABLE_NAME_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)[0-9a-f]{8,}|\d\d+").unwrap());

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
    let mut visitor = NormalizeVisitor::new();
    parsed.visit(&mut visitor);

    let concatenated = parsed
        .iter()
        .map(|statement| statement.to_string())
        .join("; ");

    // Insert placeholders that the SQL serializer cannot provide.
    let replaced = concatenated.replace("___UPDATE_LHS___ = NULL", "..");

    Ok(replaced)
}

/// A visitor that normalizes the SQL AST in-place.
///
/// Used for span description normalization.
struct NormalizeVisitor {
    /// The current depth of an expression.
    current_expr_depth: usize,
}

impl NormalizeVisitor {
    pub fn new() -> Self {
        Self {
            current_expr_depth: 0,
        }
    }

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
            _ => false,
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
    fn transform_query(&mut self, query: &mut Query) {
        if let SetExpr::Select(select) = &mut *query.body {
            self.transform_select(&mut *select);
        }
    }

    fn transform_select(&mut self, select: &mut Select) {
        // Track collapsible selected items (e.g. `SELECT col1, col2`).
        let mut collapse = vec![];

        // Iterate over selected item.
        for item in std::mem::take(&mut select.projection) {
            // Normalize aliases.
            let item = match item {
                // Remove alias.
                SelectItem::ExprWithAlias { expr, .. } => SelectItem::UnnamedExpr(expr),
                // Strip prefix, e.g. `"mytable".*`.
                SelectItem::QualifiedWildcard(_, options) => SelectItem::Wildcard(options),
                _ => item,
            };
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

    fn simplify_table_alias(alias: &mut Option<TableAlias>) {
        if let Some(TableAlias { name, columns }) = alias {
            Self::scrub_name(name);
            for column in columns {
                Self::scrub_name(column);
            }
        }
    }

    fn simplify_compound_identifier(parts: &mut Vec<Ident>) {
        if let Some(mut last) = parts.pop() {
            Self::scrub_name(&mut last);
            *parts = vec![last];
        }
    }

    fn scrub_name(name: &mut Ident) {
        name.quote_style = None;
        if let Cow::Owned(s) = TABLE_NAME_REGEX.replace_all(&name.value, "{%s}") {
            name.value = s
        };
    }

    fn erase_name(name: &mut Ident) {
        name.quote_style = None;
        name.value = "%s".into()
    }
}

impl VisitorMut for NormalizeVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        Self::simplify_compound_identifier(&mut relation.0);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        match table_factor {
            TableFactor::Table { alias, .. } => {
                Self::simplify_table_alias(alias);
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                self.transform_query(subquery);
                Self::simplify_table_alias(alias);
            }
            TableFactor::TableFunction { alias, .. } => {
                Self::simplify_table_alias(alias);
            }
            TableFactor::UNNEST {
                alias,
                with_offset_alias,
                ..
            } => {
                Self::simplify_table_alias(alias);
                if let Some(ident) = with_offset_alias {
                    Self::scrub_name(ident);
                }
            }
            TableFactor::NestedJoin { alias, .. } => {
                Self::simplify_table_alias(alias);
            }
            TableFactor::Pivot {
                table_alias,
                pivot_alias,
                ..
            } => {
                Self::simplify_table_alias(table_alias);
                Self::simplify_table_alias(pivot_alias);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if self.current_expr_depth > MAX_EXPRESSION_DEPTH {
            *expr = Expr::Value(Value::Placeholder("..".to_owned()));
            return ControlFlow::Continue(());
        }
        self.current_expr_depth += 1;
        match expr {
            // Simple values like numbers and strings are replaced by a placeholder:
            Expr::Value(x) => *x = Self::placeholder(),
            // `IN (val1, val2, val3)` is replaced by `IN (%s)`.
            Expr::InList { list, .. } => *list = vec![Expr::Value(Self::placeholder())],
            // `"table"."col"` is replaced by `col`.
            Expr::CompoundIdentifier(parts) => {
                Self::simplify_compound_identifier(parts);
            }
            // `"col"` is replaced by `col`.
            Expr::Identifier(ident) => {
                Self::scrub_name(ident);
            }
            // Recurse into subqueries.
            Expr::Subquery(query) => self.transform_query(query),
            // Remove negative sign, e.g. `-100` becomes `%s`.
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => {
                if let Expr::Value(_) = **inner {
                    *expr = Expr::Value(Self::placeholder())
                }
            }
            // Simplify `CASE WHEN..` expressions.
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                operand.take();
                *conditions = vec![Expr::Identifier(Self::ellipsis())];
                *results = vec![Expr::Identifier(Self::ellipsis())];
                else_result.take();
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Casts are omitted for simplification. Because we replace the entire expression,
        // the replacement has to occur *after* visiting its children.
        if let Expr::Cast { expr: inner, .. } = expr {
            let mut swapped = Expr::Value(Value::Null);
            std::mem::swap(&mut swapped, inner);
            *expr = swapped;
        }

        self.current_expr_depth = self.current_expr_depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(query) => {
                self.transform_query(query);
            }
            // `INSERT INTO col1, col2 VALUES (val1, val2)` becomes `INSERT INTO .. VALUES (%s)`.
            Statement::Insert {
                columns, source, ..
            } => {
                *columns = vec![Self::ellipsis()];
                if let SetExpr::Values(v) = &mut *source.body {
                    v.rows = vec![vec![Expr::Value(Self::placeholder())]]
                }
            }
            // Simple lists of col = value assignments are collapsed to `..`.
            Statement::Update { assignments, .. } => {
                if assignments.len() > 1
                    && assignments
                        .iter()
                        .all(|a| matches!(a.value, Expr::Value(_)))
                {
                    *assignments = vec![Assignment {
                        id: vec![Ident::new("___UPDATE_LHS___")],
                        value: Expr::Value(Value::Null),
                    }]
                } else {
                    for assignment in assignments.iter_mut() {
                        Self::simplify_compound_identifier(&mut assignment.id);
                    }
                }
            }
            // `SAVEPOINT foo` becomes `SAVEPOINT %s`.
            Statement::Savepoint { name } => Self::erase_name(name),
            Statement::Declare { name, query, .. } => {
                Self::erase_name(name);
                self.transform_query(query);
            }
            Statement::Fetch { name, into, .. } => {
                Self::erase_name(name);
                if let Some(into) = into {
                    into.0 = vec![Ident {
                        value: "%s".into(),
                        quote_style: None,
                    }];
                }
            }
            Statement::Close {
                cursor: CloseCursor::Specific { name },
            } => Self::erase_name(name),
            Statement::AlterTable { name, operation } => {
                Self::simplify_compound_identifier(&mut name.0);
                match operation {
                    AlterTableOperation::AddConstraint(c) => match c {
                        TableConstraint::Unique { name, columns, .. } => {
                            if let Some(name) = name {
                                Self::scrub_name(name);
                            }
                            for column in columns {
                                Self::scrub_name(column);
                            }
                        }
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            referred_columns,
                            ..
                        } => {
                            if let Some(name) = name {
                                Self::scrub_name(name);
                            }
                            for column in columns {
                                Self::scrub_name(column);
                            }
                            for column in referred_columns {
                                Self::scrub_name(column);
                            }
                        }
                        TableConstraint::Check { name, .. } => {
                            if let Some(name) = name {
                                Self::scrub_name(name);
                            }
                        }
                        TableConstraint::Index { name, columns, .. } => {
                            if let Some(name) = name {
                                Self::scrub_name(name);
                            }
                            for column in columns {
                                Self::scrub_name(column);
                            }
                        }
                        TableConstraint::FulltextOrSpatial {
                            opt_index_name,
                            columns,
                            ..
                        } => {
                            if let Some(name) = opt_index_name {
                                Self::scrub_name(name);
                            }
                            for column in columns {
                                Self::scrub_name(column);
                            }
                        }
                    },
                    AlterTableOperation::AddColumn { column_def, .. } => {
                        let ColumnDef { name, .. } = column_def;
                        Self::scrub_name(name);
                    }
                    AlterTableOperation::DropConstraint { name, .. } => Self::scrub_name(name),
                    AlterTableOperation::DropColumn { column_name, .. } => {
                        Self::scrub_name(column_name)
                    }
                    AlterTableOperation::RenameColumn {
                        old_column_name,
                        new_column_name,
                    } => {
                        Self::scrub_name(old_column_name);
                        Self::scrub_name(new_column_name);
                    }
                    AlterTableOperation::ChangeColumn {
                        old_name, new_name, ..
                    } => {
                        Self::scrub_name(old_name);
                        Self::scrub_name(new_name);
                    }
                    AlterTableOperation::RenameConstraint { old_name, new_name } => {
                        Self::scrub_name(old_name);
                        Self::scrub_name(new_name);
                    }
                    AlterTableOperation::AlterColumn { column_name, .. } => {
                        Self::scrub_name(column_name);
                    }
                    _ => {}
                }
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
    fn dialect(&self) -> std::any::TypeId {
        self.0.dialect()
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_deep_expression() {
        let query = "SELECT 1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1";
        assert_eq!(normalize_parsed_queries(None, query).as_deref(), Ok("SELECT .. + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s"));
    }
}
