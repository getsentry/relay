//! Logic for parsing SQL queries and manipulating the resulting Abstract Syntax Tree.
use std::borrow::Cow;
use std::ops::ControlFlow;

use itertools::Itertools;
use sqlparser::ast::{
    AlterTableOperation, Assignment, AssignmentTarget, BinaryOperator, CaseWhen, CloseCursor,
    ColumnDef, CopySource, CreateIndex, Declare, Expr, ExprWithAlias, FunctionArg, GranteeName,
    Ident, Insert, JsonTableColumn, JsonTableNamedColumn, JsonTableNestedColumn, LockTable,
    ObjectName, ObjectNamePart, ObjectNamePartFunction, Query, Select, SelectItem, SetExpr,
    ShowStatementFilter, Statement, TableAlias, TableAliasColumnDef, TableConstraint, TableFactor,
    UnaryOperator, Use, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::tokenizer::Span;

use crate::span::TABLE_NAME_REGEX;

/// Keeps track of the maximum depth of an SQL expression that the [`NormalizeVisitor`] encounters.
///
/// This is used to prevent the serialization of the AST from crashing.
/// Note that the expression depth does not fully cover the depth of complex SQL statements,
/// because not everything in SQL is an expression.
const MAX_EXPRESSION_DEPTH: usize = 64;

/// Derive the SQL dialect from `db_system` (the value obtained from `span.data.system`)
/// and try to parse the query into an AST.
pub fn parse_query(
    db_system: Option<&str>,
    query: &str,
) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
    relay_log::with_scope(
        |scope| {
            scope.set_tag("db_system", db_system.unwrap_or_default());
            scope.set_extra("query", query.into());
        },
        || match std::panic::catch_unwind(|| parse_query_inner(db_system, query)) {
            Ok(res) => res,
            Err(_) => Err(sqlparser::parser::ParserError::ParserError(
                "panicked".to_owned(),
            )),
        },
    )
}

fn parse_query_inner(
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
pub fn normalize_parsed_queries(
    db_system: Option<&str>,
    string: &str,
) -> Result<(String, Vec<Statement>), ()> {
    let mut parsed = parse_query(db_system, string).map_err(|_| ())?;
    let _ = parsed.visit(&mut NormalizeVisitor);
    let _ = parsed.visit(&mut MaxDepthVisitor::new());

    let concatenated = parsed
        .iter()
        .map(|statement| statement.to_string())
        .join("; ");

    // Insert placeholders that the SQL serializer cannot provide.
    let replaced = concatenated.replace("___UPDATE_LHS___ = NULL", "..");

    Ok((replaced, parsed))
}

/// A visitor that normalizes the SQL AST in-place.
///
/// Used for span description normalization.
struct NormalizeVisitor;

impl NormalizeVisitor {
    /// Placeholder for string and numerical literals.
    fn placeholder() -> ValueWithSpan {
        Value::Placeholder("%s".to_owned()).into()
    }

    /// Placeholder for a list of items that has been scrubbed.
    fn ellipsis() -> Ident {
        Ident::new("..")
    }

    /// Check if a selected item can be erased into `..`.
    /// We only collapse simple values (e.g. NULL) and columns (`col1` or `col1 AS foo`).
    fn is_collapsible(item: &SelectItem) -> bool {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                matches!(
                    expr,
                    Expr::Value(_) | Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                )
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
            for TableAliasColumnDef { name, data_type: _ } in columns {
                Self::scrub_name(name);
            }
        }
    }

    fn simplify_compound_identifier(parts: &mut Vec<Ident>) {
        if let Some(mut last) = parts.pop() {
            Self::scrub_name(&mut last);
            *parts = vec![last];
        }
    }

    fn simplify_object_name(parts: &mut Vec<ObjectNamePart>) {
        if let Some(mut last) = parts.pop() {
            Self::scrub_object_name_part(&mut last);
            *parts = vec![last];
        }
    }

    fn scrub_name(name: &mut Ident) {
        name.quote_style = None;
        if let Cow::Owned(s) = TABLE_NAME_REGEX.replace_all(&name.value, "{%s}") {
            name.value = s
        };
    }

    fn scrub_object_name_part(part: &mut ObjectNamePart) {
        match part {
            ObjectNamePart::Identifier(ident) => Self::scrub_name(ident),
            ObjectNamePart::Function(ObjectNamePartFunction { name, args }) => {
                Self::scrub_name(name);
                Self::scrub_function_args(args);
            }
        }
    }

    fn scrub_function_args(args: &mut Vec<FunctionArg>) {
        for arg in args {
            if let FunctionArg::Named { name, .. } = arg {
                Self::scrub_name(name);
            }
        }
    }

    fn erase_name(name: &mut Ident) {
        name.quote_style = None;
        name.value = "%s".into()
    }

    fn scrub_statement_filter(filter: &mut Option<ShowStatementFilter>) {
        if let Some(s) = filter {
            match s {
                ShowStatementFilter::Like(s)
                | ShowStatementFilter::ILike(s)
                | ShowStatementFilter::NoKeyword(s) => "%s".clone_into(s),
                ShowStatementFilter::Where(_expr) => {}
            }
        }
    }
}

impl VisitorMut for NormalizeVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        Self::simplify_object_name(&mut relation.0);
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
            TableFactor::Pivot { alias, .. } => {
                Self::simplify_table_alias(alias);
            }
            TableFactor::Function {
                name, args, alias, ..
            } => {
                Self::simplify_object_name(&mut name.0);
                Self::scrub_function_args(args);
                Self::simplify_table_alias(alias);
            }
            TableFactor::JsonTable { columns, alias, .. } => {
                for column in columns {
                    match column {
                        JsonTableColumn::Named(JsonTableNamedColumn { name, .. }) => {
                            Self::scrub_name(name);
                        }
                        JsonTableColumn::ForOrdinality(ident) => Self::scrub_name(ident),
                        JsonTableColumn::Nested(JsonTableNestedColumn { path: _, columns }) => {
                            columns.clear(); // bit aggressive, might revisit later.
                        }
                    }
                }
                Self::simplify_table_alias(alias);
            }
            TableFactor::Unpivot {
                value,
                name,
                columns,
                alias,
                ..
            } => {
                Self::scrub_name(name);
                for ExprWithAlias { expr, alias } in columns {
                    if let Some(alias) = alias {
                        Self::scrub_name(alias);
                    }
                }
                Self::simplify_table_alias(alias);
            }
            // Not altering the following directly, might revisit later:
            TableFactor::OpenJsonTable { .. } => {}
            TableFactor::MatchRecognize { .. } => todo!(),
            TableFactor::XmlTable { .. } => todo!(),
            TableFactor::SemanticView { .. } => todo!(),
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
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
                else_result,
                case_token: _,
                end_token: _,
            } => {
                operand.take();
                *conditions = vec![CaseWhen {
                    condition: Expr::Identifier(Self::ellipsis()),
                    result: Expr::Identifier(Self::ellipsis()),
                }];
                else_result.take();
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Casts are omitted for simplification. Because we replace the entire expression,
        // the replacement has to occur *after* visiting its children.
        match expr {
            Expr::Cast { expr: inner, .. } => {
                *expr = take_expr(inner);
            }
            Expr::BinaryOp {
                left,
                op: op @ (BinaryOperator::Or | BinaryOperator::And),
                right,
            } => {
                remove_redundant_parentheses(op, left);
                remove_redundant_parentheses(op, right);
                if left == right {
                    //     /\
                    //    /  \
                    //   /\   B
                    //  /  \
                    // A    A
                    *expr = take_expr(left);
                } else {
                    //     /\
                    //    /  \
                    //   /\   B
                    //  /  \
                    // A    B
                    if let Expr::BinaryOp {
                        left: left_left,
                        op: left_op,
                        right: left_right,
                    } = left.as_mut()
                        && left_op == op
                        && left_right == right
                    {
                        *left = Box::new(take_expr(left_left));
                    }
                }
            }
            Expr::Nested(inner) if matches!(inner.as_ref(), &Expr::Nested(_)) => {
                // Remove multiple levels of parentheses.
                // These can occur because of the binary op reduction above.
                *expr = take_expr(inner);
            }
            _ => (),
        }

        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(query) => {
                self.transform_query(query);
            }
            Statement::Insert(Insert {
                columns, source, ..
            }) => {
                *columns = vec![Self::ellipsis()];
                if let Some(source) = source.as_mut()
                    && let SetExpr::Values(v) = &mut *source.body
                {
                    v.rows = vec![vec![Expr::Value(Self::placeholder())]]
                }
            }
            Statement::Update { assignments, .. } => {
                if assignments.len() > 1
                    && assignments
                        .iter()
                        .all(|a| matches!(a.value, Expr::Value(_)))
                {
                    *assignments = vec![Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName(vec![
                            ObjectNamePart::Identifier(Ident::new("___UPDATE_LHS___")),
                        ])),
                        value: Expr::Value(Value::Null.into()),
                    }]
                } else {
                    for assignment in assignments.iter_mut() {
                        if let AssignmentTarget::ColumnName(target) = &mut assignment.target {
                            Self::simplify_object_name(&mut target.0);
                        }
                    }
                }
            }
            Statement::Savepoint { name } => Self::erase_name(name),
            Statement::ReleaseSavepoint { name } => Self::erase_name(name),
            Statement::Declare { stmts } => {
                for Declare {
                    names, for_query, ..
                } in stmts
                {
                    for name in names {
                        Self::erase_name(name);
                    }
                    if let Some(for_query) = for_query {
                        self.transform_query(for_query.as_mut());
                    }
                }
            }
            Statement::Fetch { name, into, .. } => {
                Self::erase_name(name);
                if let Some(into) = into {
                    into.0 = vec![ObjectNamePart::Identifier(Ident {
                        value: "%s".into(),
                        quote_style: None,
                        span: Span::empty(),
                    })];
                }
            }
            Statement::Close { cursor } => match cursor {
                CloseCursor::All => {}
                CloseCursor::Specific { name } => Self::erase_name(name),
            },
            Statement::AlterTable {
                name, operations, ..
            } => {
                Self::simplify_object_name(&mut name.0);
                for operation in operations {
                    match operation {
                        AlterTableOperation::AddConstraint {
                            constraint,
                            not_valid: _,
                        } => match constraint {
                            TableConstraint::Unique { name, .. } => {
                                if let Some(name) = name {
                                    Self::scrub_name(name);
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
                            TableConstraint::Index { name, .. } => {
                                if let Some(name) = name {
                                    Self::scrub_name(name);
                                }
                            }
                            TableConstraint::FulltextOrSpatial { opt_index_name, .. } => {
                                if let Some(name) = opt_index_name {
                                    Self::scrub_name(name);
                                }
                            }
                            TableConstraint::PrimaryKey {
                                name,
                                index_name,
                                index_type,
                                columns,
                                index_options,
                                characteristics,
                            } => todo!(),
                        },
                        AlterTableOperation::AddColumn { column_def, .. } => {
                            let ColumnDef { name, .. } = column_def;
                            Self::scrub_name(name);
                        }
                        AlterTableOperation::DropConstraint { name, .. } => Self::scrub_name(name),
                        AlterTableOperation::DropColumn {
                            has_column_keyword,
                            column_names,
                            if_exists,
                            drop_behavior,
                        } => {
                            for column_name in column_names {
                                Self::scrub_name(column_name);
                            }
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
            }
            Statement::Analyze { columns, .. } => {
                Self::simplify_compound_identifier(columns);
            }
            Statement::Truncate { .. } => {}
            Statement::Msck { .. } => {}
            Statement::Directory { path, .. } => {
                "%s".clone_into(path);
            }
            Statement::Call(_) => {}
            Statement::Copy { source, values, .. } => {
                if let CopySource::Table { columns, .. } = source {
                    Self::simplify_compound_identifier(columns);
                }
                *values = vec![Some("..".into())];
            }
            Statement::CopyIntoSnowflake {
                files,
                pattern,
                validation_mode,
                kind,
                into,
                into_columns,
                from_obj,
                from_obj_alias,
                stage_params,
                from_transformations,
                from_query,
                file_format,
                copy_options,
                partition,
            } => {
                if let Some(from_obj) = from_obj {
                    Self::simplify_object_name(&mut from_obj.0);
                }
                *files = None;
                *pattern = None;
                *validation_mode = None;
            }
            Statement::Delete { .. } => {}
            Statement::CreateView {
                columns,
                cluster_by,
                ..
            } => {
                for column in columns {
                    Self::scrub_name(&mut column.name);
                }
                Self::simplify_compound_identifier(cluster_by);
            }
            Statement::CreateTable { .. } => {}
            Statement::CreateVirtualTable {
                module_name,
                module_args,
                ..
            } => {
                Self::scrub_name(module_name);
                Self::simplify_compound_identifier(module_args);
            }
            Statement::CreateIndex(CreateIndex {
                name,
                table_name,
                using,
                columns,
                unique,
                concurrently,
                if_not_exists,
                include,
                nulls_distinct,
                with,
                predicate,
                index_options,
                alter_options,
            }) => {
                // `table_name` is visited by `visit_relation`, but `name` is not.
                if let Some(name) = name {
                    Self::simplify_object_name(&mut name.0);
                }
                Self::simplify_compound_identifier(include);
            }
            Statement::CreateRole { .. } => {}
            Statement::AlterIndex { name, operation } => {
                // Names here are not visited by `visit_relation`.
                Self::simplify_object_name(&mut name.0);
                match operation {
                    sqlparser::ast::AlterIndexOperation::RenameIndex { index_name } => {
                        Self::simplify_object_name(&mut index_name.0);
                    }
                }
            }
            Statement::AlterView { .. } => {}
            Statement::AlterRole { name, .. } => {
                Self::scrub_name(name);
            }
            Statement::AttachDatabase { schema_name, .. } => Self::scrub_name(schema_name),
            Statement::Drop { .. } => {}
            Statement::DropFunction { .. } => {}
            Statement::CreateExtension { name, .. } => Self::scrub_name(name),
            Statement::Flush { channel, .. } => *channel = None,
            Statement::Discard { .. } => {}
            Statement::ShowFunctions { filter } => Self::scrub_statement_filter(filter),
            Statement::ShowVariable { variable } => Self::simplify_compound_identifier(variable),
            Statement::ShowVariables { filter, .. } => Self::scrub_statement_filter(filter),
            Statement::ShowCreate { .. } => {}
            Statement::ShowColumns {
                extended,
                full,
                show_options,
            } => {}
            Statement::ShowTables {
                terse,
                history,
                extended,
                full,
                external,
                show_options,
            } => {}
            Statement::ShowCollation { filter } => Self::scrub_statement_filter(filter),
            Statement::Use(
                Use::Database(db_name)
                | Use::Catalog(db_name)
                | Use::Database(db_name)
                | Use::Schema(db_name)
                | Use::Role(db_name)
                | Use::Warehouse(db_name),
            ) => Self::simplify_object_name(&mut db_name.0),
            Statement::Use(_) => {} // gave up at this point
            Statement::StartTransaction { .. } => {}
            Statement::Comment { comment, .. } => *comment = None,
            Statement::Commit { .. } => {}
            Statement::Rollback { savepoint, .. } => {
                if let Some(savepoint) = savepoint {
                    Self::erase_name(savepoint);
                }
            }
            Statement::CreateSchema { .. } => {}
            Statement::CreateDatabase {
                location,
                managed_location,
                ..
            } => {
                *location = None;
                *managed_location = None;
            }
            Statement::CreateFunction { .. } => {}
            Statement::CreateProcedure { .. } => {}
            Statement::CreateMacro { .. } => {}
            Statement::CreateStage { comment, .. } => *comment = None,
            Statement::Assert { .. } => {}
            Statement::Grant {
                grantees,
                granted_by,
                ..
            } => {
                for grantee in grantees {
                    if let Some(GranteeName::ObjectName(name)) = &mut grantee.name {
                        Self::simplify_object_name(&mut name.0);
                    }
                }

                *granted_by = None;
            }
            Statement::Revoke {
                grantees,
                granted_by,
                ..
            } => {
                for grantee in grantees {
                    if let Some(GranteeName::ObjectName(name)) = &mut grantee.name {
                        Self::simplify_object_name(&mut name.0);
                    }
                }
                *granted_by = None;
            }
            Statement::Deallocate { name, .. } => {
                Self::scrub_name(name);
            }
            Statement::Execute { name, .. } => {
                if let Some(name) = name {
                    Self::simplify_object_name(&mut name.0);
                }
            }
            Statement::Prepare { name, .. } => Self::scrub_name(name),
            Statement::Kill { id, .. } => *id = 0,
            Statement::ExplainTable { .. } => {}
            Statement::Explain { .. } => {}
            Statement::Merge { .. } => {}
            Statement::Cache { .. } => {}
            Statement::UNCache { .. } => {}
            Statement::CreateSequence { .. } => {}
            Statement::CreateType { .. } => {}
            Statement::Pragma { .. } => {}
            Statement::LockTables { tables } => {
                for table in tables {
                    let LockTable {
                        table,
                        alias,
                        lock_type: _,
                    } = table;
                    Self::scrub_name(table);
                    if let Some(alias) = alias {
                        Self::scrub_name(alias);
                    }
                }
            }
            Statement::UnlockTables => {}
            Statement::Install { extension_name } | Statement::Load { extension_name } => {
                Self::scrub_name(extension_name)
            }
            Statement::ShowStatus {
                filter: _,
                global: _,
                session: _,
            } => {}
            Statement::Unload { to, .. } => {
                Self::scrub_name(to);
            }
            Statement::Set(set) => todo!(),
            Statement::Case(case_statement) => todo!(),
            Statement::If(if_statement) => todo!(),
            Statement::While(while_statement) => todo!(),
            Statement::Raise(raise_statement) => todo!(),
            Statement::Open(open_statement) => todo!(),
            Statement::CreateSecret {
                or_replace,
                temporary,
                if_not_exists,
                name,
                storage_specifier,
                secret_type,
                options,
            } => todo!(),
            Statement::CreateServer(create_server_statement) => todo!(),
            Statement::CreatePolicy {
                name,
                table_name,
                policy_type,
                command,
                to,
                using,
                with_check,
            } => todo!(),
            Statement::CreateConnector(create_connector) => todo!(),
            Statement::AlterSchema(alter_schema) => todo!(),
            Statement::AlterType(alter_type) => todo!(),
            Statement::AlterPolicy {
                name,
                table_name,
                operation,
            } => todo!(),
            Statement::AlterConnector {
                name,
                properties,
                url,
                owner,
            } => todo!(),
            Statement::AlterSession {
                set,
                session_params,
            } => todo!(),
            Statement::AttachDuckDBDatabase {
                if_not_exists,
                database,
                database_path,
                database_alias,
                attach_options,
            } => todo!(),
            Statement::DetachDuckDBDatabase {
                if_exists,
                database,
                database_alias,
            } => todo!(),
            Statement::DropDomain(drop_domain) => todo!(),
            Statement::DropProcedure {
                if_exists,
                proc_desc,
                drop_behavior,
            } => todo!(),
            Statement::DropSecret {
                if_exists,
                temporary,
                name,
                storage_specifier,
            } => todo!(),
            Statement::DropPolicy {
                if_exists,
                name,
                table_name,
                drop_behavior,
            } => todo!(),
            Statement::DropConnector { if_exists, name } => todo!(),
            Statement::DropExtension {
                names,
                if_exists,
                cascade_or_restrict,
            } => todo!(),
            Statement::ShowDatabases {
                terse,
                history,
                show_options,
            } => todo!(),
            Statement::ShowSchemas {
                terse,
                history,
                show_options,
            } => todo!(),
            Statement::ShowCharset(show_charset) => todo!(),
            Statement::ShowObjects(show_objects) => todo!(),
            Statement::ShowViews {
                terse,
                materialized,
                show_options,
            } => todo!(),
            Statement::CreateTrigger(create_trigger) => todo!(),
            Statement::DropTrigger(drop_trigger) => todo!(),
            Statement::Deny(deny_statement) => todo!(),
            Statement::CreateDomain(create_domain) => todo!(),
            Statement::OptimizeTable {
                name,
                on_cluster,
                partition,
                include_final,
                deduplicate,
            } => todo!(),
            Statement::LISTEN { channel } => todo!(),
            Statement::UNLISTEN { channel } => todo!(),
            Statement::NOTIFY { channel, payload } => todo!(),
            Statement::LoadData {
                local,
                inpath,
                overwrite,
                table_name,
                partitioned,
                table_format,
            } => todo!(),
            Statement::RenameTable(rename_tables) => todo!(),
            Statement::List(file_staging_command) => todo!(),
            Statement::Remove(file_staging_command) => todo!(),
            Statement::RaisError {
                message,
                severity,
                state,
                arguments,
                options,
            } => todo!(),
            Statement::Print(print_statement) => todo!(),
            Statement::Return(return_statement) => todo!(),
            Statement::ExportData(export_data) => todo!(),
            Statement::CreateUser(create_user) => todo!(),
            Statement::Vacuum(vacuum_statement) => todo!(),
        }

        ControlFlow::Continue(())
    }
}

/// Get ownership of an `Expr` and leave a NULL value in its place.
///
/// We cannot use [`std::mem::take`], because [`Expr`] does not implement [`Default`].
fn take_expr(expr: &mut Expr) -> Expr {
    let mut swapped = Expr::Value(Value::Null.into());
    std::mem::swap(&mut swapped, expr);
    swapped
}

/// Removes parentheses for equal operators, e.g. `(a OR b) OR c`.
///
/// Only use this function on operations which have the
/// [associative property](https://en.wikipedia.org/wiki/Associative_property).
fn remove_redundant_parentheses(outer_op: &BinaryOperator, expr: &mut Expr) {
    if let Expr::Nested(inner) = expr
        && let Expr::BinaryOp { op, .. } = inner.as_ref()
        && op == outer_op
    {
        *expr = take_expr(inner.as_mut());
    }
}

/// Limits the maximum expression depth of an SQL statement.
///
/// This prevents stack overflows when serializing the query back to a string.
struct MaxDepthVisitor {
    /// The current depth of an expression.
    current_expr_depth: usize,
}

impl MaxDepthVisitor {
    pub fn new() -> Self {
        Self {
            current_expr_depth: 0,
        }
    }
}

impl VisitorMut for MaxDepthVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if self.current_expr_depth > MAX_EXPRESSION_DEPTH {
            *expr = Expr::Value(Value::Placeholder("..".to_owned()).into());
            return ControlFlow::Continue(());
        }
        self.current_expr_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &mut Expr) -> ControlFlow<Self::Break> {
        self.current_expr_depth = self.current_expr_depth.saturating_sub(1);
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

    fn supports_filter_during_aggregation(&self) -> bool {
        self.0.supports_filter_during_aggregation()
    }

    fn supports_within_after_array_aggregation(&self) -> bool {
        self.0.supports_within_after_array_aggregation()
    }

    fn supports_group_by_expr(&self) -> bool {
        self.0.supports_group_by_expr()
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
        assert_eq!(
            normalize_parsed_queries(None, query).unwrap().0,
            "SELECT .. + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s"
        );
    }

    #[test]
    fn parse_dont_panic() {
        assert!(parse_query_inner(None, "REPLACE g;'341234c").is_err());
    }
}
