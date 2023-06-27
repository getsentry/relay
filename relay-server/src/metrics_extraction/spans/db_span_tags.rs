use std::collections::BTreeMap;

use once_cell::sync::Lazy;
use regex::Regex;
use relay_general::protocol::Span;

use crate::metrics_extraction::spans::extract_captured_substring;
use crate::metrics_extraction::spans::types::SpanTagKey;

pub(crate) fn extract_db_span_tags(span: &Span) -> BTreeMap<SpanTagKey, String> {
    let mut tags = BTreeMap::new();

    tags.insert(SpanTagKey::Module, "db".to_owned());

    if let Some(action) = db_action(span) {
        tags.insert(SpanTagKey::Action, action);
    }

    if let Some(domain) = span
        .description
        .value()
        .and_then(|query| sql_table_from_query(query))
        .map(|t| t.to_lowercase())
    {
        tags.insert(SpanTagKey::Domain, domain);
    }

    if let Some(system) = span
        .data
        .value()
        .and_then(|v| v.get("db.system"))
        .and_then(|system| system.as_str())
    {
        tags.insert(SpanTagKey::System, system.to_lowercase());
    }

    tags
}

fn db_action(span: &Span) -> Option<String> {
    let action_from_data = span
        .data
        .value()
        .and_then(|v| v.get("db.operation"))
        .and_then(|db_op| db_op.as_str())
        .map(|s| s.to_uppercase());
    action_from_data.or_else(|| {
        span.description
            .value()
            .and_then(|d| sql_action_from_query(d))
            .map(|a| a.to_uppercase())
    })
}

/// Regex with a capture group to extract the database action from a query.
///
/// Currently, we're only interested in either `SELECT` or `INSERT` statements.
static SQL_ACTION_EXTRACTOR_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(?P<action>(SELECT|INSERT))"#).unwrap());

pub(crate) fn sql_action_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_ACTION_EXTRACTOR_REGEX)
}

/// Regex with a capture group to extract the table from a database query,
/// based on `FROM` and `INTO` keywords.
static SQL_TABLE_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(from|into)(\s|"|'|\()+(?P<table>(\w+(\.\w+)*))(\s|"|'|\))+"#).unwrap()
});

/// Returns the table in the SQL query, if any.
///
/// If multiple tables exist, only the first one is returned.
pub(crate) fn sql_table_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_TABLE_EXTRACTOR_REGEX)
}
