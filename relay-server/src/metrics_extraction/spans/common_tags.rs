use std::collections::BTreeMap;

use relay_general::protocol::Span;

use crate::metrics_extraction::spans::types::SpanTagKey;
use crate::metrics_extraction::spans::{
    domain_from_http_url, sanitized_span_description, sql_action_from_query, sql_table_from_query,
};
use crate::metrics_extraction::utils::http_status_code_from_span;

pub(crate) fn extract_common_span_tags(
    span: &Span,
    max_tag_len: usize,
) -> BTreeMap<SpanTagKey, String> {
    let mut tags = BTreeMap::new();

    if let Some(span_status) = span.status.value() {
        tags.insert(SpanTagKey::Status, span_status.to_string());
    }

    if let Some(status_code) = http_status_code_from_span(span) {
        tags.insert(SpanTagKey::StatusCode, status_code);
    }

    if let Some(normalized_desc) = get_normalized_description(span) {
        // Truncating the span description's tag value is, for now,
        // a temporary solution to not get large descriptions dropped. The
        // group tag mustn't be affected by this, and still be
        // computed from the full, untruncated description.

        let mut span_group = format!("{:?}", md5::compute(&normalized_desc));
        span_group.truncate(16);
        tags.insert(SpanTagKey::Group, span_group);

        let truncated = truncate_string(normalized_desc, max_tag_len);
        tags.insert(SpanTagKey::Description, truncated);
    }

    tags
}

fn get_normalized_description(span: &Span) -> Option<String> {
    if let Some(unsanitized_span_op) = span.op.value() {
        let span_op = unsanitized_span_op.to_owned().to_lowercase();

        let span_module = if span_op.starts_with("http") {
            Some("http")
        } else if span_op.starts_with("db") {
            Some("db")
        } else if span_op.starts_with("cache") {
            Some("cache")
        } else {
            None
        };

        let scrubbed_description = span
            .data
            .value()
            .and_then(|data| data.get("description.scrubbed"))
            .and_then(|value| value.as_str());

        let action = match span_module {
            Some("http") => span
                .data
                .value()
                // TODO(iker): some SDKs extract this as method
                .and_then(|v| v.get("http.method"))
                .and_then(|method| method.as_str())
                .map(|s| s.to_uppercase()),
            Some("db") => {
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
            _ => None,
        };

        let domain = if span_op == "http.client" {
            span.description
                .value()
                .and_then(|url| domain_from_http_url(url))
                .map(|d| d.to_lowercase())
        } else if span_op.starts_with("db") {
            span.description
                .value()
                .and_then(|query| sql_table_from_query(query))
                .map(|t| t.to_lowercase())
        } else {
            None
        };

        let sanitized_description = sanitized_span_description(
            scrubbed_description,
            span_module,
            action.as_deref(),
            domain.as_deref(),
        );
        // dbg!(&sanitized_description);
        return sanitized_description;
    }
    None
}

/// Trims the given string with the given maximum bytes. Splitting only happens
/// on char boundaries.
///
/// If the string is short, it remains unchanged. If it's long, this method
/// truncates it to the maximum allowed size and sets the last character to
/// `*`.
fn truncate_string(mut string: String, max_bytes: usize) -> String {
    if string.len() <= max_bytes {
        return string;
    }

    if max_bytes == 0 {
        return String::new();
    }

    let mut cutoff = max_bytes - 1; // Leave space for `*`

    while cutoff > 0 && !string.is_char_boundary(cutoff) {
        cutoff -= 1;
    }

    string.truncate(cutoff);
    string.push('*');
    string
}
