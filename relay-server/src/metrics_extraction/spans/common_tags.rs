use std::collections::BTreeMap;

use relay_general::protocol::Span;

use crate::metrics_extraction::spans::http_span_tags;
use crate::metrics_extraction::spans::types::SpanTagKey;
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

    if let Some(normalized_desc) = normalized_description(span) {
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

fn normalized_description(span: &Span) -> Option<String> {
    if let Some(unsanitized_span_op) = span.op.value() {
        let span_op = unsanitized_span_op.to_owned().to_lowercase();

        let span_module = if span_op.starts_with("http") {
            Some("http")
        } else {
            None
        };

        let scrubbed_description = scrubbed_description(span);
        let action = http_span_tags::action(span);
        let domain = http_span_tags::domain(span);

        if let Some(d) = scrubbed_description {
            return Some(d.to_owned());
        }
        return fallback_span_description(span_module, action.as_deref(), domain.as_deref());
    }
    None
}

fn scrubbed_description(span: &Span) -> Option<&str> {
    span.data
        .value()
        .and_then(|data| data.get("description.scrubbed"))
        .and_then(|value| value.as_str())
}

/// Returns the sanitized span description.
///
/// If a scrub description is provided, that's returned. If not, a new
/// description is built for `http*` modules with the following format:
/// `{action} {domain}/<unparameterized>`.
fn fallback_span_description(
    module: Option<&str>,
    action: Option<&str>,
    domain: Option<&str>,
) -> Option<String> {
    match module {
        Some("http") => (),
        _ => return None,
    };

    let mut sanitized = String::new();

    if let Some(a) = action {
        sanitized.push_str(&format!("{a} "));
    }
    if let Some(d) = domain {
        sanitized.push_str(&format!("{d}/"));
    }
    sanitized.push_str("<unparameterized>");

    Some(sanitized)
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

#[cfg(test)]
mod tests {
    use crate::metrics_extraction::spans::common_tags::truncate_string;

    #[test]
    fn test_truncate_string_no_panic() {
        let string = "ÆÆ".to_owned();

        let truncated = truncate_string(string.clone(), 0);
        assert_eq!(truncated, "");

        let truncated = truncate_string(string.clone(), 1);
        assert_eq!(truncated, "*");

        let truncated = truncate_string(string.clone(), 2);
        assert_eq!(truncated, "*");

        let truncated = truncate_string(string.clone(), 3);
        assert_eq!(truncated, "Æ*");

        let truncated = truncate_string(string.clone(), 4);
        assert_eq!(truncated, "ÆÆ");

        let truncated = truncate_string(string, 5);
        assert_eq!(truncated, "ÆÆ");
    }
}
