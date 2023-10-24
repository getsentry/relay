//! **Deprecated.** Utilities for extracting common event fields.
//!
//! This utility module is being phased out. Functionality in this module should be moved to the
//! specific normalization file requiring this data access.

use relay_event_schema::protocol::{Event, ResponseContext, Span, TraceContext, User};

/// Used to decide when to extract mobile-specific tags.
pub const MOBILE_SDKS: [&str; 4] = [
    "sentry.cocoa",
    "sentry.dart.flutter",
    "sentry.java.android",
    "sentry.javascript.react-native",
];

/// Maximum length of a mobile span or measurement in milliseconds.
///
/// Spans like `ui.load` with an `exclusive_time` that exceeds this number will be removed,
/// as well as mobile measurements (on transactions) such as `app.start.cold`, etc.
pub const MAX_DURATION_MOBILE_MS: f64 = 180_000.0;

/// Extract the HTTP status code from the span data.
pub fn http_status_code_from_span(span: &Span) -> Option<String> {
    // For SDKs which put the HTTP status code into the span data.
    if let Some(status_code) = span
        .data
        .value()
        .and_then(|v| {
            v.get("http.response.status_code")
                .or_else(|| v.get("status_code"))
        })
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
    {
        return Some(status_code);
    }

    // For SDKs which put the HTTP status code into the span tags.
    if let Some(status_code) = span
        .tags
        .value()
        .and_then(|tags| tags.get("http.status_code"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_owned())
    {
        return Some(status_code);
    }

    None
}

/// Extracts the HTTP status code.
pub fn extract_http_status_code(event: &Event) -> Option<String> {
    // For SDKs which put the HTTP status code in the event tags.
    if let Some(status_code) = event.tag_value("http.status_code") {
        return Some(status_code.to_owned());
    }

    if let Some(spans) = event.spans.value() {
        for span in spans {
            if let Some(span_value) = span.value() {
                if let Some(status_code) = http_status_code_from_span(span_value) {
                    return Some(status_code);
                }
            }
        }
    }

    // For SDKs which put the HTTP status code into the breadcrumbs data.
    if let Some(breadcrumbs) = event.breadcrumbs.value() {
        if let Some(values) = breadcrumbs.values.value() {
            for breadcrumb in values {
                // We need only the `http` type.
                if let Some(crumb) = breadcrumb
                    .value()
                    .filter(|bc| bc.ty.as_str() == Some("http"))
                {
                    // Try to get the status code om the map.
                    if let Some(status_code) = crumb.data.value().and_then(|v| v.get("status_code"))
                    {
                        return status_code.value().and_then(|v| v.as_str()).map(Into::into);
                    }
                }
            }
        }
    }

    // For SDKs which put the HTTP status code in the `Response` context.
    if let Some(response_context) = event.context::<ResponseContext>() {
        let status_code = response_context
            .status_code
            .value()
            .map(|code| code.to_string());
        return status_code;
    }

    None
}

/// Compute the transaction event's "user" tag as close as possible to how users are determined in
/// the transactions dataset in Snuba. This should produce the exact same user counts as the `user`
/// column in Discover for Transactions, barring:
///
/// * imprecision caused by HLL sketching in Snuba, which we don't have in events
/// * hash collisions in `BucketValue::set_from_display`, which we don't have in events
/// * MD5-collisions caused by `EventUser.hash_from_tag`, which we don't have in metrics
///
///   MD5 is used to efficiently look up the current event user for an event, and if there is a
///   collision it seems that this code will fetch an event user with potentially different values
///   for everything that is in `defaults`:
///   <https://github.com/getsentry/sentry/blob/f621cd76da3a39836f34802ba9b35133bdfbe38b/src/sentry/event_manager.py#L1058-L1060>
///
/// The performance product runs a discover query such as `count_unique(user)`, which maps to two
/// things:
///
/// * `user` metric for the metrics dataset
/// * the "promoted tag" column `user` in the transactions clickhouse table
///
/// A promoted tag is a tag that snuba pulls out into its own column. In this case it pulls out the
/// `sentry:user` tag from the event payload:
/// <https://github.com/getsentry/snuba/blob/430763e67e30957c89126e62127e34051eb52fd6/snuba/datasets/transactions_processor.py#L151>
///
/// Sentry's processing pipeline defers to `sentry.models.EventUser` to produce the `sentry:user` tag
/// here: <https://github.com/getsentry/sentry/blob/f621cd76da3a39836f34802ba9b35133bdfbe38b/src/sentry/event_manager.py#L790-L794>
///
/// `sentry.models.eventuser.KEYWORD_MAP` determines which attributes are looked up in which order, here:
/// <https://github.com/getsentry/sentry/blob/f621cd76da3a39836f34802ba9b35133bdfbe38b/src/sentry/models/eventuser.py#L18>
/// If its order is changed, this function needs to be changed.
pub fn get_eventuser_tag(user: &User) -> Option<String> {
    if let Some(id) = user.id.as_str() {
        return Some(format!("id:{id}"));
    }

    if let Some(username) = user.username.as_str() {
        return Some(format!("username:{username}"));
    }

    if let Some(email) = user.email.as_str() {
        return Some(format!("email:{email}"));
    }

    if let Some(ip_address) = user.ip_address.as_str() {
        return Some(format!("ip:{ip_address}"));
    }

    None
}

/// Returns a normalized `op` from the given trace context.
pub fn extract_transaction_op(trace_context: &TraceContext) -> Option<String> {
    let op = trace_context.op.value()?;
    if op == "default" {
        // This was likely set by normalization, so let's treat it as None
        // See https://github.com/getsentry/relay/blob/bb2ac4ee82c25faa07a6d078f93d22d799cfc5d1/relay-general/src/store/transactions.rs#L96

        // Note that this is the opposite behavior of what we do for transaction.status, where
        // we coalesce None to "unknown".
        return None;
    }
    Some(op.to_string())
}
