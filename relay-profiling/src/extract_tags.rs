use relay_general::protocol::{AsPair, Context, ContextInner, Event, TraceContext};
use relay_general::types::Annotated;
use std::collections::{BTreeMap, BTreeSet};

use relay_common::SpanStatus;

pub fn extract_tags(
    event: &Event,
    extract_custom_tags: BTreeSet<String>,
) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    if let Some(release) = event.release.as_str() {
        tags.insert("release".to_owned(), release.to_owned());
    }
    if let Some(dist) = event.dist.value() {
        tags.insert("dist".to_owned(), dist.clone());
    }
    if let Some(environment) = event.environment.as_str() {
        tags.insert("environment".to_owned(), environment.to_owned());
    }

    tags.insert(
        "transaction".to_owned(),
        event.get_transaction_source().to_string(),
    );

    if let Some(trace_context) = get_trace_context(event) {
        let status = extract_transaction_status(trace_context);
        tags.insert("transaction.status".to_owned(), status.to_string());

        if let Some(op) = trace_context.op.value() {
            tags.insert("transaction.op".to_owned(), op.to_owned());
        }
    }

    if let Some(http_method) = extract_http_method(event) {
        tags.insert("http.method".to_owned(), http_method);
    }

    let custom_tags = &extract_custom_tags;
    if !custom_tags.is_empty() {
        // XXX(slow): event tags are a flat array
        if let Some(event_tags) = event.tags.value() {
            for tag_entry in &**event_tags {
                if let Some(entry) = tag_entry.value() {
                    let (key, value) = entry.as_pair();
                    if let (Some(key), Some(value)) = (key.as_str(), value.as_str()) {
                        if custom_tags.contains(key) {
                            tags.insert(key.to_owned(), value.to_owned());
                        }
                    }
                }
            }
        }
    }

    tags
}

/// Extract transaction status, defaulting to [`SpanStatus::Unknown`].
/// Must be consistent with `process_trace_context` in [`relay_general::store`].
fn extract_transaction_status(trace_context: &TraceContext) -> SpanStatus {
    *trace_context.status.value().unwrap_or(&SpanStatus::Unknown)
}

/// Extract HTTP method
/// See <https://github.com/getsentry/snuba/blob/2e038c13a50735d58cc9397a29155ab5422a62e5/snuba/datasets/errors_processor.py#L64-L67>.
fn extract_http_method(transaction: &Event) -> Option<String> {
    let request = transaction.request.value()?;
    let method = request.method.value()?;
    Some(method.clone())
}

fn get_trace_context(event: &Event) -> Option<&TraceContext> {
    let contexts = event.contexts.value()?;
    let trace = contexts.get("trace").and_then(Annotated::value);
    if let Some(ContextInner(Context::Trace(trace_context))) = trace {
        return Some(trace_context.as_ref());
    }

    None
}
