use std::collections::BTreeMap;

use chrono::SecondsFormat;

use relay_event_schema::protocol::{AppContext, AsPair, Event, SpanStatus, TraceContext};

pub fn extract_transaction_metadata(event: &Event) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();

    if let Some(release) = event.release.as_str() {
        tags.insert("release".to_owned(), release.to_owned());
    }
    if let Some(dist) = event.dist.as_str() {
        tags.insert("dist".to_owned(), dist.to_owned());
    }
    if let Some(environment) = event.environment.as_str() {
        tags.insert("environment".to_owned(), environment.to_owned());
    }

    if let Some(transaction) = event.transaction.as_str() {
        tags.insert("transaction".to_owned(), transaction.to_owned());
    }

    if let Some(trace_context) = event.context::<TraceContext>() {
        let status = extract_transaction_status(trace_context);
        tags.insert("transaction.status".to_owned(), status.to_string());

        if let Some(op) = trace_context.op.value() {
            tags.insert("transaction.op".to_owned(), op.to_owned());
        }
    }

    if let Some(http_method) = extract_http_method(event) {
        tags.insert("http.method".to_owned(), http_method);
    }

    if let Some(timestamp) = event.start_timestamp.value() {
        tags.insert(
            "transaction.start".to_owned(),
            timestamp
                .into_inner()
                .to_rfc3339_opts(SecondsFormat::Nanos, false),
        );
    }

    if let Some(timestamp) = event.timestamp.value() {
        tags.insert(
            "transaction.end".to_owned(),
            timestamp
                .into_inner()
                .to_rfc3339_opts(SecondsFormat::Nanos, false),
        );
    }

    if let Some(app_context) = event.context::<AppContext>() {
        if let Some(app_identifier) = app_context.app_identifier.value() {
            tags.insert("app.identifier".to_owned(), app_identifier.to_owned());
        }
    }

    tags
}

pub fn extract_transaction_tags(event: &Event) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();

    // XXX(slow): event tags are a flat array
    if let Some(event_tags) = event.tags.value() {
        for tag_entry in &**event_tags {
            if let Some(entry) = tag_entry.value() {
                let (key, value) = entry.as_pair();
                if let (Some(key), Some(value)) = (key.as_str(), value.as_str()) {
                    tags.insert(key.to_owned(), value.to_owned());
                }
            }
        }
    }

    tags
}

/// Extract transaction status, defaulting to [`SpanStatus::Unknown`].
///
/// Must be consistent with `process_trace_context` in `relay_event_normalization`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use relay_event_schema::protocol::Event;
    use relay_protocol::FromValue;

    #[test]
    fn test_extract_transaction_metadata() {
        let event = Event::from_value(
            serde_json::json!({
                "release": "myrelease",
                "dist": "mydist",
                "environment": "myenvironment",
                "transaction": "mytransaction",
                "contexts": {
                    "app": {
                        "app_identifier": "io.sentry.myexample",
                    },
                    "trace": {
                        "status": "ok",
                        "op": "myop",
                    },
                },
                "request": {
                    "method": "GET",
                },
                "timestamp": "2011-05-02T17:41:36Z",
                "start_timestamp": "2011-05-02T17:40:36Z",
            })
            .into(),
        );

        let metadata = extract_transaction_metadata(&event.0.unwrap());
        insta::assert_debug_snapshot!(metadata, @r#"
        {
            "app.identifier": "io.sentry.myexample",
            "dist": "mydist",
            "environment": "myenvironment",
            "http.method": "GET",
            "release": "myrelease",
            "transaction": "mytransaction",
            "transaction.end": "2011-05-02T17:41:36.000000000+00:00",
            "transaction.op": "myop",
            "transaction.start": "2011-05-02T17:40:36.000000000+00:00",
            "transaction.status": "ok",
        }
        "#);
    }
}
