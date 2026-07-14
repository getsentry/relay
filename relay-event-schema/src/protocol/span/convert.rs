//! This module defines bidirectional field mappings between spans and transactions.

use relay_conventions::attributes::{
    BROWSER__NAME, HTTP__QUERY, SENTRY__ENVIRONMENT, SENTRY__RELEASE, SENTRY__SDK__NAME,
    SENTRY__SDK__VERSION, SENTRY__SEGMENT__NAME, URL__QUERY,
};
use relay_protocol::{Annotated, IntoValue, Value};

use crate::protocol::{BrowserContext, Event, ProfileContext, Span, SpanData, TraceContext};

impl From<&Event> for Span {
    fn from(event: &Event) -> Self {
        let Event {
            transaction,

            platform,
            timestamp,
            start_timestamp,
            received,
            release,
            environment,
            tags,

            measurements,
            _metrics,
            ..
        } = event;

        let trace = event.context::<TraceContext>();

        // Fill data from trace context:
        let mut data = trace
            .map(|c| c.data.clone().map_value(SpanData::from))
            .unwrap_or_default();

        // Overwrite specific fields:
        let span_data = data.get_or_insert_with(Default::default);
        span_data.other.insert(
            SENTRY__SEGMENT__NAME.to_owned(),
            transaction.clone().map_value(IntoValue::into_value),
        );
        // For root spans, the name should just be the transaction name.
        span_data.other.insert(
            "sentry.name".to_owned(),
            transaction.clone().map_value(IntoValue::into_value),
        );
        span_data.other.insert(
            SENTRY__RELEASE.to_owned(),
            release.clone().map_value(IntoValue::into_value),
        );
        span_data.other.insert(
            SENTRY__ENVIRONMENT.to_owned(),
            environment.clone().map_value(IntoValue::into_value),
        );
        if let Some(browser) = event.context::<BrowserContext>() {
            span_data.other.insert(
                BROWSER__NAME.to_owned(),
                browser.name.clone().map_value(IntoValue::into_value),
            );
        }
        if let Some(client_sdk) = event.client_sdk.value() {
            span_data.other.insert(
                SENTRY__SDK__NAME.to_owned(),
                client_sdk.name.clone().map_value(IntoValue::into_value),
            );
            span_data.other.insert(
                SENTRY__SDK__VERSION.to_owned(),
                client_sdk.version.clone().map_value(IntoValue::into_value),
            );
        }
        if let Some(request) = event.request.value()
            && let Some(query) = request.query_string.value()
            && let Some(qs) = query.to_query_string()
        {
            span_data.other.insert(
                HTTP__QUERY.to_owned(),
                Annotated::new(Value::String(format!("?{qs}"))),
            );
            span_data
                .other
                .insert(URL__QUERY.to_owned(), Annotated::new(Value::String(qs)));
        }

        Self {
            timestamp: timestamp.clone(),
            start_timestamp: start_timestamp.clone(),
            exclusive_time: trace.map(|c| c.exclusive_time.clone()).unwrap_or_default(),
            op: trace.map(|c| c.op.clone()).unwrap_or_default(),
            span_id: trace.map(|c| c.span_id.clone()).unwrap_or_default(),
            parent_span_id: trace.map(|c| c.parent_span_id.clone()).unwrap_or_default(),
            trace_id: trace.map(|c| c.trace_id.clone()).unwrap_or_default(),
            segment_id: trace.map(|c| c.span_id.clone()).unwrap_or_default(),
            is_segment: true.into(),
            // NB: Technically, this span may not be an actual remote span if this is a child
            // transaction created within the same service as its parent. We still set `is_remote`
            // as the best proxy to ensure this span will be detected as a segment by the spans
            // pipeline.
            is_remote: true.into(),
            status: trace.map(|c| c.status.clone()).unwrap_or_default(),
            description: transaction.clone(),
            tags: tags.clone().map_value(|t| t.into()),
            origin: trace.map(|c| c.origin.clone()).unwrap_or_default(),
            profile_id: event
                .context::<ProfileContext>()
                .map(|c| c.profile_id.clone())
                .unwrap_or_default(),
            data,
            links: trace.map(|c| c.links.clone()).unwrap_or_default(),
            sentry_tags: Default::default(),
            received: received.clone(),
            measurements: measurements.clone(),
            platform: platform.clone(),
            was_transaction: true.into(),
            kind: Default::default(),
            other: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;

    #[test]
    fn convert() {
        let event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "platform": "php",
                "sdk": {"name": "sentry.php", "version": "1.2.3"},
                "release": "myapp@1.0.0",
                "environment": "prod",
                "transaction": "my 1st transaction",
                "contexts": {
                    "browser": {"name": "Chrome"},
                    "profile": {"profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab"},
                    "trace": {
                        "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                        "span_id": "FA90FDEAD5F74052",
                        "type": "trace",
                        "origin": "manual",
                        "op": "myop",
                        "status": "ok",
                        "exclusive_time": 123.4,
                        "parent_span_id": "FA90FDEAD5F74051",
                        "data": {
                            "custom_attribute": 42
                        },
                        "links": [
                            {
                                "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                                "span_id": "fa90fdead5f74052",
                                "sampled": true,
                                "attributes": {
                                    "sentry.link.type": "previous_trace"
                                }
                            }
                        ]
                    }
                },
                "request": {
                    "url": "http://example.com/api/0/organizations/",
                    "method": "GET",
                    "query_string": "project=1&sort=date"
                },
                "measurements": {
                    "memory": {
                        "value": 9001.0,
                        "unit": "byte"
                    }
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let span_from_event = Span::from(&event);
        insta::assert_debug_snapshot!(span_from_event, @r#"
        Span {
            timestamp: ~,
            start_timestamp: ~,
            exclusive_time: 123.4,
            op: "myop",
            span_id: SpanId("fa90fdead5f74052"),
            parent_span_id: SpanId("fa90fdead5f74051"),
            trace_id: TraceId("4c79f60c11214eb38604f4ae0781bfb2"),
            segment_id: SpanId("fa90fdead5f74052"),
            is_segment: true,
            is_remote: true,
            status: Ok,
            description: "my 1st transaction",
            tags: ~,
            origin: "manual",
            profile_id: EventId(
                a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab,
            ),
            data: SpanData {
                other: {
                    "browser.name": String(
                        "Chrome",
                    ),
                    "custom_attribute": I64(
                        42,
                    ),
                    "http.query": String(
                        "?project=1&sort=date",
                    ),
                    "sentry.environment": String(
                        "prod",
                    ),
                    "sentry.name": String(
                        "my 1st transaction",
                    ),
                    "sentry.release": String(
                        "myapp@1.0.0",
                    ),
                    "sentry.sdk.name": String(
                        "sentry.php",
                    ),
                    "sentry.sdk.version": String(
                        "1.2.3",
                    ),
                    "sentry.segment.name": String(
                        "my 1st transaction",
                    ),
                    "url.query": String(
                        "project=1&sort=date",
                    ),
                },
            },
            links: [
                SpanLink {
                    trace_id: TraceId("4c79f60c11214eb38604f4ae0781bfb2"),
                    span_id: SpanId("fa90fdead5f74052"),
                    sampled: true,
                    attributes: {
                        "sentry.link.type": String(
                            "previous_trace",
                        ),
                    },
                    other: {},
                },
            ],
            sentry_tags: ~,
            received: ~,
            measurements: Measurements(
                {
                    "memory": Measurement {
                        value: 9001.0,
                        unit: Information(
                            Byte,
                        ),
                    },
                },
            ),
            platform: "php",
            was_transaction: true,
            kind: ~,
            other: {},
        }
        "#);
    }
}
