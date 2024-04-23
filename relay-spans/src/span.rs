use std::str::FromStr;

use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;

use crate::otel_trace::{status::StatusCode as OtelStatusCode, Span as OtelSpan};
use crate::status_codes;
use relay_event_schema::protocol::{
    EventId, Span as EventSpan, SpanData, SpanId, SpanStatus, Timestamp, TraceId,
};
use relay_protocol::{Annotated, FromValue, Object};

/// convert_from_otel_to_sentry_status returns a status as defined by Sentry based on the OTel status.
fn convert_from_otel_to_sentry_status(
    status_code: Option<i32>,
    http_status_code: Option<i64>,
    grpc_status_code: Option<i64>,
) -> SpanStatus {
    if let Some(status_code) = status_code {
        if status_code == OtelStatusCode::Unset as i32 || status_code == OtelStatusCode::Ok as i32 {
            return SpanStatus::Ok;
        }
    }

    if let Some(code) = http_status_code {
        if let Some(sentry_status) = status_codes::HTTP.get(&code) {
            if let Ok(span_status) = SpanStatus::from_str(sentry_status) {
                return span_status;
            }
        }
    }

    if let Some(code) = grpc_status_code {
        if let Some(sentry_status) = status_codes::GRPC.get(&code) {
            if let Ok(span_status) = SpanStatus::from_str(sentry_status) {
                return span_status;
            }
        }
    }

    SpanStatus::Unknown
}

fn otel_value_to_i64(value: OtelValue) -> Option<i64> {
    match value {
        OtelValue::IntValue(v) => Some(v),
        _ => None,
    }
}

fn otel_value_to_string(value: OtelValue) -> Option<String> {
    match value {
        OtelValue::StringValue(v) => Some(v),
        OtelValue::BoolValue(v) => Some(v.to_string()),
        OtelValue::IntValue(v) => Some(v.to_string()),
        OtelValue::DoubleValue(v) => Some(v.to_string()),
        OtelValue::BytesValue(v) => match String::from_utf8(v) {
            Ok(v) => Some(v),
            Err(_) => None,
        },
        _ => None,
    }
}

fn otel_value_to_span_id(value: OtelValue) -> Option<String> {
    match value {
        OtelValue::StringValue(s) => Some(s),
        OtelValue::BytesValue(b) => Some(hex::encode(b)),
        _ => None,
    }
}

/// Transform an OtelSpan to a Sentry span.
pub fn otel_to_sentry_span(otel_span: OtelSpan) -> EventSpan {
    let mut exclusive_time_ms = 0f64;
    let mut data = Object::new();
    let start_timestamp = Utc.timestamp_nanos(otel_span.start_time_unix_nano as i64);
    let end_timestamp = Utc.timestamp_nanos(otel_span.end_time_unix_nano as i64);
    let OtelSpan {
        trace_id,
        span_id,
        parent_span_id,
        name,
        attributes,
        status,
        kind,
        ..
    } = otel_span;

    let span_id = hex::encode(span_id);
    let trace_id = hex::encode(trace_id);
    let parent_span_id = match parent_span_id.as_slice() {
        &[] => None,
        _ => Some(hex::encode(parent_span_id)),
    };

    let mut op = None;
    let mut description = if name.is_empty() { None } else { Some(name) };
    let mut http_method = None;
    let mut http_route = None;
    let mut http_status_code = None;
    let mut grpc_status_code = None;
    let mut platform = None;
    let mut segment_id = None;
    let mut profile_id = None;
    for attribute in attributes.into_iter() {
        if let Some(value) = attribute.value.and_then(|v| v.value) {
            match attribute.key.as_str() {
                "sentry.op" => {
                    op = otel_value_to_string(value);
                }
                key if key.starts_with("db") => {
                    op = op.or(Some("db".to_string()));
                    if key == "db.statement" {
                        if let Some(statement) = otel_value_to_string(value) {
                            description = Some(statement);
                        }
                    }
                }
                "http.method" | "http.request.method" => {
                    let http_op = match kind {
                        2 => "http.server",
                        3 => "http.client",
                        _ => "http",
                    };
                    op = op.or(Some(http_op.to_string()));
                    http_method = otel_value_to_string(value);
                }
                "http.route" | "url.path" => {
                    http_route = otel_value_to_string(value);
                }
                key if key.contains("exclusive_time_ns") => {
                    let value = match value {
                        OtelValue::IntValue(v) => v as f64,
                        OtelValue::DoubleValue(v) => v,
                        OtelValue::StringValue(v) => v.parse::<f64>().unwrap_or_default(),
                        _ => 0f64,
                    };
                    exclusive_time_ms = value / 1e6f64;
                }
                "http.status_code" => {
                    http_status_code = otel_value_to_i64(value);
                }
                "rpc.grpc.status_code" => {
                    grpc_status_code = otel_value_to_i64(value);
                }
                "sentry.platform" => {
                    platform = otel_value_to_string(value);
                }
                "sentry.segment.id" => {
                    segment_id = otel_value_to_span_id(value);
                }
                "sentry.profile.id" => {
                    profile_id = otel_value_to_span_id(value);
                }
                _other => {
                    let key = attribute.key;
                    match value {
                        OtelValue::ArrayValue(_) => {}
                        OtelValue::BoolValue(v) => {
                            data.insert(key, Annotated::new(v.into()));
                        }
                        OtelValue::BytesValue(v) => {
                            if let Ok(v) = String::from_utf8(v) {
                                data.insert(key, Annotated::new(v.into()));
                            }
                        }
                        OtelValue::DoubleValue(v) => {
                            data.insert(key, Annotated::new(v.into()));
                        }
                        OtelValue::IntValue(v) => {
                            data.insert(key, Annotated::new(v.into()));
                        }
                        OtelValue::KvlistValue(_) => {}
                        OtelValue::StringValue(v) => {
                            data.insert(key, Annotated::new(v.into()));
                        }
                    }
                }
            }
        }
    }
    if exclusive_time_ms == 0f64 {
        exclusive_time_ms =
            (otel_span.end_time_unix_nano - otel_span.start_time_unix_nano) as f64 / 1e6f64;
    }

    if let (Some(http_method), Some(http_route)) = (http_method, http_route) {
        description = Some(format!("{http_method} {http_route}"));
    }

    EventSpan {
        op: op.into(),
        description: description.into(),
        data: SpanData::from_value(Annotated::new(data.into())),
        exclusive_time: exclusive_time_ms.into(),
        parent_span_id: parent_span_id.map(SpanId).into(),
        segment_id: segment_id.map(SpanId).into(),
        span_id: Annotated::new(SpanId(span_id)),
        profile_id: profile_id
            .as_deref()
            .and_then(|s| EventId::from_str(s).ok())
            .into(),
        start_timestamp: Timestamp(start_timestamp).into(),
        status: Annotated::new(convert_from_otel_to_sentry_status(
            status.map(|s| s.code),
            http_status_code,
            grpc_status_code,
        )),
        timestamp: Timestamp(end_timestamp).into(),
        trace_id: TraceId(trace_id).into(),
        platform: platform.into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::get_path;

    #[test]
    fn parse_span() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": 1697620454980000000,
            "endTimeUnixNano": 1697620454980078800,
            "attributes": [
                {
                    "key": "sentry.environment",
                    "value": {
                        "stringValue": "test"
                    }
                },
                {
                "key": "fastify.type",
                    "value": {
                        "stringValue": "middleware"
                    }
                },
                {
                    "key": "plugin.name",
                    "value": {
                        "stringValue": "fastify -> @fastify/multipart"
                    }
                },
                {
                    "key": "hook.name",
                    "value": {
                        "stringValue": "onResponse"
                    }
                },
                {
                    "key": "sentry.sample_rate",
                    "value": {
                        "intValue": 1
                    }
                },
                {
                    "key": "sentry.parentSampled",
                    "value": {
                        "boolValue": true
                    }
                },
                {
                    "key": "sentry.exclusive_time_ns",
                    "value": {
                        "intValue": 1000000000
                    }
                }
            ],
            "droppedAttributesCount": 0,
            "events": [],
            "droppedEventsCount": 0,
            "status": {
                "code": 0,
                "message": "test"
            },
            "links": [],
            "droppedLinksCount": 0
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span);
        assert_eq!(event_span.exclusive_time, Annotated::new(1000.0));
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        assert_eq!(
            get_path!(annotated_span.data.environment),
            Some(&Annotated::new("test".into()))
        );
    }

    #[test]
    fn parse_span_with_exclusive_time_ns_attribute() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": 1697620454980000000,
            "endTimeUnixNano": 1697620454980078800,
            "attributes": [
                {
                    "key": "sentry.exclusive_time_ns",
                    "value": {
                        "intValue": 3200000000
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span);
        assert_eq!(event_span.exclusive_time, Annotated::new(3200.0));
    }

    #[test]
    fn parse_span_no_exclusive_time_ns_attribute() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": 1697620454980000000,
            "endTimeUnixNano": 1697620454980078800
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span);
        assert_eq!(event_span.exclusive_time, Annotated::new(0.0788));
    }

    #[test]
    fn parse_span_with_db_attributes() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "database query",
            "kind": 3,
            "startTimeUnixNano": 1697620454980000000,
            "endTimeUnixNano": 1697620454980078800,
            "attributes": [
                {
                    "key" : "db.name",
                    "value": {
                        "stringValue": "database"
                    }
                },
                {
                    "key" : "db.type",
                    "value": {
                        "stringValue": "sql"
                    }
                },
                {
                    "key" : "db.statement",
                    "value": {
                        "stringValue": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s"
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span);
        assert_eq!(event_span.op, Annotated::new("db".into()));
        assert_eq!(
            event_span.description,
            Annotated::new(
                "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s".into()
            )
        );
    }

    #[test]
    fn parse_span_with_http_attributes() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "http client request",
            "kind": 3,
            "startTimeUnixNano": 1697620454980000000,
            "endTimeUnixNano": 1697620454980078800,
            "attributes": [
                {
                    "key" : "http.request.method",
                    "value": {
                        "stringValue": "GET"
                    }
                },
                {
                    "key" : "url.path",
                    "value": {
                        "stringValue": "/api/search?q=foobar"
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span);
        assert_eq!(event_span.op, Annotated::new("http.client".into()));
        assert_eq!(
            event_span.description,
            Annotated::new("GET /api/search?q=foobar".into())
        );
    }

    /// Intended to be synced with `relay-event-schema::protocol::span::convert::tests::roundtrip`.
    #[test]
    fn parse_sentry_attributes() {
        let json = r#"{
            "traceId": "4c79f60c11214eb38604f4ae0781bfb2",
            "spanId": "fa90fdead5f74052",
            "parentSpanId": "fa90fdead5f74051",
            "startTimeUnixNano": 123000000000,
            "endTimeUnixNano": 123500000000,
            "status": {"code": 0, "message": "foo"},
            "attributes": [
                {
                    "key" : "browser.name",
                    "value": {
                        "stringValue": "Chrome"
                    }
                },
                {
                    "key" : "sentry.environment",
                    "value": {
                        "stringValue": "prod"
                    }
                },
                {
                    "key" : "sentry.op",
                    "value": {
                        "stringValue": "myop"
                    }
                },
                {
                    "key" : "sentry.platform",
                    "value": {
                        "stringValue": "php"
                    }
                },
                {
                    "key" : "sentry.profile.id",
                    "value": {
                        "stringValue": "a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab"
                    }
                },
                {
                    "key" : "sentry.release",
                    "value": {
                        "stringValue": "myapp@1.0.0"
                    }
                },
                {
                    "key" : "sentry.sdk.name",
                    "value": {
                        "stringValue": "sentry.php"
                    }
                },
                {
                    "key" : "sentry.segment.id",
                    "value": {
                        "stringValue": "fa90fdead5f74052"
                    }
                },
                {
                    "key" : "sentry.segment.name",
                    "value": {
                        "stringValue": "my 1st transaction"
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let span_from_otel = otel_to_sentry_span(otel_span);

        // TODO: measurements, metrics_summary

        insta::assert_debug_snapshot!(span_from_otel, @r###"
        Span {
            timestamp: Timestamp(
                1970-01-01T00:02:03.500Z,
            ),
            start_timestamp: Timestamp(
                1970-01-01T00:02:03Z,
            ),
            exclusive_time: 500.0,
            description: ~,
            op: "myop",
            span_id: SpanId(
                "fa90fdead5f74052",
            ),
            parent_span_id: SpanId(
                "fa90fdead5f74051",
            ),
            trace_id: TraceId(
                "4c79f60c11214eb38604f4ae0781bfb2",
            ),
            segment_id: SpanId(
                "fa90fdead5f74052",
            ),
            is_segment: ~,
            status: Ok,
            tags: ~,
            origin: ~,
            profile_id: EventId(
                a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab,
            ),
            data: SpanData {
                app_start_type: ~,
                browser_name: "Chrome",
                code_filepath: ~,
                code_lineno: ~,
                code_function: ~,
                code_namespace: ~,
                db_operation: ~,
                db_system: ~,
                environment: "prod",
                release: LenientString(
                    "myapp@1.0.0",
                ),
                http_decoded_response_content_length: ~,
                http_request_method: ~,
                http_response_content_length: ~,
                http_response_transfer_size: ~,
                resource_render_blocking_status: ~,
                server_address: ~,
                cache_hit: ~,
                cache_item_size: ~,
                http_response_status_code: ~,
                ai_input_messages: ~,
                ai_completion_tokens_used: ~,
                ai_prompt_tokens_used: ~,
                ai_total_tokens_used: ~,
                ai_responses: ~,
                thread_name: ~,
                segment_name: "my 1st transaction",
                ui_component_name: ~,
                url_scheme: ~,
                user: ~,
                replay_id: ~,
                sdk_name: "sentry.php",
                frames_slow: ~,
                frames_frozen: ~,
                frames_total: ~,
                other: {},
            },
            sentry_tags: ~,
            received: ~,
            measurements: ~,
            _metrics_summary: ~,
            platform: "php",
            was_transaction: ~,
            other: {},
        }
        "###);
    }
}
