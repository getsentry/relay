use std::str::FromStr;

use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;

use relay_event_schema::protocol::{
    Span as EventSpan, SpanData, SpanId, SpanStatus, Timestamp, TraceId,
};
use relay_protocol::Annotated;

use crate::otel_to_sentry_tags::OTEL_TO_SENTRY_TAGS;
use crate::otel_trace::{status::StatusCode as OtelStatusCode, Span as OtelSpan};
use crate::status_codes;

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

/// Transform an OtelSpan to a Sentry span.
pub fn otel_to_sentry_span(otel_span: OtelSpan) -> EventSpan {
    let mut exclusive_time_ms = 0f64;
    let mut data = SpanData::default();
    let start_timestamp = Utc.timestamp_nanos(otel_span.start_time_unix_nano as i64);
    let end_timestamp = Utc.timestamp_nanos(otel_span.end_time_unix_nano as i64);
    let OtelSpan {
        trace_id,
        span_id,
        parent_span_id,
        name,
        attributes,
        status,
        ..
    } = otel_span;

    let span_id = hex::encode(span_id);
    let trace_id = hex::encode(trace_id);
    let parent_span_id = hex::encode(parent_span_id);

    let segment_id = if parent_span_id.is_empty() {
        Annotated::new(SpanId(span_id.clone()))
    } else {
        Annotated::empty()
    };

    let mut op = None;
    let mut http_status_code = None;
    let mut grpc_status_code = None;
    for attribute in attributes.into_iter() {
        if let Some(value) = attribute.value.and_then(|v| v.value) {
            let key: String = if let Some(key) = OTEL_TO_SENTRY_TAGS.get(attribute.key.as_str()) {
                key.to_string()
            } else {
                attribute.key
            };
            if key == "sentry.op" {
                op = otel_value_to_string(value);
            } else if key.contains("exclusive_time_ns") {
                let value = match value {
                    OtelValue::IntValue(v) => v as f64,
                    OtelValue::DoubleValue(v) => v,
                    OtelValue::StringValue(v) => v.parse::<f64>().unwrap_or_default(),
                    _ => 0f64,
                };
                exclusive_time_ms = value / 1e6f64;
            } else if key == "http.status_code" {
                http_status_code = otel_value_to_i64(value);
            } else if key == "rpc.grpc.status_code" {
                grpc_status_code = otel_value_to_i64(value);
            } else {
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
                };
            }
        }
    }
    if exclusive_time_ms == 0f64 {
        exclusive_time_ms =
            (otel_span.end_time_unix_nano - otel_span.start_time_unix_nano) as f64 / 1e6f64;
    }

    let is_segment = parent_span_id.is_empty().into();

    EventSpan {
        op: op.into(),
        data: data.into(),
        description: name.into(),
        exclusive_time: exclusive_time_ms.into(),
        parent_span_id: SpanId(parent_span_id).into(),
        segment_id,
        span_id: Annotated::new(SpanId(span_id)),
        start_timestamp: Timestamp(start_timestamp).into(),
        status: Annotated::new(convert_from_otel_to_sentry_status(
            status.map(|s| s.code),
            http_status_code,
            grpc_status_code,
        )),
        timestamp: Timestamp(end_timestamp).into(),
        trace_id: TraceId(trace_id).into(),
        is_segment,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{get_path, Annotated};

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
            get_path!(annotated_span.data["environment"]),
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
}
