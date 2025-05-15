use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;
use opentelemetry_proto::tonic::trace::v1::span::Link as OtelLink;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind as OtelSpanKind;
use relay_event_schema::protocol::SpanKind;

use crate::otel_trace::{
    Span as OtelSpan, SpanFlags as OtelSpanFlags, status::StatusCode as OtelStatusCode,
};
use crate::status_codes;
use relay_event_schema::protocol::{
    EventId, Span as EventSpan, SpanData, SpanId, SpanLink, SpanStatus, Timestamp, TraceId,
};
use relay_protocol::{Annotated, Error, FromValue, Object, Value};

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
        OtelValue::BytesValue(v) => String::from_utf8(v).ok(),
        _ => None,
    }
}

fn otel_value_to_span_id(value: OtelValue) -> Option<String> {
    let decoded = match value {
        OtelValue::StringValue(s) => hex::decode(s).ok()?,
        OtelValue::BytesValue(b) => b,
        _ => None?,
    };
    Some(hex::encode(decoded))
}

fn otel_flags_is_remote(value: u32) -> Option<bool> {
    if value & OtelSpanFlags::ContextHasIsRemoteMask as u32 == 0 {
        None
    } else {
        Some(value & OtelSpanFlags::ContextIsRemoteMask as u32 != 0)
    }
}

/// Transform an OtelSpan to a Sentry span.
pub fn otel_to_sentry_span(otel_span: OtelSpan) -> Result<EventSpan, Error> {
    let mut exclusive_time_ms = 0f64;
    let mut data = Object::new();
    let start_timestamp = Utc.timestamp_nanos(otel_span.start_time_unix_nano as i64);
    let end_timestamp = Utc.timestamp_nanos(otel_span.end_time_unix_nano as i64);
    let OtelSpan {
        trace_id,
        span_id,
        parent_span_id,
        flags,
        name,
        kind,
        attributes,
        status,
        links,
        ..
    } = otel_span;

    let span_id = hex::encode(span_id);
    let trace_id: TraceId = hex::encode(trace_id).parse()?;
    let parent_span_id = match parent_span_id.as_slice() {
        &[] => None,
        _ => Some(hex::encode(parent_span_id)),
    };

    let mut op = if name.is_empty() { None } else { Some(name) };
    let mut description = None;
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
                "sentry.description" => {
                    description = otel_value_to_string(value);
                }
                key if key.starts_with("db") => {
                    op = op.or(Some("db".to_string()));
                    if key == "db.statement" {
                        description = description.or_else(|| otel_value_to_string(value));
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
                key if key.contains("exclusive_time_nano") => {
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
                    profile_id = otel_value_to_string(value);
                }
                _ => {
                    let key = attribute.key;
                    if let Some(v) = otel_to_sentry_value(value) {
                        data.insert(key, Annotated::new(v));
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
        description = description.or(Some(format!("{http_method} {http_route}")));
    }

    let sentry_links: Vec<Annotated<SpanLink>> = links
        .into_iter()
        .map(|link| otel_to_sentry_link(link).map(Into::into))
        .collect::<Result<_, _>>()?;

    let event_span = EventSpan {
        op: op.into(),
        description: description.into(),
        data: SpanData::from_value(Annotated::new(data.into())),
        exclusive_time: exclusive_time_ms.into(),
        parent_span_id: parent_span_id.map(SpanId).into(),
        segment_id: segment_id.map(SpanId).into(),
        span_id: Annotated::new(SpanId(span_id)),
        is_remote: Annotated::from(otel_flags_is_remote(flags)),
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
        trace_id: Annotated::new(trace_id),
        platform: platform.into(),
        kind: OtelSpanKind::try_from(kind)
            .map_or(SpanKind::Unspecified, SpanKind::from)
            .into(),
        links: sentry_links.into(),
        ..Default::default()
    };

    Ok(event_span)
}

fn otel_to_sentry_link(otel_link: OtelLink) -> Result<SpanLink, Error> {
    // See the W3C trace context specification:
    // <https://www.w3.org/TR/trace-context-2/#sampled-flag>
    const W3C_TRACE_CONTEXT_SAMPLED: u32 = 1 << 0;

    let attributes = BTreeMap::from_iter(otel_link.attributes.into_iter().flat_map(|kv| {
        kv.value
            .and_then(|v| v.value)
            .and_then(otel_to_sentry_value)
            .map(|v| (kv.key, v.into()))
    }))
    .into();

    let span_link = SpanLink {
        trace_id: Annotated::new(hex::encode(otel_link.trace_id).parse()?),
        span_id: SpanId(hex::encode(otel_link.span_id)).into(),
        sampled: (otel_link.flags & W3C_TRACE_CONTEXT_SAMPLED != 0).into(),
        attributes,
        other: Default::default(),
    };

    Ok(span_link)
}

fn otel_to_sentry_value(value: OtelValue) -> Option<Value> {
    match value {
        OtelValue::BoolValue(v) => Some(Value::Bool(v)),
        OtelValue::DoubleValue(v) => Some(Value::F64(v)),
        OtelValue::IntValue(v) => Some(Value::I64(v)),
        OtelValue::StringValue(v) => Some(Value::String(v)),
        OtelValue::BytesValue(v) => {
            String::from_utf8(v).map_or(None, |str| Some(Value::String(str)))
        }
        OtelValue::ArrayValue(_) => None,
        OtelValue::KvlistValue(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{SerializableAnnotated, get_path};

    #[test]
    fn parse_span() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800",
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
                        "intValue": "1"
                    }
                },
                {
                    "key": "sentry.parentSampled",
                    "value": {
                        "boolValue": true
                    }
                },
                {
                    "key": "sentry.exclusive_time_nano",
                    "value": {
                        "intValue": "1000000000"
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.exclusive_time, Annotated::new(1000.0));
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        assert_eq!(
            get_path!(annotated_span.data.environment),
            Some(&Annotated::new("test".into()))
        );
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 1000.0,
          "op": "middleware - fastify -> @fastify/multipart",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "ok",
          "data": {
            "sentry.environment": "test",
            "fastify.type": "middleware",
            "hook.name": "onResponse",
            "plugin.name": "fastify -> @fastify/multipart",
            "sentry.parentSampled": true,
            "sentry.sample_rate": 1
          },
          "links": [],
          "kind": "internal"
        }
        "###);
    }

    #[test]
    fn parse_span_with_exclusive_time_nano_attribute() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800",
            "attributes": [
                {
                    "key": "sentry.exclusive_time_nano",
                    "value": {
                        "intValue": "3200000000"
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.exclusive_time, Annotated::new(3200.0));
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 3200.0,
          "op": "middleware - fastify -> @fastify/multipart",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "internal"
        }
        "###);
    }

    #[test]
    fn parse_span_no_exclusive_time_nano_attribute() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "middleware - fastify -> @fastify/multipart",
            "kind": 1,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800"
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.exclusive_time, Annotated::new(0.0788));
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "middleware - fastify -> @fastify/multipart",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "internal"
        }
        "###);
    }

    #[test]
    fn parse_span_with_db_attributes() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "database query",
            "kind": 3,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800",
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.op, Annotated::new("database query".into()));
        assert_eq!(
            event_span.description,
            Annotated::new(
                "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s".into()
            )
        );
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "database query",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
          "data": {},
          "links": [],
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_span_with_db_attributes_and_description() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "database query",
            "kind": 3,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800",
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
                },
                {
                    "key": "sentry.description",
                    "value": {
                        "stringValue": "index view query"
                    }
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.op, Annotated::new("database query".into()));
        assert_eq!(
            event_span.description,
            Annotated::new("index view query".into())
        );
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "database query",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "index view query",
          "data": {},
          "links": [],
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn parse_span_with_http_attributes() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "name": "http client request",
            "kind": 3,
            "startTimeUnixNano": "1697620454980000000",
            "endTimeUnixNano": "1697620454980078800",
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
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.op, Annotated::new("http client request".into()));
        assert_eq!(
            event_span.description,
            Annotated::new("GET /api/search?q=foobar".into())
        );
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 1697620454.980079,
          "start_timestamp": 1697620454.98,
          "exclusive_time": 0.0788,
          "op": "http client request",
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "description": "GET /api/search?q=foobar",
          "data": {},
          "links": [],
          "kind": "client"
        }
        "###);
    }

    /// Intended to be synced with `relay-event-schema::protocol::span::convert::tests::roundtrip`.
    #[test]
    fn parse_sentry_attributes() {
        let json = r#"{
            "traceId": "4c79f60c11214eb38604f4ae0781bfb2",
            "spanId": "fa90fdead5f74052",
            "parentSpanId": "fa90fdead5f74051",
            "startTimeUnixNano": "123000000000",
            "endTimeUnixNano": "123500000000",
            "name": "myname",
            "status": {"code": 0, "message": "foo"},
            "attributes": [
                {
                    "key" : "browser.name",
                    "value": {
                        "stringValue": "Chrome"
                    }
                },
                {
                    "key" : "sentry.description",
                    "value": {
                        "stringValue": "mydescription"
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
                        "stringValue": "FA90FDEAD5F74052"
                    }
                },
                {
                    "key" : "sentry.segment.name",
                    "value": {
                        "stringValue": "my 1st transaction"
                    }
                },
                {
                    "key": "sentry.metrics_summary.some_metric",
                    "value": {
                        "arrayValue": {
                            "values": [
                                {
                                    "kvlistValue": {
                                        "values": [
                                            {
                                                "key": "min",
                                                "value": {
                                                    "doubleValue": 1.0
                                                }
                                            },
                                            {
                                                "key": "max",
                                                "value": {
                                                    "doubleValue": 2.0
                                                }
                                            },
                                            {
                                                "key": "sum",
                                                "value": {
                                                    "doubleValue": 3.0
                                                }
                                            },
                                            {
                                                "key": "count",
                                                "value": {
                                                    "intValue": "2"
                                                }
                                            },
                                            {
                                                "key": "tags",
                                                "value": {
                                                    "kvlistValue": {
                                                        "values": [
                                                            {
                                                                "key": "environment",
                                                                "value": {
                                                                    "stringValue": "test"
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            ]
        }"#;

        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span = otel_to_sentry_span(otel_span).unwrap();

        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "op": "myname",
          "span_id": "fa90fdead5f74052",
          "parent_span_id": "fa90fdead5f74051",
          "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
          "segment_id": "fa90fdead5f74052",
          "status": "ok",
          "description": "mydescription",
          "profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
          "data": {
            "browser.name": "Chrome",
            "sentry.environment": "prod",
            "sentry.release": "myapp@1.0.0",
            "sentry.segment.name": "my 1st transaction",
            "sentry.sdk.name": "sentry.php",
            "sentry.op": "myop"
          },
          "links": [],
          "platform": "php",
          "kind": "unspecified"
        }
        "###);
    }

    #[test]
    fn parse_span_is_remote() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "startTimeUnixNano": "123000000000",
            "endTimeUnixNano": "123500000000",
            "flags": 768
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.is_remote, Annotated::new(true));
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "is_remote": true,
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "unspecified"
        }
        "###);
    }

    #[test]
    fn parse_span_is_not_remote() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "startTimeUnixNano": "123000000000",
            "endTimeUnixNano": "123500000000",
            "flags": 256
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        assert_eq!(event_span.is_remote, Annotated::new(false));
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "is_remote": false,
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "unspecified"
        }
        "###);
    }

    #[test]
    fn extract_span_kind() {
        let json = r#"{
            "traceId": "89143b0763095bd9c9955e8175d1fb23",
            "spanId": "e342abb1214ca181",
            "parentSpanId": "0c7a7dea069bf5a6",
            "startTimeUnixNano": "123000000000",
            "endTimeUnixNano": "123500000000",
            "kind": 3
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let kind = event_span.kind.value().expect("kind should be set");
        assert_eq!(kind, &SpanKind::Client);
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 123.5,
          "start_timestamp": 123.0,
          "exclusive_time": 500.0,
          "span_id": "e342abb1214ca181",
          "parent_span_id": "0c7a7dea069bf5a6",
          "trace_id": "89143b0763095bd9c9955e8175d1fb23",
          "status": "unknown",
          "data": {},
          "links": [],
          "kind": "client"
        }
        "###);
    }

    #[test]
    fn uppercase_span_id() {
        let input = OtelValue::StringValue("FA90FDEAD5F74052".to_owned());
        assert_eq!(
            otel_value_to_span_id(input).as_deref(),
            Some("fa90fdead5f74052")
        );
    }

    #[test]
    fn parse_link() {
        let json = r#"{
            "traceId": "3c79f60c11214eb38604f4ae0781bfb2",
            "links": [
                {
                    "traceId": "4c79f60c11214eb38604f4ae0781bfb2",
                    "spanId": "fa90fdead5f74052",
                    "attributes": [
                        {
                            "key": "str_key",
                            "value": {
                                "stringValue": "str_value"
                            }
                        },
                        {
                            "key": "bool_key",
                            "value": {
                                "boolValue": true
                            }
                        },
                        {
                            "key": "int_key",
                            "value": {
                                "intValue": "123"
                            }
                        },
                        {
                            "key": "double_key",
                            "value": {
                                "doubleValue": 1.23
                            }
                        }
                    ],
                    "flags": 1
                }
            ]
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_to_sentry_span(otel_span).unwrap();
        let annotated_span: Annotated<EventSpan> = Annotated::new(event_span);

        assert_eq!(
            get_path!(annotated_span.trace_id),
            Some(&Annotated::new(
                "3c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()
            ))
        );
        assert_eq!(
            get_path!(annotated_span.links[0].trace_id),
            Some(&Annotated::new(
                "4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()
            ))
        );
        assert_eq!(
            get_path!(annotated_span.links[0].span_id),
            Some(&Annotated::new(SpanId("fa90fdead5f74052".into())))
        );
        assert_eq!(
            get_path!(annotated_span.links[0].attributes["str_key"]),
            Some(&Annotated::new(Value::String("str_value".into())))
        );
        assert_eq!(
            get_path!(annotated_span.links[0].attributes["bool_key"]),
            Some(&Annotated::new(Value::Bool(true)))
        );
        assert_eq!(
            get_path!(annotated_span.links[0].attributes["int_key"]),
            Some(&Annotated::new(Value::I64(123)))
        );
        assert_eq!(
            get_path!(annotated_span.links[0].attributes["double_key"]),
            Some(&Annotated::new(Value::F64(1.23)))
        );
        assert_eq!(
            get_path!(annotated_span.links[0].sampled),
            Some(&Annotated::new(true))
        );
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_span), @r###"
        {
          "timestamp": 0.0,
          "start_timestamp": 0.0,
          "exclusive_time": 0.0,
          "span_id": "",
          "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
          "status": "unknown",
          "data": {},
          "links": [
            {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74052",
              "sampled": true,
              "attributes": {
                "bool_key": true,
                "double_key": 1.23,
                "int_key": 123,
                "str_key": "str_value"
              }
            }
          ],
          "kind": "unspecified"
        }
        "###);
    }
}
