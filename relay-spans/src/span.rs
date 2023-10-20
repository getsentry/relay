use std::str::FromStr;

use chrono::{TimeZone, Utc};
use serde::Deserialize;
use serde_repr::Deserialize_repr;

use relay_event_schema::protocol::{Span as EventSpan, SpanId, SpanStatus, Timestamp, TraceId};
use relay_protocol::{Annotated, Object, Value};

use crate::status_codes;

/// This is a serde implementation of https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto.

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    /// Trace ID
    pub trace_id: String,
    /// Span ID
    pub span_id: String,
    /// Trace state
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub trace_state: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parent_span_id: String,
    pub name: String,
    pub kind: SpanKind,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub events: Vec<Event>,
    pub dropped_events_count: u32,
    pub links: Vec<Link>,
    pub dropped_links_count: u32,
    #[serde(default)]
    pub status: Status,
}

impl Span {
    pub fn sentry_status(&self) -> &'static str {
        let status_code = self.status.code.clone();

        if status_code == StatusCode::Unset || status_code == StatusCode::Ok {
            return "ok";
        }

        if let Some(code) = self
            .attributes
            .clone()
            .into_iter()
            .find(|a| a.key == "http.status_code")
        {
            if let Some(code_value) = code.value.to_i64() {
                if let Some(sentry_status) = status_codes::HTTP.get(&code_value) {
                    return sentry_status;
                }
            }
        }

        if let Some(code) = self
            .attributes
            .clone()
            .into_iter()
            .find(|a| a.key == "rpc.grpc.status_code")
        {
            if let Some(code_value) = code.value.to_i64() {
                if let Some(sentry_status) = status_codes::GRPC.get(&code_value) {
                    return sentry_status;
                }
            }
        }

        "unknown_error"
    }
}

impl From<Span> for EventSpan {
    fn from(from: Span) -> Self {
        let start_timestamp = Utc.timestamp_nanos(from.start_time_unix_nano);
        let end_timestamp = Utc.timestamp_nanos(from.end_time_unix_nano);
        let exclusive_time = (from.end_time_unix_nano - from.start_time_unix_nano) as f64 / 1e6f64;
        let mut attributes: Object<Value> = Object::new();
        for attribute in from.attributes.clone() {
            match attribute.value {
                AnyValue::Array(_) => todo!(),
                AnyValue::Bool(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
                AnyValue::Bytes(_) => todo!(),
                AnyValue::Double(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
                AnyValue::Int(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
                AnyValue::Kvlist(_) => todo!(),
                AnyValue::String(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
            };
        }
        let mut span = EventSpan {
            data: attributes.into(),
            description: from.name.clone().into(),
            exclusive_time: exclusive_time.into(),
            span_id: SpanId(from.span_id.clone()).into(),
            parent_span_id: SpanId(from.parent_span_id.clone()).into(),
            start_timestamp: Timestamp(start_timestamp).into(),
            is_segment: from.parent_span_id.is_empty().into(),
            timestamp: Timestamp(end_timestamp).into(),
            trace_id: TraceId(from.trace_id.clone()).into(),
            ..Default::default()
        };
        if let Ok(status) = SpanStatus::from_str(from.sentry_status()) {
            span.status = status.into();
        }
        if from.parent_span_id.is_empty() {
            span.segment_id = span.span_id.clone();
        }
        span
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Event {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Link {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

#[derive(Clone, Debug, Deserialize_repr)]
#[repr(u32)]
pub enum SpanKind {
    Unspecified = 0,
    Internal = 1,
    Server = 2,
    Client = 3,
    Producer = 4,
    Consumer = 5,
}

#[derive(Clone, Debug, Deserialize, Default)]
pub struct Status {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    pub code: StatusCode,
}

#[derive(Clone, Debug, Deserialize_repr, Default, PartialEq)]
#[repr(u32)]
pub enum StatusCode {
    #[default]
    Unset = 0,
    Ok = 1,
    Error = 2,
}

#[derive(Clone, Debug, Deserialize)]
pub enum AnyValue {
    #[serde(rename = "arrayValue")]
    Array(ArrayValue),
    #[serde(rename = "boolValue")]
    Bool(bool),
    #[serde(rename = "bytesValue")]
    Bytes(Vec<u8>),
    #[serde(rename = "doubleValue")]
    Double(f64),
    #[serde(rename = "intValue")]
    Int(i64),
    #[serde(rename = "kvlistValue")]
    Kvlist(KeyValueList),
    #[serde(rename = "stringValue")]
    String(String),
}

impl AnyValue {
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            AnyValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            AnyValue::String(v) => Some(v.clone()),
            AnyValue::Bool(v) => Some(v.to_string()),
            AnyValue::Int(v) => Some(v.to_string()),
            AnyValue::Double(v) => Some(v.to_string()),
            AnyValue::Bytes(v) => match String::from_utf8(v.clone()) {
                Ok(v) => Some(v),
                Err(_) => None,
            },
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ArrayValue {
    pub values: Vec<AnyValue>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct KeyValueList {
    pub values: Vec<KeyValue>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: AnyValue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::Annotated;

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
                }
            ],
            "droppedAttributesCount": 0,
            "events": [],
            "droppedEventsCount": 0,
            "status": {
                "code": 0
            },
            "links": [],
            "droppedLinksCount": 0
        }"#;
        let otel_span: Span = serde_json::from_str(json).unwrap();
        let event_span: Annotated<EventSpan> = Annotated::new(otel_span.into());
        assert_eq!(
            event_span
                .value()
                .unwrap()
                .data
                .value()
                .unwrap()
                .get("sentry.environment")
                .unwrap()
                .as_str(),
            Some("test")
        );
    }
}
