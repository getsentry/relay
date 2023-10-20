use std::str::FromStr;

use chrono::{TimeZone, Utc};
use serde::Deserialize;
use serde_repr::Deserialize_repr;

use relay_event_schema::protocol::{Span as EventSpan, SpanId, SpanStatus, Timestamp, TraceId};
use relay_protocol::{Annotated, Object, Value};

use crate::status_codes;

/// This is a serde implementation of https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto.
/// A Span represents a single operation performed by a single component of the system.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    /// A unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes OR
    /// of length other than 16 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    ///
    /// This field is required.
    pub trace_id: String,
    /// A unique identifier for a span within a trace, assigned when the span
    /// is created. The ID is an 8-byte array. An ID with all zeroes OR of length
    /// other than 8 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    ///
    /// This field is required.
    pub span_id: String,
    /// trace_state conveys information about request position in multiple distributed tracing graphs.
    /// It is a trace_state in w3c-trace-context format: <https://www.w3.org/TR/trace-context/#tracestate-header>
    /// See also <https://github.com/w3c/distributed-tracing> for more details about this field.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub trace_state: String,
    /// The `span_id` of this span's parent span. If this is a root span, then this
    /// field must be empty. The ID is an 8-byte array.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parent_span_id: String,
    /// A description of the span's operation.
    ///
    /// For example, the name can be a qualified method name or a file name
    /// and a line number where the operation is called. A best practice is to use
    /// the same display name at the same call point in an application.
    /// This makes it easier to correlate spans in different traces.
    ///
    /// This field is semantically required to be set to non-empty string.
    /// Empty value is equivalent to an unknown span name.
    ///
    /// This field is required.
    pub name: String,
    /// Distinguishes between spans generated in a particular context. For example,
    /// two spans with the same name may be distinguished using `CLIENT` (caller)
    /// and `SERVER` (callee) to identify queueing latency associated with the span.
    pub kind: SpanKind,
    /// Timestamp when the span started in nanoseconds.
    pub start_time_unix_nano: i64,
    /// Timestamp when the span ended in nanoseconds.
    pub end_time_unix_nano: i64,
    /// Arbitrary additional data on a span, like `extra` on the top-level event.
    pub attributes: Vec<KeyValue>,
    /// dropped_attributes_count is the number of attributes that were discarded. Attributes
    /// can be discarded because their keys are too long or because there are too many
    /// attributes. If this value is 0, then no attributes were dropped.
    pub dropped_attributes_count: u32,
    /// events is a collection of Event items.``
    pub events: Vec<Event>,
    /// dropped_events_count is the number of dropped events. If the value is 0, then no
    /// events were dropped.
    pub dropped_events_count: u32,
    /// links is a collection of Links, which are references from this span to a span
    /// in the same or different trace.
    pub links: Vec<Link>,
    /// links is a collection of Links, which are references from this span to a span
    /// in the same or different trace.
    pub dropped_links_count: u32,
    /// An optional final status for this span. Semantically when Status isn't set, it means
    /// span's status code is unset, i.e. assume STATUS_CODE_UNSET (code = 0).
    #[serde(default)]
    pub status: Status,
}

impl Span {
    /// sentry_status() returns a status as defined by Sentry based on the span status.
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
            get_path!(event_span.data["sentry.environment"]),
            Some(&Annotated::new("test".into()))
        );
    }
}
