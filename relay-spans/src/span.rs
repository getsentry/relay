use std::str::FromStr;

use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use relay_event_schema::protocol::{Span as EventSpan, SpanId, SpanStatus, Timestamp, TraceId};
use relay_protocol::{Annotated, Object, Value};

use crate::otel_to_sentry_tags::OTEL_TO_SENTRY_TAGS;
use crate::status_codes;

/// This is a serde implementation of <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>.
/// A Span represents a single operation performed by a single component of the system.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OtelSpan {
    /// A unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes OR
    /// of length other than 16 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    pub trace_id: String,
    /// A unique identifier for a span within a trace, assigned when the span
    /// is created. The ID is an 8-byte array. An ID with all zeroes OR of length
    /// other than 8 bytes is considered invalid (empty string in OTLP/JSON
    /// is zero-length and thus is also invalid).
    pub span_id: String,
    /// trace_state conveys information about request position in multiple distributed tracing graphs.
    /// It is a trace_state in w3c-trace-context format: <https://www.w3.org/TR/trace-context/#tracestate-header>.
    /// See also <https://github.com/w3c/distributed-tracing> for more details about this field.
    #[serde(default)]
    pub trace_state: String,
    /// The `span_id` of this span's parent span. If this is a root span, then this
    /// field must be empty. The ID is an 8-byte array.
    #[serde(default)]
    pub parent_span_id: String,
    /// Flags, a bit field. 8 least significant bits are the trace
    /// flags as defined in W3C Trace Context specification. Readers
    /// MUST not assume that 24 most significant bits will be zero.
    /// To read the 8-bit W3C trace flag, use `flags & SPAN_FLAGS_TRACE_FLAGS_MASK`.
    ///
    /// When creating span messages, if the message is logically forwarded from another source
    /// with an equivalent flags fields (i.e., usually another OTLP span message), the field SHOULD
    /// be copied as-is. If creating from a source that does not have an equivalent flags field
    /// (such as a runtime representation of an OpenTelemetry span), the high 24 bits MUST
    /// be set to zero.
    ///
    /// See <https://www.w3.org/TR/trace-context-2/#trace-flags> for the flag definitions.
    #[serde(default)]
    pub flags: u32,
    /// A description of the span's operation.
    ///
    /// For example, the name can be a qualified method name or a file name
    /// and a line number where the operation is called. A best practice is to use
    /// the same display name at the same call point in an application.
    /// This makes it easier to correlate spans in different traces.
    ///
    /// This field is semantically required to be set to non-empty string.
    /// Empty value is equivalent to an unknown span name.
    pub name: String,
    /// Distinguishes between spans generated in a particular context. For example,
    /// two spans with the same name may be distinguished using `CLIENT` (caller)
    /// and `SERVER` (callee) to identify queueing latency associated with the span.
    #[serde(default)]
    pub kind: SpanKind,
    /// start_time_unix_nano is the start time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution starts. On the server side, this
    /// is the time when the server's application handler starts running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    ///
    /// This field is semantically required and it is expected that end_time >= start_time.
    pub start_time_unix_nano: i64,
    /// end_time_unix_nano is the end time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution ends. On the server side, this
    /// is the time when the server application handler stops running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    ///
    /// This field is semantically required and it is expected that end_time >= start_time.
    pub end_time_unix_nano: i64,
    /// attributes is a collection of key/value pairs. Note, global attributes
    /// like server name can be set using the resource API.
    ///
    /// The OpenTelemetry API specification further restricts the allowed value types:
    /// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/common/README.md#attribute>
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    /// dropped_attributes_count is the number of attributes that were discarded. Attributes
    /// can be discarded because their keys are too long or because there are too many
    /// attributes. If this value is 0, then no attributes were dropped.
    #[serde(default)]
    pub dropped_attributes_count: u32,
    /// events is a collection of Event items.``
    #[serde(default)]
    pub events: Vec<Event>,
    /// dropped_events_count is the number of dropped events. If the value is 0, then no
    /// events were dropped.
    #[serde(default)]
    pub dropped_events_count: u32,
    /// links is a collection of Links, which are references from this span to a span
    /// in the same or different trace.
    #[serde(default)]
    pub links: Vec<Link>,
    /// links is a collection of Links, which are references from this span to a span
    /// in the same or different trace.
    #[serde(default)]
    pub dropped_links_count: u32,
    /// An optional final status for this span. Semantically when Status isn't set, it means
    /// span's status code is unset, i.e. assume STATUS_CODE_UNSET (code = 0).
    #[serde(default)]
    pub status: Status,
}

/// convert_from_otel_to_sentry_status returns a status as defined by Sentry based on the OTel status.
fn convert_from_otel_to_sentry_status(
    status_code: StatusCode,
    http_status_code: Option<i64>,
    grpc_status_code: Option<i64>,
) -> SpanStatus {
    if status_code == StatusCode::Unset || status_code == StatusCode::Ok {
        return SpanStatus::Ok;
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

impl From<OtelSpan> for EventSpan {
    fn from(from: OtelSpan) -> Self {
        let mut exclusive_time_ms = 0f64;
        let mut data: Object<Value> = Object::new();
        let start_timestamp = Utc.timestamp_nanos(from.start_time_unix_nano);
        let end_timestamp = Utc.timestamp_nanos(from.end_time_unix_nano);
        let OtelSpan {
            trace_id,
            span_id,
            parent_span_id,
            name,
            attributes,
            status,
            ..
        } = from;
        let segment_id = if parent_span_id.is_empty() {
            Annotated::new(SpanId(span_id.clone()))
        } else {
            Annotated::empty()
        };

        let mut http_status_code = None;
        let mut grpc_status_code = None;
        for attribute in attributes.into_iter() {
            let key: String = if let Some(key) = OTEL_TO_SENTRY_TAGS.get(attribute.key.as_str()) {
                key.to_string()
            } else {
                attribute.key
            };
            if key.contains("exclusive_time_ns") {
                let value = match attribute.value {
                    AnyValue::Int(v) => v as f64,
                    AnyValue::Double(v) => v,
                    AnyValue::String(v) => v.parse::<f64>().unwrap_or_default(),
                    _ => 0f64,
                };
                exclusive_time_ms = value / 1e6f64;
                continue;
            }
            if key == "http.status_code" {
                http_status_code = attribute.value.to_i64();
            } else if key == "rpc.grpc.status_code" {
                grpc_status_code = attribute.value.to_i64();
            }
            match attribute.value {
                AnyValue::Array(_) => {}
                AnyValue::Bool(v) => {
                    data.insert(key, Annotated::new(v.into()));
                }
                AnyValue::Bytes(v) => {
                    if let Ok(v) = String::from_utf8(v) {
                        data.insert(key, Annotated::new(v.into()));
                    }
                }
                AnyValue::Double(v) => {
                    data.insert(key, Annotated::new(v.into()));
                }
                AnyValue::Int(v) => {
                    data.insert(key, Annotated::new(v.into()));
                }
                AnyValue::Kvlist(_) => {}
                AnyValue::String(v) => {
                    data.insert(key, Annotated::new(v.into()));
                }
            };
        }
        if exclusive_time_ms == 0f64 {
            exclusive_time_ms =
                (from.end_time_unix_nano - from.start_time_unix_nano) as f64 / 1e6f64;
        }

        EventSpan {
            data: data.into(),
            description: name.into(),
            exclusive_time: exclusive_time_ms.into(),
            parent_span_id: SpanId(parent_span_id).into(),
            segment_id,
            span_id: Annotated::new(SpanId(span_id)),
            start_timestamp: Timestamp(start_timestamp).into(),
            status: Annotated::new(convert_from_otel_to_sentry_status(
                status.code,
                http_status_code,
                grpc_status_code,
            )),
            timestamp: Timestamp(end_timestamp).into(),
            trace_id: TraceId(trace_id).into(),
            ..Default::default()
        }
    }
}

/// Event is a time-stamped annotation of the span, consisting of user-supplied
/// text description and key-value pairs.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// attributes is a collection of attribute key/value pairs on the event.
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    /// dropped_attributes_count is the number of dropped attributes. If the value is 0,
    /// then no attributes were dropped.
    #[serde(default)]
    pub dropped_attributes_count: u32,
    /// name of the event.
    /// This field is semantically required to be set to non-empty string.
    pub name: String,
    /// time_unix_nano is the time the event occurred.
    #[serde(default)]
    pub time_unix_nano: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    /// attributes is a collection of attribute key/value pairs on the link.
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    /// dropped_attributes_count is the number of dropped attributes. If the value is 0,
    /// then no attributes were dropped.
    #[serde(default)]
    pub dropped_attributes_count: u32,
    /// A unique identifier for the linked span. The ID is an 8-byte array.
    #[serde(default)]
    pub span_id: String,
    /// A unique identifier of a trace that this linked span is part of. The ID is a
    /// 16-byte array.
    #[serde(default)]
    pub trace_id: String,
    /// The trace_state associated with the link.
    #[serde(default)]
    pub trace_state: String,
}

#[derive(Clone, Default, Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum SpanKind {
    /// Unspecified. Do NOT use as default.
    /// Implementations MAY assume SpanKind to be INTERNAL when receiving UNSPECIFIED.
    Unspecified = 0,
    /// Indicates that the span represents an internal operation within an application,
    /// as opposed to an operation happening at the boundaries. Default value.
    #[default]
    Internal = 1,
    /// Indicates that the span covers server-side handling of an RPC or other
    /// remote network request.
    Server = 2,
    /// Indicates that the span describes a request to some remote service.
    Client = 3,
    /// Indicates that the span describes a producer sending a message to a broker.
    /// Unlike CLIENT and SERVER, there is often no direct critical path latency relationship
    /// between producer and consumer spans. A PRODUCER span ends when the message was accepted
    /// by the broker while the logical processing of the message might span a much longer time.
    Producer = 4,
    /// Indicates that the span describes consumer receiving a message from a broker.
    /// Like the PRODUCER kind, there is often no direct critical path latency relationship
    /// between producer and consumer spans.
    Consumer = 5,
}

#[derive(Clone, Debug, Deserialize, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    /// A developer-facing human readable error message.
    #[serde(default)]
    pub message: String,
    /// The status code.
    #[serde(default)]
    pub code: StatusCode,
}

#[derive(Clone, Copy, Debug, Deserialize_repr, Default, PartialEq, Serialize_repr)]
#[repr(u8)]
pub enum StatusCode {
    /// The default status.
    #[default]
    Unset = 0,
    /// The Span has been validated by an Application developer or Operator to
    /// have completed successfully.
    Ok = 1,
    /// The Span contains an error.
    Error = 2,
}

/// AnyValue is used to represent any type of attribute value. AnyValue may contain a
/// primitive value such as a string or integer or it may contain an arbitrary nested
/// object containing arrays, key-value lists and primitives.
/// The value is one of the listed fields. It is valid for all values to be unspecified
/// in which case this AnyValue is considered to be "empty".
#[derive(Clone, Debug, Deserialize, Serialize)]
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

/// ArrayValue is a list of AnyValue messages. We need ArrayValue as a message
/// since oneof in AnyValue does not allow repeated fields.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArrayValue {
    /// Array of values. The array may be empty (contain 0 elements).
    #[serde(default)]
    pub values: Vec<AnyValue>,
}

/// KeyValueList is a list of KeyValue messages. We need KeyValueList as a message
///
/// since `oneof` in AnyValue does not allow repeated fields. Everywhere else where we need
/// a list of KeyValue messages (e.g. in Span) we use `repeated KeyValue` directly to
/// avoid unnecessary extra wrapping (which slows down the protocol). The 2 approaches
/// are semantically equivalent.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KeyValueList {
    /// A collection of key/value pairs of key-value pairs. The list may be empty (may
    /// contain 0 elements).
    /// The keys MUST be unique (it is not allowed to have more than one
    /// value with the same key).
    pub values: Vec<KeyValue>,
}

/// KeyValue is a key-value pair that is used to store Span attributes, Link
/// attributes, etc.
#[derive(Clone, Debug, Deserialize, Serialize)]
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
                "code": 0
            },
            "links": [],
            "droppedLinksCount": 0
        }"#;
        let otel_span: OtelSpan = serde_json::from_str(json).unwrap();
        let event_span: EventSpan = otel_span.into();
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
        let event_span: EventSpan = otel_span.into();
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
        let event_span: EventSpan = otel_span.into();
        assert_eq!(event_span.exclusive_time, Annotated::new(0.0788));
    }
}
