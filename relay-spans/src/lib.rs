use chrono::{TimeZone, Utc};

use serde::Deserialize;
use serde_repr::Deserialize_repr;

use relay_event_schema::protocol::{Span as EventSpan, SpanId, Timestamp, TraceId};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: Option<String>,
    pub parent_span_id: Option<String>,
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
    pub status: Option<Status>,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Deserialize)]
pub struct Link {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Deserialize_repr)]
#[repr(u32)]
pub enum SpanKind {
    Unspecified = 0,
    Internal = 1,
    Server = 2,
    Client = 3,
    Producer = 4,
    Consumer = 5,
}

impl SpanKind {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SpanKind::Unspecified => "SPAN_KIND_UNSPECIFIED",
            SpanKind::Internal => "SPAN_KIND_INTERNAL",
            SpanKind::Server => "SPAN_KIND_SERVER",
            SpanKind::Client => "SPAN_KIND_CLIENT",
            SpanKind::Producer => "SPAN_KIND_PRODUCER",
            SpanKind::Consumer => "SPAN_KIND_CONSUMER",
        }
    }
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SPAN_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "SPAN_KIND_INTERNAL" => Some(Self::Internal),
            "SPAN_KIND_SERVER" => Some(Self::Server),
            "SPAN_KIND_CLIENT" => Some(Self::Client),
            "SPAN_KIND_PRODUCER" => Some(Self::Producer),
            "SPAN_KIND_CONSUMER" => Some(Self::Consumer),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Status {
    pub message: Option<String>,
    pub code: StatusCode,
}

#[derive(Debug, Deserialize_repr)]
#[repr(u32)]
pub enum StatusCode {
    Unset = 0,
    Ok = 1,
    Error = 2,
}

impl StatusCode {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StatusCode::Unset => "STATUS_CODE_UNSET",
            StatusCode::Ok => "STATUS_CODE_OK",
            StatusCode::Error => "STATUS_CODE_ERROR",
        }
    }
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "STATUS_CODE_UNSET" => Some(Self::Unset),
            "STATUS_CODE_OK" => Some(Self::Ok),
            "STATUS_CODE_ERROR" => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AnyValue {
    pub value: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub enum Value {
    StringValue(String),
    BoolValue(bool),
    IntValue(i64),
    DoubleValue(f64),
    ArrayValue(ArrayValue),
    KvlistValue(KeyValueList),
    BytesValue(Vec<u8>),
}

#[derive(Debug, Deserialize)]
pub struct ArrayValue {
    pub values: Vec<AnyValue>,
}

#[derive(Debug, Deserialize)]
pub struct KeyValueList {
    pub values: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: Option<AnyValue>,
}

#[derive(Debug, Deserialize)]
pub struct InstrumentationScope {
    pub name: String,
    pub version: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

impl From<Span> for EventSpan {
    fn from(from: Span) -> Self {
        let start_timestamp = Utc.timestamp_nanos(from.start_time_unix_nano);
        let end_timestamp = Utc.timestamp_nanos(from.end_time_unix_nano);
        let exclusive_time = (from.end_time_unix_nano - from.start_time_unix_nano) as f64 / 1e6f64;
        let mut span = EventSpan {
            description: from.name.into(),
            exclusive_time: exclusive_time.into(),
            span_id: SpanId(from.span_id).into(),
            start_timestamp: Timestamp(start_timestamp).into(),
            timestamp: Timestamp(end_timestamp).into(),
            trace_id: TraceId(from.trace_id).into(),
            ..Default::default()
        };
        if let Some(parent_span_id) = from.parent_span_id {
            span.is_segment = false.into();
            span.parent_span_id = SpanId(parent_span_id).into();
        } else {
            span.is_segment = true.into();
            span.segment_id = span.span_id.clone();
        }
        span
    }
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
                .sentry_tags
                .value()
                .unwrap()
                .get("environment")
                .unwrap(),
            &Annotated::new("test".to_string())
        );
    }
}
