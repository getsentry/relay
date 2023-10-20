use std::str::FromStr;

use chrono::{TimeZone, Utc};
use serde::Deserialize;
use serde_repr::Deserialize_repr;

use relay_event_schema::protocol::{Span as EventSpan, SpanId, SpanStatus, Timestamp, TraceId};
use relay_protocol::{Annotated, Object, Value};

mod status_codes;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub trace_state: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
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
    #[serde(default)]
    pub status: Status,
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

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AnyValue {
    ArrayValue(ArrayValue),
    BoolValue(bool),
    BytesValue(Vec<u8>),
    DoubleValue(f64),
    IntValue(i64),
    KvlistValue(KeyValueList),
    StringValue(String),
}

impl AnyValue {
    pub fn to_i64(self) -> Option<i64> {
        match self {
            AnyValue::IntValue(v) => Some(v),
            _ => None,
        }
    }

    pub fn to_string(self) -> Option<String> {
        match self {
            AnyValue::StringValue(v) => Some(v),
            AnyValue::BoolValue(v) => Some(v.to_string()),
            AnyValue::IntValue(v) => Some(v.to_string()),
            AnyValue::DoubleValue(v) => Some(v.to_string()),
            AnyValue::BytesValue(v) => match String::from_utf8(v) {
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
        let mut attributes: Object<Value> = Object::new();
        for attribute in from.attributes.clone() {
            match attribute.value {
                AnyValue::ArrayValue(_) => todo!(),
                AnyValue::BoolValue(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
                AnyValue::BytesValue(_) => todo!(),
                AnyValue::DoubleValue(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
                AnyValue::IntValue(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
                AnyValue::KvlistValue(_) => todo!(),
                AnyValue::StringValue(v) => {
                    attributes.insert(attribute.key, Annotated::new(v.into()));
                }
            };
        }
        let mut span = EventSpan {
            data: attributes.into(),
            description: from.name.clone().into(),
            exclusive_time: exclusive_time.into(),
            span_id: SpanId(from.span_id.clone()).into(),
            start_timestamp: Timestamp(start_timestamp).into(),
            timestamp: Timestamp(end_timestamp).into(),
            trace_id: TraceId(from.trace_id.clone()).into(),
            ..Default::default()
        };
        if let Ok(status) = SpanStatus::from_str(from.sentry_status()) {
            span.status = status.into();
        }
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
