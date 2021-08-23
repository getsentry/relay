use crate::protocol::{JsonLenientString, OperationType, SpanId, SpanStatus, Timestamp, TraceId};
use crate::types::{Annotated, Object, Value};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_span", value_type = "Span")]
pub struct Span {
    /// Timestamp when the span was ended.
    #[metastructure(required = "true")]
    pub timestamp: Annotated<Timestamp>,

    /// Timestamp when the span started.
    #[metastructure(required = "true")]
    pub start_timestamp: Annotated<Timestamp>,

    pub exclusive_time: Annotated<f64>,

    /// Human readable description of a span (e.g. method URL).
    #[metastructure(pii = "maybe")]
    pub description: Annotated<String>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = "enumlike")]
    pub op: Annotated<OperationType>,

    /// The Span id.
    #[metastructure(required = "true")]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// The ID of the trace the span belongs to.
    #[metastructure(required = "true")]
    pub trace_id: Annotated<TraceId>,

    /// The status of a span
    pub status: Annotated<SpanStatus>,

    /// Arbitrary tags on a span, like on the top-level event.
    #[metastructure(pii = "maybe")]
    pub tags: Annotated<Object<JsonLenientString>>,

    /// Arbitrary additional data on a span, like `extra` on the top-level event.
    #[metastructure(pii = "maybe")]
    pub data: Annotated<Object<Value>>,

    // TODO remove retain when the api stabilizes
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_span_serialization() {
        let json = r#"{
  "timestamp": 0.0,
  "start_timestamp": -63158400.0,
  "exclusive_time": 1.23,
  "description": "desc",
  "op": "operation",
  "span_id": "fa90fdead5f74052",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "status": "ok"
}"#;

        let span = Annotated::new(Span {
            timestamp: Annotated::new(Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(1968, 1, 1).and_hms_nano(0, 0, 0, 0).into()),
            exclusive_time: Annotated::new(1.23),
            description: Annotated::new("desc".to_owned()),
            op: Annotated::new("operation".to_owned()),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
            status: Annotated::new(SpanStatus::Ok),
            ..Default::default()
        });
        assert_eq_str!(json, span.to_json_pretty().unwrap());

        let span_from_string = Annotated::from_json(json).unwrap();
        assert_eq_dbg!(span, span_from_string);
    }
}
