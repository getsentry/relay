use chrono::{DateTime, Utc};

use crate::protocol::{OperationType, SpanId, TraceId};
use crate::types::{Annotated, Object, Value};

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Empty,
    FromValue,
    ToValue,
    ProcessValue,
    SchemaAttributes,
    PiiAttributes,
    TrimmingAttributes,
)]
#[metastructure(process_func = "process_span", value_type = "Span")]
pub struct Span {
    /// Timestamp when the span was ended.
    #[required]
    pub timestamp: Annotated<DateTime<Utc>>,

    /// Timestamp when the span started.
    #[required]
    pub start_timestamp: Annotated<DateTime<Utc>>,

    /// Human readable description of a span (e.g. method URL).
    #[max_chars = "summary"]
    pub description: Annotated<String>,

    /// Span type (see `OperationType` docs).
    #[max_chars = "enumlike"]
    pub op: Annotated<OperationType>,

    /// The Span id.
    #[required]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// The ID of the trace the span belongs to.
    #[required]
    pub trace_id: Annotated<TraceId>,

    // TODO remove retain when the api stabilizes
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_span_serialization() {
        let json = r#"{
  "timestamp": 0.0,
  "start_timestamp": -63158400.0,
  "description": "desc",
  "op": "operation",
  "span_id": "fa90fdead5f74052",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
}"#;

        let span = Annotated::new(Span {
            timestamp: Annotated::new(Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(1968, 1, 1).and_hms_nano(0, 0, 0, 0)),
            description: Annotated::new("desc".to_owned()),
            op: Annotated::new("operation".to_owned()),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
            ..Default::default()
        });
        assert_eq_str!(json, span.to_json_pretty().unwrap());

        let span_from_string = Annotated::from_json(json).unwrap();
        assert_eq_dbg!(span, span_from_string);
    }
}
