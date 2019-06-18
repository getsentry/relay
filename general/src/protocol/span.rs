use chrono::{DateTime, Utc};

use crate::protocol::{SpanId, TraceId};
use crate::types::{Annotated, Object, Value};

// TODO typing ( union of predefined types and custom 'string' types)
type OperationType = String;

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct Span {
    /// Timestamp when the span was ended.
    pub timestamp: Annotated<DateTime<Utc>>,

    /// Timestamp when the span started
    pub start_timestamp: Annotated<DateTime<Utc>>,

    /// Human readable description of a span (e.g. method URL)
    pub description: Annotated<String>,

    /// span type
    pub op: Annotated<OperationType>,

    /// the span id
    pub span_id: Annotated<SpanId>,

    /// the trace id
    pub trace_id: Annotated<TraceId>,

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
