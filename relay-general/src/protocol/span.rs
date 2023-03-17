use crate::protocol::{
    DataElement, JsonLenientString, OperationType, SpanId, SpanStatus, Timestamp, TraceId,
};
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

    /// The amount of time in milliseconds spent in this span,
    /// excluding its immediate child spans.
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
    pub data: Annotated<DataElement>,

    // TODO remove retain when the api stabilizes
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use similar_asserts::assert_eq;

    use super::*;
    use crate::protocol::HttpElement;

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
  "status": "ok",
  "data": {
    "http": {
      "query": "is_sample_query=true",
      "not_query": "this is not the query"
    },
    "more_fields": {
      "how_many": "yes",
      "many": "a lot!"
    }
  }
}"#;

        let span = Annotated::new(Span {
            timestamp: Annotated::new(Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1968, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            exclusive_time: Annotated::new(1.23),
            description: Annotated::new("desc".to_owned()),
            op: Annotated::new("operation".to_owned()),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
            status: Annotated::new(SpanStatus::Ok),
            data: Annotated::new(DataElement {
                http: Annotated::new(HttpElement {
                    query: Annotated::new(Value::String("is_sample_query=true".to_owned())),
                    other: {
                        let mut map = BTreeMap::new();
                        map.insert(
                            "not_query".to_owned(),
                            Annotated::new(Value::String("this is not the query".to_owned())),
                        );
                        map
                    },
                }),
                other: {
                    let mut inner_map = BTreeMap::new();
                    inner_map.insert(
                        "how_many".to_owned(),
                        Annotated::new(Value::String("yes".to_owned())),
                    );
                    inner_map.insert(
                        "many".to_owned(),
                        Annotated::new(Value::String("a lot!".to_owned())),
                    );

                    let mut outter_map = BTreeMap::new();
                    outter_map.insert(
                        "more_fields".to_owned(),
                        Annotated::new(Value::Object(inner_map)),
                    );

                    outter_map
                },
            }),
            ..Default::default()
        });
        assert_eq!(json, span.to_json_pretty().unwrap());

        let span_from_string = Annotated::from_json(json).unwrap();
        assert_eq!(span, span_from_string);
    }
}
