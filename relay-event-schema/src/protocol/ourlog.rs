use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::{SpanId, TraceId};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_ourlog", value_type = "OurLog")]
pub struct OurLog {
    /// Time when the event occurred.
    #[metastructure(required = true, trim = false)]
    pub timestamp_nanos: Annotated<u64>,

    /// Time when the event was observed.
    #[metastructure(required = true, trim = false)]
    pub observed_timestamp_nanos: Annotated<u64>,

    /// The ID of the trace the log belongs to.
    #[metastructure(required = false, trim = false)]
    pub trace_id: Annotated<TraceId>,
    /// The Span id.
    ///
    #[metastructure(required = false, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// Trace flag bitfield.
    #[metastructure(required = false)]
    pub trace_flags: Annotated<f64>,

    /// This is the original string representation of the severity as it is known at the source
    #[metastructure(required = false, max_chars = 32, pii = "maybe", trim = false)]
    pub severity_text: Annotated<String>,

    /// Numerical representation of the severity level
    #[metastructure(required = false)]
    pub severity_number: Annotated<i64>,

    /// Log body.
    #[metastructure(required = true, pii = "maybe", trim = false)]
    pub body: Annotated<String>,

    /// Arbitrary attributes on a log.
    #[metastructure(pii = "maybe", trim = false)]
    pub attributes: Annotated<Object<AttributeValue>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe", trim = false)]
    pub other: Object<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct AttributeValue {
    pub string_value: Annotated<Value>,
    pub int_value: Annotated<Value>,
    pub double_value: Annotated<Value>,
    pub bool_value: Annotated<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ourlog_serialization() {
        let json = r#"{
  "timestamp_nanos": 1544712660300000000,
  "observed_timestamp_nanos": 1544712660300000000,
  "severity_number": 10,
  "severity_text": "Information",
  "trace_id": "5b8efff798038103d269b633813fc60c",
  "span_id": "eee19b7ec3c1b174",
  "body": "Example log record",
  "attributes": {
    "string.attribute": {
      "string_value": "some string"
    },
    "boolean.attribute": {
      "bool_value": true
    },
    "int.attribute": {
      "int_value": 10
    },
    "double.attribute": {
      "double_value": 637.704
    }
  }
}"#;

        let mut attributes = Object::new();
        attributes.insert(
            "string.attribute".into(),
            Annotated::new(AttributeValue {
                string_value: Annotated::new(Value::String("some string".into())),
                ..Default::default()
            }),
        );
        attributes.insert(
            "boolean.attribute".into(),
            Annotated::new(AttributeValue {
                bool_value: Annotated::new(Value::Bool(true)),
                ..Default::default()
            }),
        );
        attributes.insert(
            "int.attribute".into(),
            Annotated::new(AttributeValue {
                int_value: Annotated::new(Value::I64(10)),
                ..Default::default()
            }),
        );
        attributes.insert(
            "double.attribute".into(),
            Annotated::new(AttributeValue {
                double_value: Annotated::new(Value::F64(637.704)),
                ..Default::default()
            }),
        );

        let log = Annotated::new(OurLog {
            timestamp_nanos: Annotated::new(1544712660300000000),
            observed_timestamp_nanos: Annotated::new(1544712660300000000),
            severity_number: Annotated::new(10),
            severity_text: Annotated::new("Information".to_string()),
            trace_id: Annotated::new(TraceId("5b8efff798038103d269b633813fc60c".into())),
            span_id: Annotated::new(SpanId("eee19b7ec3c1b174".into())),
            body: Annotated::new("Example log record".to_string()),
            attributes: Annotated::new(attributes),
            ..Default::default()
        });

        let expected: serde_json::Value = serde_json::from_str(json).unwrap();
        let actual: serde_json::Value =
            serde_json::from_str(&log.to_json_pretty().unwrap()).unwrap();
        assert_eq!(expected, actual);

        let log_from_string = Annotated::<OurLog>::from_json(json).unwrap();
        assert_eq!(log, log_from_string);
    }
}
