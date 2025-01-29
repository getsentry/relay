use relay_protocol::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};

use serde::ser::SerializeMap;

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
    pub trace_flags: Annotated<u64>,

    /// This is the original string representation of the severity as it is known at the source
    #[metastructure(required = false, max_chars = 32, pii = "true", trim = false)]
    pub severity_text: Annotated<String>,

    /// Numerical representation of the severity level
    #[metastructure(required = false)]
    pub severity_number: Annotated<i64>,

    /// Log body.
    #[metastructure(required = true, pii = "true", trim = false)]
    pub body: Annotated<String>,

    /// Arbitrary attributes on a log.
    #[metastructure(pii = "true", trim = false)]
    pub attributes: Annotated<Object<AttributeValue>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe", trim = false)]
    pub other: Object<Value>,
}

#[derive(Debug, Clone, PartialEq, ProcessValue)]
pub enum AttributeValue {
    #[metastructure(field = "string_value", pii = "true")]
    StringValue(String),
    #[metastructure(field = "int_value", pii = "true")]
    IntValue(i64),
    #[metastructure(field = "double_value", pii = "true")]
    DoubleValue(f64),
    #[metastructure(field = "bool_value", pii = "true")]
    BoolValue(bool),
    /// Any other unknown attribute value.
    ///
    /// This exists to ensure other attribute values such as array and object can be added in the future.
    Unknown(String),
}

impl IntoValue for AttributeValue {
    fn into_value(self) -> Value {
        let mut map = Object::new();
        match self {
            AttributeValue::StringValue(v) => {
                map.insert("string_value".to_string(), Annotated::new(Value::String(v)));
            }
            AttributeValue::IntValue(v) => {
                map.insert("int_value".to_string(), Annotated::new(Value::I64(v)));
            }
            AttributeValue::DoubleValue(v) => {
                map.insert("double_value".to_string(), Annotated::new(Value::F64(v)));
            }
            AttributeValue::BoolValue(v) => {
                map.insert("bool_value".to_string(), Annotated::new(Value::Bool(v)));
            }
            AttributeValue::Unknown(v) => {
                map.insert("unknown".to_string(), Annotated::new(Value::String(v)));
            }
        }
        Value::Object(map)
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        let mut map = s.serialize_map(None)?;
        match self {
            AttributeValue::StringValue(v) => {
                map.serialize_entry("string_value", v)?;
            }
            AttributeValue::IntValue(v) => {
                map.serialize_entry("int_value", v)?;
            }
            AttributeValue::DoubleValue(v) => {
                map.serialize_entry("double_value", v)?;
            }
            AttributeValue::BoolValue(v) => {
                map.serialize_entry("bool_value", v)?;
            }
            AttributeValue::Unknown(v) => {
                map.serialize_entry("unknown", v)?;
            }
        }
        map.end()
    }
}

impl AttributeValue {
    pub fn string_value(&self) -> Option<&String> {
        match self {
            AttributeValue::StringValue(s) => Some(s),
            _ => None,
        }
    }
    pub fn int_value(&self) -> Option<i64> {
        match self {
            AttributeValue::IntValue(i) => Some(*i),
            _ => None,
        }
    }
    pub fn double_value(&self) -> Option<f64> {
        match self {
            AttributeValue::DoubleValue(d) => Some(*d),
            _ => None,
        }
    }
    pub fn bool_value(&self) -> Option<bool> {
        match self {
            AttributeValue::BoolValue(b) => Some(*b),
            _ => None,
        }
    }
}

impl Empty for AttributeValue {
    #[inline]
    fn is_empty(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }
}

impl FromValue for AttributeValue {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), meta) => {
                Annotated(Some(AttributeValue::StringValue(value)), meta)
            }
            Annotated(Some(Value::I64(value)), meta) => {
                Annotated(Some(AttributeValue::IntValue(value)), meta)
            }
            Annotated(Some(Value::F64(value)), meta) => {
                Annotated(Some(AttributeValue::DoubleValue(value)), meta)
            }
            Annotated(Some(Value::Bool(value)), meta) => {
                Annotated(Some(AttributeValue::BoolValue(value)), meta)
            }
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected(
                    "a valid attribute value (string, int, double, bool)",
                ));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ourlog_serialization() {
        let json = r#"{
  "timestamp_nanos": 1544712660300000000,
  "observed_timestamp_nanos": 1544712660300000000,
  "trace_id": "5b8efff798038103d269b633813fc60c",
  "span_id": "eee19b7ec3c1b174",
  "severity_text": "Information",
  "severity_number": 10,
  "body": "Example log record",
  "attributes": {
    "boolean.attribute": {
      "bool_value": true
    },
    "double.attribute": {
      "double_value": 637.704
    },
    "int.attribute": {
      "int_value": 10
    },
    "string.attribute": {
      "string_value": "some string"
    }
  }
}"#;

        let mut attributes = Object::new();
        attributes.insert(
            "string.attribute".into(),
            Annotated::new(AttributeValue::StringValue("some string".into())),
        );
        attributes.insert(
            "boolean.attribute".into(),
            Annotated::new(AttributeValue::BoolValue(true)),
        );
        attributes.insert(
            "int.attribute".into(),
            Annotated::new(AttributeValue::IntValue(10)),
        );
        attributes.insert(
            "double.attribute".into(),
            Annotated::new(AttributeValue::DoubleValue(637.704)),
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

        assert_eq!(json, log.to_json_pretty().unwrap());
    }
}
