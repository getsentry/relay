use relay_protocol::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};
use std::fmt::{self, Display};
use std::str::FromStr;

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::{SpanId, Timestamp, TraceId};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_ourlog", value_type = "OurLog")]
pub struct OurLog {
    /// Timestamp when the log was created.
    #[metastructure(required = true)]
    pub timestamp: Annotated<Timestamp>,

    /// The ID of the trace the log belongs to.
    #[metastructure(required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The Span id.
    #[metastructure(required = false, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// The log level.
    #[metastructure(required = true)]
    pub level: Annotated<OurLogLevel>,

    /// Log body.
    #[metastructure(required = true, pii = "true", trim = false)]
    pub body: Annotated<String>,

    /// Arbitrary attributes on a log.
    #[metastructure(pii = "true", trim = false)]
    pub attributes: Annotated<Object<AttributeValue>>,

    /// The severity number of the log.
    #[metastructure(required = false, trim = false)]
    #[deprecated]
    pub severity_number: Annotated<i64>,

    /// The severity text of the log.
    #[metastructure(required = false, trim = false)]
    #[deprecated]
    pub severity_text: Annotated<String>,

    /// The timestamp of the log.
    #[metastructure(required = false, trim = false)]
    #[deprecated]
    pub timestamp_nanos: Annotated<u64>,

    /// The observed timestamp of the log.
    #[metastructure(required = false, trim = false)]
    #[deprecated]
    pub observed_timestamp_nanos: Annotated<u64>,

    /// The trace flags of the log.
    #[metastructure(required = false, trim = false)]
    #[deprecated]
    pub trace_flags: Annotated<i64>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe", trim = false)]
    pub other: Object<Value>,
}

impl OurLog {
    pub fn attribute(&self, key: &str) -> Option<Value> {
        Some(match self.attributes.value()?.get(key) {
            Some(value) => match value.value() {
                Some(v) => match v {
                    AttributeValue::StringValue(s) => Value::String(s.clone()),
                    AttributeValue::IntValue(i) => Value::I64(*i),
                    AttributeValue::DoubleValue(f) => Value::F64(*f),
                    AttributeValue::BoolValue(b) => Value::Bool(*b),
                    _ => return None,
                },
                None => return None,
            },
            None => return None,
        })
    }
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
            Annotated(Some(Value::Object(mut object)), meta) => {
                if let Some(Annotated(Some(Value::String(string_value)), _)) =
                    object.remove("string_value")
                {
                    return Annotated(Some(AttributeValue::StringValue(string_value)), meta);
                }
                if let Some(Annotated(Some(Value::I64(int_value)), _)) = object.remove("int_value")
                {
                    return Annotated(Some(AttributeValue::IntValue(int_value)), meta);
                }
                if let Some(Annotated(Some(Value::F64(double_value)), _)) =
                    object.remove("double_value")
                {
                    return Annotated(Some(AttributeValue::DoubleValue(double_value)), meta);
                }
                if let Some(Annotated(Some(Value::Bool(bool_value)), _)) =
                    object.remove("bool_value")
                {
                    return Annotated(Some(AttributeValue::BoolValue(bool_value)), meta);
                }

                let mut meta = meta;
                meta.add_error(Error::expected(
                    "a valid attribute value (string, int, double, bool)",
                ));
                meta.set_original_value(Some(Value::Object(object)));
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("an object"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OurLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
    /// Unknown status, for forward compatibility.
    Unknown(String),
}

impl OurLogLevel {
    fn as_str(&self) -> &str {
        match self {
            OurLogLevel::Trace => "trace",
            OurLogLevel::Debug => "debug",
            OurLogLevel::Info => "info",
            OurLogLevel::Warn => "warn",
            OurLogLevel::Error => "error",
            OurLogLevel::Fatal => "fatal",
            OurLogLevel::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for OurLogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
impl FromStr for OurLogLevel {
    type Err = ParseOurLogLevelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "trace" => OurLogLevel::Trace,
            "debug" => OurLogLevel::Debug,
            "info" => OurLogLevel::Info,
            "warn" => OurLogLevel::Warn,
            "error" => OurLogLevel::Error,
            "fatal" => OurLogLevel::Fatal,
            other => OurLogLevel::Unknown(other.to_owned()),
        })
    }
}

impl FromValue for OurLogLevel {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                match OurLogLevel::from_str(&value) {
                    Ok(value) => Annotated(Some(value), meta),
                    Err(err) => {
                        meta.add_error(Error::invalid(err));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a level"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for OurLogLevel {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

impl ProcessValue for OurLogLevel {}

impl Empty for OurLogLevel {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// An error used when parsing `OurLogLevel`.
#[derive(Debug)]
pub struct ParseOurLogLevelError;

impl fmt::Display for ParseOurLogLevelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid our log level")
    }
}

impl std::error::Error for ParseOurLogLevelError {}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    #[test]
    fn test_ourlog_serialization() {
        let json = r#"{
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Example log record",
            "attributes": {
                "boolean.attribute": {
                    "bool_value": true
                },
                "sentry.severity_text": {
                    "string_value": "info"
                },
                "sentry.severity_number": {
                    "int_value": 10
                },
                "sentry.observed_timestamp_nanos": {
                    "int_value": 1544712660300000000
                },
                "sentry.trace_flags": {
                    "int_value": 10
                }
            }
        }"#;

        let data = Annotated::<OurLog>::from_json(json).unwrap();
        insta::assert_debug_snapshot!(data, @r###"
        OurLog {
            timestamp: Timestamp(
                2018-12-13T16:51:00Z,
            ),
            trace_id: TraceId(
                "5b8efff798038103d269b633813fc60c",
            ),
            span_id: SpanId(
                "eee19b7ec3c1b174",
            ),
            level: Info,
            body: "Example log record",
            attributes: {
                "boolean.attribute": BoolValue(
                    true,
                ),
                "sentry.observed_timestamp_nanos": IntValue(
                    1544712660300000000,
                ),
                "sentry.severity_number": IntValue(
                    10,
                ),
                "sentry.severity_text": StringValue(
                    "info",
                ),
                "sentry.trace_flags": IntValue(
                    10,
                ),
            },
            severity_number: ~,
            severity_text: ~,
            timestamp_nanos: ~,
            observed_timestamp_nanos: ~,
            trace_flags: ~,
            other: {},
        }
        "###);

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Example log record",
          "attributes": {
            "boolean.attribute": {
              "bool_value": true
            },
            "sentry.observed_timestamp_nanos": {
              "int_value": 1544712660300000000
            },
            "sentry.severity_number": {
              "int_value": 10
            },
            "sentry.severity_text": {
              "string_value": "info"
            },
            "sentry.trace_flags": {
              "int_value": 10
            }
          }
        }
        "###);
    }
}
