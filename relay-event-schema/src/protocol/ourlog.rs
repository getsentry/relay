use relay_protocol::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};
use std::fmt::{self, Display};
use std::str::FromStr;

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
    pub attributes: Annotated<Object<OurLogAttribute>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe", trim = false)]
    pub other: Object<Value>,
}

impl OurLog {
    pub fn attribute(&self, key: &str) -> Option<Annotated<Value>> {
        Some(
            self.attributes
                .value()?
                .get(key)?
                .value()?
                .value
                .clone()
                .value,
        )
    }
}

#[derive(Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct OurLogAttribute {
    #[metastructure(flatten)]
    pub value: OurLogAttributeValue,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl OurLogAttribute {
    pub fn new(attribute_type: OurLogAttributeType, value: Value) -> Self {
        Self {
            value: OurLogAttributeValue::new(Annotated::new(attribute_type), Annotated::new(value)),
            other: Object::new(),
        }
    }
}

impl fmt::Debug for OurLogAttribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OurLogAttribute")
            .field("value", &self.value.value)
            .field("type", &self.value.ty)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct OurLogAttributeValue {
    #[metastructure(field = "type", required = true, trim = false)]
    pub ty: Annotated<String>,
    #[metastructure(required = true, pii = "true")]
    pub value: Annotated<Value>,
}

impl OurLogAttributeValue {
    pub fn new(attribute_type: Annotated<OurLogAttributeType>, value: Annotated<Value>) -> Self {
        Self {
            ty: attribute_type.map_value(|at| at.as_str().to_string()),
            value,
        }
    }
}

#[derive(Debug)]
pub enum OurLogAttributeType {
    Bool,
    Int,
    Double,
    String,
    Unknown(String),
}

impl OurLogAttributeType {
    fn as_str(&self) -> &str {
        match self {
            Self::Bool => "boolean",
            Self::Int => "integer",
            Self::Double => "double",
            Self::String => "string",
            Self::Unknown(value) => value,
        }
    }
}

impl fmt::Display for OurLogAttributeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for OurLogAttributeType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "boolean" => Self::Bool,
            "integer" => Self::Int,
            "double" => Self::Double,
            "string" => Self::String,
            _ => Self::Unknown(value),
        }
    }
}

impl Empty for OurLogAttributeType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for OurLogAttributeType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(value.into()), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for OurLogAttributeType {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(match self {
            Self::Unknown(s) => s,
            s => s.to_string(),
        })
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::ser::Serialize::serialize(self.as_str(), s)
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
                    "value": true,
                    "type": "boolean"
                },
                "sentry.severity_text": {
                    "value": "info",
                    "type": "string"
                },
                "sentry.severity_number": {
                    "value": "10",
                    "type": "integer"
                },
                "sentry.observed_timestamp_nanos": {
                    "value": "1544712660300000000",
                    "type": "integer"
                },
                "sentry.trace_flags": {
                    "value": "10",
                    "type": "integer"
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
                "boolean.attribute": OurLogAttribute {
                    value: Bool(
                        true,
                    ),
                    type: "boolean",
                },
                "sentry.observed_timestamp_nanos": OurLogAttribute {
                    value: String(
                        "1544712660300000000",
                    ),
                    type: "integer",
                },
                "sentry.severity_number": OurLogAttribute {
                    value: String(
                        "10",
                    ),
                    type: "integer",
                },
                "sentry.severity_text": OurLogAttribute {
                    value: String(
                        "info",
                    ),
                    type: "string",
                },
                "sentry.trace_flags": OurLogAttribute {
                    value: String(
                        "10",
                    ),
                    type: "integer",
                },
            },
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
              "type": "boolean",
              "value": true
            },
            "sentry.observed_timestamp_nanos": {
              "type": "integer",
              "value": "1544712660300000000"
            },
            "sentry.severity_number": {
              "type": "integer",
              "value": "10"
            },
            "sentry.severity_text": {
              "type": "string",
              "value": "info"
            },
            "sentry.trace_flags": {
              "type": "integer",
              "value": "10"
            }
          }
        }
        "###);
    }

    #[test]
    fn test_invalid_int_attribute() {
        let json = r#"{
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Example log record",
            "attributes": {
                "sentry.severity_number": {
                    "value": 10,
                    "type": "integer"
                }
            }
        }"#;

        let data = Annotated::<OurLog>::from_json(json).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Example log record",
          "attributes": {
            "sentry.severity_number": {
              "type": "integer",
              "value": 10
            }
          }
        }
        "###);
    }
}
