use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, SkipSerialization, Value};
use std::fmt::{self, Display};

use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::{Attribute, SpanId, Timestamp, TraceId};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_ourlog", value_type = "OurLog")]
pub struct OurLog {
    /// Timestamp when the log was created.
    #[metastructure(required = true)]
    pub timestamp: Annotated<Timestamp>,

    /// The ID of the trace the log belongs to.
    #[metastructure(required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The Span this log entry belongs to.
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
    pub attributes: Annotated<Object<Attribute>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl OurLog {
    pub fn attribute(&self, key: &str) -> Option<&Annotated<Value>> {
        Some(&self.attributes.value()?.get(key)?.value()?.value.value)
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

impl From<String> for OurLogLevel {
    fn from(value: String) -> Self {
        match value.as_str() {
            "trace" => OurLogLevel::Trace,
            "debug" => OurLogLevel::Debug,
            "info" => OurLogLevel::Info,
            "warn" => OurLogLevel::Warn,
            "error" => OurLogLevel::Error,
            "fatal" => OurLogLevel::Fatal,
            _ => OurLogLevel::Unknown(value),
        }
    }
}

impl FromValue for OurLogLevel {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(value.into()), meta),
            Annotated(None, meta) => Annotated(None, meta),
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
                "double.attribute": {
                    "value": 1.23,
                    "type": "double"
                },
                "string.attribute": {
                    "value": "some string",
                    "type": "string"
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
            trace_id: TraceId("5b8efff798038103d269b633813fc60c"),
            span_id: SpanId("eee19b7ec3c1b174"),
            level: Info,
            body: "Example log record",
            attributes: {
                "boolean.attribute": Attribute {
                    value: Bool(
                        true,
                    ),
                    type: Boolean,
                    other: {},
                },
                "double.attribute": Attribute {
                    value: F64(
                        1.23,
                    ),
                    type: Double,
                    other: {},
                },
                "sentry.observed_timestamp_nanos": Attribute {
                    value: String(
                        "1544712660300000000",
                    ),
                    type: Integer,
                    other: {},
                },
                "sentry.severity_number": Attribute {
                    value: String(
                        "10",
                    ),
                    type: Integer,
                    other: {},
                },
                "sentry.severity_text": Attribute {
                    value: String(
                        "info",
                    ),
                    type: String,
                    other: {},
                },
                "sentry.trace_flags": Attribute {
                    value: String(
                        "10",
                    ),
                    type: Integer,
                    other: {},
                },
                "string.attribute": Attribute {
                    value: String(
                        "some string",
                    ),
                    type: String,
                    other: {},
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
            "double.attribute": {
              "type": "double",
              "value": 1.23
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
            },
            "string.attribute": {
              "type": "string",
              "value": "some string"
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
