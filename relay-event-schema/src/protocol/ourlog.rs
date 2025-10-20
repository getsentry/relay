use relay_protocol::{
    Annotated, Empty, FromValue, Getter, IntoValue, Object, SkipSerialization, Value,
};
use std::collections::BTreeMap;
use std::fmt::{self, Display};

use serde::{Deserialize, Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::{Attributes, SpanId, Timestamp, TraceId};

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
    #[metastructure(required = true, pii = "maybe", trim = false)]
    pub body: Annotated<String>,

    /// Arbitrary attributes on a log.
    #[metastructure(pii = "maybe", trim = false)]
    pub attributes: Annotated<Attributes>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl Getter for OurLog {
    fn get_value(&self, path: &str) -> Option<relay_protocol::Val<'_>> {
        Some(match path.strip_prefix("log.")? {
            "body" => self.body.as_str()?.into(),
            path => {
                if let Some(key) = path.strip_prefix("attributes.") {
                    let key = key.strip_suffix(".value")?;
                    self.attributes.value()?.get_value(key)?.into()
                } else {
                    return None;
                }
            }
        })
    }
}

/// Relay specific metadata embedded into the log item.
///
/// This metadata is purely an internal protocol extension used by Relay,
/// no one except Relay should be sending this data, nor should anyone except Relay rely on it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OurLogHeader {
    /// Original (calculated) size of the log item when it was first received by a Relay.
    ///
    /// If this value exists, Relay uses it as quantity for all outcomes emitted to the
    /// log byte data category.
    pub byte_size: Option<u64>,

    /// Forward compatibility for additional headers.
    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
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
