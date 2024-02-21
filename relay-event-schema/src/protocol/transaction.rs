use std::fmt;
use std::str::FromStr;

#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, ErrorKind, FromValue, IntoValue, SkipSerialization, Value};
use serde::{Deserialize, Serialize};

use crate::processor::ProcessValue;
use crate::protocol::Timestamp;

/// Describes how the name of the transaction was determined.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "jsonschema", schemars(rename_all = "kebab-case"))]
pub enum TransactionSource {
    /// User-defined name set through `set_transaction_name`.
    Custom,
    /// Raw URL, potentially containing identifiers.
    Url,
    /// Parametrized URL or route.
    Route,
    /// Name of the view handling the request.
    View,
    /// Named after a software component, such as a function or class name.
    Component,
    /// The transaction name was updated to reduce the name cardinality.
    Sanitized,
    /// Name of a background task (e.g. a Celery task).
    Task,
    /// This is the default value set by Relay for legacy SDKs.
    Unknown,
    /// Any other unknown source that is not explicitly defined above.
    #[cfg_attr(feature = "jsonschema", schemars(skip))]
    Other(String),
}

impl TransactionSource {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Custom => "custom",
            Self::Url => "url",
            Self::Route => "route",
            Self::View => "view",
            Self::Component => "component",
            Self::Sanitized => "sanitized",
            Self::Task => "task",
            Self::Unknown => "unknown",
            Self::Other(ref s) => s,
        }
    }
}

impl FromStr for TransactionSource {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "custom" => Ok(Self::Custom),
            "url" => Ok(Self::Url),
            "route" => Ok(Self::Route),
            "view" => Ok(Self::View),
            "component" => Ok(Self::Component),
            "sanitized" => Ok(Self::Sanitized),
            "task" => Ok(Self::Task),
            "unknown" => Ok(Self::Unknown),
            s => Ok(Self::Other(s.to_owned())),
        }
    }
}

impl fmt::Display for TransactionSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for TransactionSource {
    fn default() -> Self {
        Self::Unknown
    }
}

impl Empty for TransactionSource {
    #[inline]
    fn is_empty(&self) -> bool {
        matches!(self, Self::Unknown)
    }
}

impl FromValue for TransactionSource {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), mut meta) => match value.parse() {
                Ok(source) => Annotated(Some(source), meta),
                Err(_) => {
                    meta.add_error(ErrorKind::InvalidData);
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for TransactionSource {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for TransactionSource {}

#[derive(Clone, Debug, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct TransactionNameChange {
    /// Describes how the previous transaction name was determined.
    pub source: Annotated<TransactionSource>,

    /// The number of propagations from the start of the transaction to this change.
    pub propagations: Annotated<u64>,

    /// Timestamp when the transaction name was changed.
    ///
    /// This adheres to the event timestamp specification.
    pub timestamp: Annotated<Timestamp>,
}

/// Additional information about the name of the transaction.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct TransactionInfo {
    /// Describes how the name of the transaction was determined.
    ///
    /// This will be used by the server to decide whether or not to scrub identifiers from the
    /// transaction name, or replace the entire name with a placeholder.
    pub source: Annotated<TransactionSource>,

    /// The unmodified transaction name as obtained by the source.
    ///
    /// This value will only be set if the transaction name was modified during event processing.
    #[metastructure(max_chars = "culprit", trim_whitespace = "true")]
    pub original: Annotated<String>,

    /// A list of changes prior to the final transaction name.
    ///
    /// This list must be empty if the transaction name is set at the beginning of the transaction
    /// and never changed. There is no placeholder entry for the initial transaction name.
    pub changes: Annotated<Vec<Annotated<TransactionNameChange>>>,

    /// The total number of propagations during the transaction.
    pub propagations: Annotated<u64>,
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_other_source_roundtrip() {
        let json = r#""something-new""#;
        let source = Annotated::new(TransactionSource::Other("something-new".to_owned()));

        assert_eq!(source, Annotated::from_json(json).unwrap());
        assert_eq!(json, source.payload_to_json_pretty().unwrap());
    }

    #[test]
    fn test_transaction_info_roundtrip() {
        let json = r#"{
  "source": "route",
  "original": "/auth/login/john123/",
  "changes": [
    {
      "source": "url",
      "propagations": 1,
      "timestamp": 946684800.0
    }
  ],
  "propagations": 2
}"#;

        let info = Annotated::new(TransactionInfo {
            source: Annotated::new(TransactionSource::Route),
            original: Annotated::new("/auth/login/john123/".to_owned()),
            changes: Annotated::new(vec![Annotated::new(TransactionNameChange {
                source: Annotated::new(TransactionSource::Url),
                propagations: Annotated::new(1),
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
            })]),
            propagations: Annotated::new(2),
        });

        assert_eq!(info, Annotated::from_json(json).unwrap());
        assert_eq!(json, info.to_json_pretty().unwrap());
    }
}
