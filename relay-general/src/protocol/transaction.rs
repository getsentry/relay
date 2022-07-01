use std::fmt;
use std::str::FromStr;

use crate::processor::ProcessValue;
use crate::types::{Annotated, Empty, ErrorKind, FromValue, IntoValue, SkipSerialization, Value};

/// Describes how the name of the transaction was determined.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[schemars(rename_all = "kebab-case")]
pub enum TransactionSource {
    /// User-defined name set through `set_transaction_name`.
    Custom,
    /// Raw URL, potentially containing identifiers.
    Url,
    /// The SDK is configured to send a [`route`](Self::Route), but has to fall back to a raw URL
    /// for this particular transaction.
    UrlFallback,
    /// Parametrized URL or route.
    Route,
    /// Name of the view handling the request.
    View,
    /// Named after a software component, such as a function or class name.
    Component,
    /// Name of a background task (e.g. a Celery task).
    Task,
    /// This is the default value set by Relay for legacy SDKs.
    Unknown,
    /// Any other unknown source that is not explicitly defined above.
    #[schemars(skip)]
    Other(String),
}

impl FromStr for TransactionSource {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "custom" => Ok(Self::Custom),
            "url" => Ok(Self::Url),
            "url-fallback" => Ok(Self::UrlFallback),
            "route" => Ok(Self::Route),
            "view" => Ok(Self::View),
            "component" => Ok(Self::Component),
            "task" => Ok(Self::Task),
            "unknown" => Ok(Self::Unknown),
            s => Ok(Self::Other(s.to_owned())),
        }
    }
}

impl fmt::Display for TransactionSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Custom => write!(f, "custom"),
            Self::Url => write!(f, "url"),
            Self::UrlFallback => write!(f, "url-fallback"),
            Self::Route => write!(f, "route"),
            Self::View => write!(f, "view"),
            Self::Component => write!(f, "component"),
            Self::Task => write!(f, "task"),
            Self::Unknown => write!(f, "unknown"),
            Self::Other(s) => write!(f, "{}", s),
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_fallback_roundtrip() {
        let json = r#""url-fallback""#;
        let source = Annotated::new(TransactionSource::UrlFallback);

        assert_eq_dbg!(source, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, source.payload_to_json_pretty().unwrap());
    }

    #[test]
    fn test_other_source_roundtrip() {
        let json = r#""something-new""#;
        let source = Annotated::new(TransactionSource::Other("something-new".to_owned()));

        assert_eq_dbg!(source, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, source.payload_to_json_pretty().unwrap());
    }

    #[test]
    fn test_transaction_info_roundtrip() {
        let json = r#"{
  "source": "url",
  "original": "/auth/login/john123/"
}"#;

        let info = Annotated::new(TransactionInfo {
            source: Annotated::new(TransactionSource::Url),
            original: Annotated::new("/auth/login/john123/".to_owned()),
        });

        assert_eq_dbg!(info, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, info.to_json_pretty().unwrap());
    }
}
