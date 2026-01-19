//! Contains definitions for the Cross-Origin-Opener-Policy (COOP) interface.
//!
//! See: <https://github.com/camillelamy/explainers/blob/main/coop_reporting.md>

use std::fmt;
use std::str::FromStr;

use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::processor::ProcessValue;

/// The disposition of the COOP policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ProcessValue)]
#[serde(rename_all = "lowercase")]
pub enum CoopDisposition {
    /// The policy is being enforced.
    Enforce,
    /// The policy is in report-only mode.
    Reporting,
    /// For forward-compatibility.
    Other(String),
}

impl CoopDisposition {
    /// Creates the string representation of the current enum value.
    pub fn as_str(&self) -> &str {
        match *self {
            CoopDisposition::Enforce => "enforce",
            CoopDisposition::Reporting => "reporting",
            CoopDisposition::Other(ref unknown) => unknown,
        }
    }
}

impl fmt::Display for CoopDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for CoopDisposition {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Empty for CoopDisposition {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// Error parsing a [`CoopDisposition`].
#[derive(Clone, Copy, Debug)]
pub struct ParseCoopDispositionError;

impl fmt::Display for ParseCoopDispositionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid COOP disposition")
    }
}

impl FromStr for CoopDisposition {
    type Err = ParseCoopDispositionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        Ok(match s.as_str() {
            "enforce" => CoopDisposition::Enforce,
            "reporting" => CoopDisposition::Reporting,
            _ => CoopDisposition::Other(s),
        })
    }
}

impl FromValue for CoopDisposition {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(disposition) => Annotated(Some(disposition), meta),
                Err(_) => {
                    meta.add_error(relay_protocol::Error::expected("a string"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(relay_protocol::Error::expected("a string"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for CoopDisposition {
    fn into_value(self) -> Value {
        Value::String(match self {
            Self::Other(s) => s,
            _ => self.as_str().to_owned(),
        })
    }

    fn serialize_payload<S>(
        &self,
        s: S,
        _behavior: relay_protocol::SkipSerialization,
    ) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

/// The COOP parsing errors.
#[derive(Debug, Error)]
pub enum CoopReportError {
    /// Incoming Json is unparsable.
    #[error("incoming json is unparsable")]
    InvalidJson(#[from] serde_json::Error),
}

/// Body of a COOP report.
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
pub struct CoopBodyRaw {
    /// The disposition of the COOP policy ("enforce" or "reporting").
    pub disposition: Annotated<CoopDisposition>,
    /// The effective COOP policy value.
    #[metastructure(field = "effectivePolicy")]
    pub effective_policy: Annotated<String>,
    /// The URL of the previous response (for navigate-to-document violations).
    #[metastructure(field = "previousResponseURL", pii = "true")]
    pub previous_response_url: Annotated<String>,
    /// The URL of the next response (for navigate-from-document violations).
    #[metastructure(field = "nextResponseURL", pii = "true")]
    pub next_response_url: Annotated<String>,
    /// The URL of the opener window.
    #[metastructure(field = "openerURL", pii = "true")]
    pub opener_url: Annotated<String>,
    /// The URL of the window that was opened.
    #[metastructure(field = "openeeURL", pii = "true")]
    pub openee_url: Annotated<String>,
    /// The URL of the other document involved in the violation.
    #[metastructure(field = "otherDocumentURL", pii = "true")]
    pub other_document_url: Annotated<String>,
    /// The referrer of the document.
    pub referrer: Annotated<String>,
    /// The type of COOP violation.
    pub violation: Annotated<String>,
    /// The property that was accessed.
    pub property: Annotated<String>,
    /// The initial popup URL (for openee violations).
    #[metastructure(field = "initialPopupURL", pii = "true")]
    pub initial_popup_url: Annotated<String>,
    /// The source file where the violation occurred.
    #[metastructure(field = "sourceFile", pii = "true")]
    pub source_file: Annotated<String>,
    /// The line number where the violation occurred.
    #[metastructure(field = "lineNumber")]
    pub line_number: Annotated<u64>,
    /// The column number where the violation occurred.
    #[metastructure(field = "columnNumber")]
    pub column_number: Annotated<u64>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

/// Models the content of a COOP report.
///
/// See <https://github.com/camillelamy/explainers/blob/main/coop_reporting.md>
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
pub struct CoopReportRaw {
    /// The age of the report since it got collected and before it got sent.
    pub age: Annotated<i64>,
    /// The type of the report.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,
    /// The URL of the document in which the violation occurred.
    #[metastructure(pii = "true")]
    pub url: Annotated<String>,
    /// The User-Agent HTTP header.
    pub user_agent: Annotated<String>,
    /// The body of the COOP report.
    pub body: Annotated<CoopBodyRaw>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use relay_protocol::{assert_annotated_snapshot, Annotated};

    use crate::protocol::CoopReportRaw;

    #[test]
    fn test_coop_raw_navigate_to() {
        let json = r#"{
            "age": 0,
            "body": {
                "disposition": "enforce",
                "effectivePolicy": "same-origin",
                "previousResponseURL": "https://example.com/previous",
                "referrer": "https://referrer.com/",
                "violation": "navigate-to-document"
            },
            "type": "coop",
            "url": "https://example.com/",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }"#;

        let report: Annotated<CoopReportRaw> =
            Annotated::from_json_bytes(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(report, @r###"
        {
          "age": 0,
          "type": "coop",
          "url": "https://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "body": {
            "disposition": "enforce",
            "effectivePolicy": "same-origin",
            "previousResponseURL": "https://example.com/previous",
            "referrer": "https://referrer.com/",
            "violation": "navigate-to-document"
          }
        }
        "###);
    }

    #[test]
    fn test_coop_raw_access_from() {
        let json = r#"{
            "age": 123,
            "body": {
                "disposition": "reporting",
                "effectivePolicy": "same-origin-allow-popups",
                "openerURL": "https://opener.com/",
                "referrer": "",
                "violation": "access-from-coop-page-to-opener",
                "property": "postMessage",
                "sourceFile": "https://example.com/script.js",
                "lineNumber": 42,
                "columnNumber": 15
            },
            "type": "coop",
            "url": "https://example.com/page",
            "user_agent": "Mozilla/5.0"
        }"#;

        let report: Annotated<CoopReportRaw> =
            Annotated::from_json_bytes(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(report, @r###"
        {
          "age": 123,
          "type": "coop",
          "url": "https://example.com/page",
          "user_agent": "Mozilla/5.0",
          "body": {
            "disposition": "reporting",
            "effectivePolicy": "same-origin-allow-popups",
            "openerURL": "https://opener.com/",
            "referrer": "",
            "violation": "access-from-coop-page-to-opener",
            "property": "postMessage",
            "sourceFile": "https://example.com/script.js",
            "lineNumber": 42,
            "columnNumber": 15
          }
        }
        "###);
    }
}
