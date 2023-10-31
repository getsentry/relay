//! Contains definitions for the Network Error Logging (NEL) interface.
//!
//! See: [`crate::protocol::contexts::NelContext`].

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::processor::ProcessValue;
use crate::protocol::IpAddr;

/// Describes which phase the error occurred in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum NetworkReportPhases {
    /// The error occurred during DNS resolution.
    DNS,
    /// The error occurred during secure connection establishment.
    Connections,
    /// The error occurred during the transmission of request and response .
    Application,
    /// For forward-compatibility.
    Other(String),
}

impl NetworkReportPhases {
    /// Creates the string representation of the current enum value.
    pub fn as_str(&self) -> &str {
        match *self {
            NetworkReportPhases::DNS => "dns",
            NetworkReportPhases::Connections => "connection",
            NetworkReportPhases::Application => "application",
            NetworkReportPhases::Other(ref unknown) => unknown,
        }
    }
}

impl fmt::Display for NetworkReportPhases {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for NetworkReportPhases {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Empty for NetworkReportPhases {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// Error parsing a [`NetworkReportPhases`].
#[derive(Clone, Copy, Debug)]
pub struct ParseNetworkReportPhaseError;

impl fmt::Display for ParseNetworkReportPhaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid network report phase")
    }
}

impl FromStr for NetworkReportPhases {
    type Err = ParseNetworkReportPhaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "dns" => NetworkReportPhases::DNS,
            "connection" => NetworkReportPhases::Connections,
            "application" => NetworkReportPhases::Application,
            unknown => NetworkReportPhases::Other(unknown.to_string()),
        })
    }
}

impl FromValue for NetworkReportPhases {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(phase) => Annotated(Some(phase), meta),
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

impl IntoValue for NetworkReportPhases {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
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
        Serialize::serialize(&self.to_string(), s)
    }
}

/// The NEL parsing errors.
#[derive(Debug, Error)]
pub enum NetworkReportError {
    /// Incoming Json is unparsable.
    #[error("incoming json is unparsable")]
    InvalidJson(#[from] serde_json::Error),
}

/// Generated network error report (NEL).
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct BodyRaw {
    /// The time between the start of the resource fetch and when it was completed or aborted.
    pub elapsed_time: Annotated<u64>,
    /// HTTP method.
    pub method: Annotated<String>,
    /// If request failed, the phase of its network error. If request succeeded, "application".
    pub phase: Annotated<NetworkReportPhases>,
    /// The HTTP protocol and version.
    pub protocol: Annotated<String>,
    /// Request's referrer, as determined by the referrer policy associated with its client.
    pub referrer: Annotated<String>,
    /// The sampling rate.
    pub sampling_fraction: Annotated<f64>,
    /// The IP address of the server where the site is hosted.
    pub server_ip: Annotated<IpAddr>,
    /// HTTP status code.
    pub status_code: Annotated<i64>,
    /// If request failed, the type of its network error. If request succeeded, "ok".
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

/// Models the content of a NEL report.
///
/// See <https://w3c.github.io/network-error-logging/>
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NetworkReportRaw {
    /// The age of the report since it got collected and before it got sent.
    pub age: Annotated<i64>,
    /// The type of the report.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,
    /// The URL of the document in which the error occurred.
    #[metastructure(pii = "true")]
    pub url: Annotated<String>,
    /// The User-Agent HTTP header.
    pub user_agent: Annotated<String>,
    /// The body of the NEL report.
    pub body: Annotated<BodyRaw>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use relay_protocol::{assert_annotated_snapshot, Annotated};

    use crate::protocol::NetworkReportRaw;

    #[test]
    fn test_nel_raw_basic() {
        let json = r#"{
            "age": 31042,
            "body": {
                "elapsed_time": 0,
                "method": "GET",
                "phase": "connection",
                "protocol": "http/1.1",
                "referrer": "",
                "sampling_fraction": 1.0,
                "server_ip": "127.0.0.1",
                "status_code": 0,
                "type": "tcp.refused"
            },
            "type": "network-error",
            "url": "http://example.com/",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }"#;

        let report: Annotated<NetworkReportRaw> =
            Annotated::from_json_bytes(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(report, @r###"
        {
          "age": 31042,
          "type": "network-error",
          "url": "http://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "body": {
            "elapsed_time": 0,
            "method": "GET",
            "phase": "connection",
            "protocol": "http/1.1",
            "referrer": "",
            "sampling_fraction": 1.0,
            "server_ip": "127.0.0.1",
            "status_code": 0,
            "type": "tcp.refused"
          }
        }
        "###);
    }
}
