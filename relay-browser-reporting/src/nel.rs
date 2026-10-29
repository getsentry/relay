//! Network Error Logging reports.
//!
//! See <https://www.w3.org/TR/network-error-logging/>.

use std::fmt;
use std::net::IpAddr;

use serde::{Deserialize, Deserializer, Serialize, de};

/// Describes which phase the error occurred in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "phase")]
pub enum NetworkReportPhase {
    /// The error occurred during DNS resolution.
    Dns,
    /// The error occurred during secure connection establishment.
    Connection {
        /// The HTTP protocol and version.
        protocol: String,
        /// The IP address of the server where the site is hosted, or an empty string.
        #[serde(deserialize_with = "deserialize_ip_opt")]
        server_ip: Option<IpAddr>,
    },
    /// The error occurred during the transmission of request and response .
    Application {
        /// HTTP method.
        method: String,
        /// The HTTP protocol and version.
        protocol: String,
        /// Request's referrer, as determined by the referrer policy associated with its client.
        referrer: String,
        /// The IP address of the server where the site is hosted, or an empty string.
        #[serde(deserialize_with = "deserialize_ip_opt")]
        server_ip: Option<IpAddr>,
        /// HTTP status code.
        status_code: u16,
    },
}

impl NetworkReportPhase {
    /// The name of the phase.
    ///
    /// This corresponds to the value of the `"phase"`
    /// field in the serialized report body.
    pub fn name(&self) -> &'static str {
        match self {
            NetworkReportPhase::Dns => "dns",
            NetworkReportPhase::Connection { .. } => "connection",
            NetworkReportPhase::Application { .. } => "application",
        }
    }
}

/// The NEL report's type (either `"ok"` or an error string).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReportType {
    #[serde(rename = "ok")]
    Ok,
    #[serde(untagged)]
    Error(String),
}

impl fmt::Display for ReportType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReportType::Ok => f.write_str("ok"),
            ReportType::Error(error) => f.write_str(error),
        }
    }
}

/// The body of a Network Error report, as defined in
/// <https://www.w3.org/TR/network-error-logging/#generate-a-network-error-report> 6–8.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body {
    /// The time between the start of the resource fetch and when it was completed or aborted.
    pub elapsed_time: u64,
    /// If request failed, the phase of its network error. If request succeeded, "application".
    #[serde(flatten)]
    pub phase: NetworkReportPhase,
    /// The sampling rate.
    pub sampling_fraction: f64,
    /// If request failed, the type of its network error. If request succeeded, "ok".
    #[serde(rename = "type")]
    pub ty: ReportType,
}

/// Deserializes an optional [`IpAddr`], treating `""` as `None`.
fn deserialize_ip_opt<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<IpAddr>, D::Error> {
    struct Visitor;

    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Option<IpAddr>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a valid IP address or an empty string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if v.is_empty() {
                return Ok(None);
            }

            Ok(Some(IpAddr::deserialize(de::value::StrDeserializer::new(
                v,
            ))?))
        }
    }

    deserializer.deserialize_str(Visitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_server_ip() {
        let json = r#"{
            "elapsed_time": 0,
            "method": "GET",
            "phase": "connection",
            "protocol": "http/1.1",
            "referrer": "",
            "sampling_fraction": 1.0,
            "server_ip": "",
            "status_code": 0,
            "type": "tcp.refused"
        }"#;

        let body: Body = serde_json::from_str(json).unwrap();

        let NetworkReportPhase::Connection { server_ip, .. } = body.phase else {
            unreachable!();
        };

        assert!(server_ip.is_none());
    }

    #[test]
    fn parse_missing_server_ip() {
        let json = r#"{
            "elapsed_time": 0,
            "method": "GET",
            "phase": "connection",
            "protocol": "http/1.1",
            "referrer": "",
            "sampling_fraction": 1.0,
            "status_code": 0,
            "type": "tcp.refused"
        }"#;

        // The `server_ip` field can be empty, but it can't be missing.
        assert!(serde_json::from_str::<Body>(json).is_err());
    }
}
