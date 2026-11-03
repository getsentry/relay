//! Relay support for Browser Reports
//!
//! The browser reporting API is defined in
//! <https://www.w3.org/TR/reporting-1/>.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub mod nel;

use serde::{Deserialize, Deserializer, Serialize, de::IgnoredAny};

/// The contents of the `body` field of a [`BrowserReport`].
///
/// This contains a variant for each supported report type,
/// plus an `Unsupported` variant into which all reports with
/// unknown `"type"` fields get deserialized.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "body")]
#[serde(rename_all = "kebab-case")]
pub enum BrowserReportBody {
    /// The body of a Networ Error report.
    NetworkError(nel::Body),
    /// An unsupported report body.
    #[serde(other, deserialize_with = "deserialize_unknown_report_body")]
    Unsupported,
}

impl BrowserReportBody {
    /// The type of the browser report.
    ///
    /// This corresponds to the value of the `"type"`
    /// field in the serialized report.
    pub fn report_type(&self) -> &'static str {
        match self {
            BrowserReportBody::NetworkError(_) => "network-error",
            BrowserReportBody::Unsupported => "unsupported",
        }
    }
}

/// A serialized Browser Report sent by the
/// Browser Reporting API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserReport {
    /// The time between the generation and sending of the report, in milliseconds.
    ///
    /// See <https://www.w3.org/TR/reporting-1/#serialize-reports>.
    pub age: u64,
    /// The URL of the document in which the error occurred.
    pub url: String,
    /// The User-Agent HTTP header.
    pub user_agent: String,
    /// The body of the report.
    #[serde(flatten)]
    pub body: BrowserReportBody,
}

/// See <https://github.com/serde-rs/serde/issues/1714>.
fn deserialize_unknown_report_body<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(IgnoredAny)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;

    use super::*;

    #[test]
    fn parse_nel() {
        let json = r#"{
            "age": 31042,
            "body": {
                "elapsed_time": 0,
                "method": "GET",
                "phase": "application",
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

        let report: BrowserReport = serde_json::from_str(json).unwrap();

        assert_json_snapshot!(report, @r#"
        {
          "age": 31042,
          "url": "http://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "type": "network-error",
          "body": {
            "elapsed_time": 0,
            "phase": "application",
            "method": "GET",
            "protocol": "http/1.1",
            "referrer": "",
            "server_ip": "127.0.0.1",
            "status_code": 0,
            "sampling_fraction": 1.0,
            "type": "tcp.refused"
          }
        }
        "#);
    }

    #[test]
    fn parse_missing_report_type() {
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
                "url": "http://example.com/",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
            }"#;

        assert!(serde_json::from_str::<BrowserReport>(json).is_err());
    }

    #[test]
    fn parse_unknown_report_type() {
        let json = r#"{
                "age": 31042,
                "body": {
                    "frobnicated": true,
                    "mana_used": 477
                },
                "type": "future-magic",
                "url": "http://example.com/",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
            }"#;

        let report: BrowserReport = serde_json::from_str(json).unwrap();

        assert_json_snapshot!(report, @r#"
        {
          "age": 31042,
          "url": "http://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "type": "unsupported"
        }
        "#);
    }
}
