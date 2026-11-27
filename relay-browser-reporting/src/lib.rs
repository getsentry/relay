//! Relay support for Browser Reports
//!
//! The browser reporting API is defined in
//! <https://www.w3.org/TR/reporting-1/>.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub mod coop;
pub mod csp;
pub mod integrity;
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
    /// The body of a Network Error report.
    NetworkError(nel::Body),
    /// The body of an Integrity Violation report.
    IntegrityViolation(integrity::Body),
    /// The body of a CSP Violation report.
    CspViolation(csp::Body),
    /// The body of a Cross-Origin-Opener-Policy violation report.
    Coop(coop::Body),
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
            BrowserReportBody::IntegrityViolation(_) => "integrity-violation",
            BrowserReportBody::CspViolation(_) => "csp-violation",
            BrowserReportBody::Coop(_) => "coop",
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
    #[serde(default)]
    pub age: u64,
    /// The URL of the document in which the error occurred.
    pub url: String,
    /// The User-Agent HTTP header.
    pub user_agent: Option<String>,
    /// The body of the report.
    #[serde(flatten)]
    pub body: BrowserReportBody,
}

/// Source file information in a report body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFile {
    /// The URL of the resource where the violation occurred.
    pub source_file: String,
    /// The column number in source-file on which the violation occurred.
    ///
    /// According to the spec, this should always be set if `source_file` is,
    /// but it doesn't cost us anything to allow it to be optional.
    pub column_number: Option<u64>,
    /// The line number in source-file on which the violation occurred.
    ///
    /// According to the spec, this should always be set if `source_file` is,
    /// but it doesn't cost us anything to allow it to be optional.
    pub line_number: Option<u64>,
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
    fn parse_integrity() {
        let json = r#"{
            "age": 31042,
            "body": {
                "documentURL": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                "blockedURL": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                "reportOnly": false,
                "destination": "image"
            },
            "type": "integrity-violation",
            "url": "http://example.com/",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }"#;

        let report: BrowserReport = serde_json::from_str(json).unwrap();

        assert_json_snapshot!(report, @r#"
        {
          "age": 31042,
          "url": "http://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "type": "integrity-violation",
          "body": {
            "documentURL": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
            "blockedURL": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
            "destination": "image",
            "reportOnly": false
          }
        }
        "#);
    }

    #[test]
    fn parse_csp() {
        let json = r#"{
          "age":0,
          "type":"csp-violation",
          "url":"https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
          "user_agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
          "body": {
            "blockedURL":"https://example.com/tst/media/7_del.png",
            "disposition":"enforce",
            "documentURL":"https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
            "effectiveDirective":"img-src",
            "lineNumber":9,
            "originalPolicy":"default-src 'none'; report-to endpoint-csp;",
            "referrer":"https://example.com/test229/",
            "sourceFile":"https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
            "statusCode":0
            }
        }"#;

        let report: BrowserReport = serde_json::from_str(json).unwrap();

        assert_json_snapshot!(report, @r#"
        {
          "age": 0,
          "url": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
          "user_agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
          "type": "csp-violation",
          "body": {
            "documentURL": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
            "referrer": "https://example.com/test229/",
            "blockedURL": "https://example.com/tst/media/7_del.png",
            "effectiveDirective": "img-src",
            "originalPolicy": "default-src 'none'; report-to endpoint-csp;",
            "sourceFile": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
            "columnNumber": null,
            "lineNumber": 9,
            "sample": null,
            "disposition": "enforce",
            "statusCode": 0
          }
        }
        "#);
    }

    #[test]
    fn parse_csp_from_safari() {
        let json = r#"{
            "type": "csp-violation",
            "url": "https://example.com/",
            "body": {
                "documentURL": "https://example.com/",
                "disposition": "report",
                "referrer": "",
                "effectiveDirective": "script-src-elem",
                "blockedURL": "inline",
                "originalPolicy": "script-src 'none'; report-to csp-endpoint",
                "statusCode": 200,
                "sample": "",
                "sourceFile": "https://example.com/",
                "lineNumber": 1,
                "columnNumber": 1
            }
        }"#;

        let report: BrowserReport = serde_json::from_str(json).unwrap();

        assert_json_snapshot!(report, @r#"
        {
          "age": 0,
          "url": "https://example.com/",
          "user_agent": null,
          "type": "csp-violation",
          "body": {
            "documentURL": "https://example.com/",
            "referrer": "",
            "blockedURL": "inline",
            "effectiveDirective": "script-src-elem",
            "originalPolicy": "script-src 'none'; report-to csp-endpoint",
            "sourceFile": "https://example.com/",
            "columnNumber": 1,
            "lineNumber": 1,
            "sample": "",
            "disposition": "report",
            "statusCode": 200
          }
        }
        "#);
    }

    #[test]
    fn parse_coop() {
        let json = r#"{
            "age": 0,
            "body": {
                "disposition": "reporting",
                "effectivePolicy": "same-origin",
                "initialPopupURL": "https://example.org/popup",
                "openeeURL": "",
                "property": "postMessage",
                "type": "access-from-coop-page-to-openee"
            },
            "type": "coop",
            "url": "https://example.com/",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36"
        }"#;

        let report: BrowserReport = serde_json::from_str(json).unwrap();

        assert_json_snapshot!(report, @r#"
        {
          "age": 0,
          "url": "https://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36",
          "type": "coop",
          "body": {
            "type": "access-from-coop-page-to-openee",
            "disposition": "reporting",
            "effectivePolicy": "same-origin",
            "property": "postMessage",
            "previousResponseURL": null,
            "nextResponseURL": null,
            "openerURL": null,
            "openeeURL": "",
            "initialPopupURL": "https://example.org/popup",
            "otherDocumentURL": null,
            "referrer": null
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
