use chrono::{DateTime, Duration, Utc};

use relay_browser_reporting::{BrowserReport, BrowserReportBody};
use relay_conventions::attributes::*;
use relay_event_schema::protocol::{Attributes, OurLog, Timestamp, TraceId};
use relay_protocol::Annotated;

use url::Url;

macro_rules! add_attribute {
    ($attrs:ident, $name:ident, $value:expr) => {{
        $attrs.insert($name.to_owned(), $value);
    }};
}

macro_rules! add_string_attribute {
    ($attrs:ident, $name:ident, $value:expr) => {{
        let val = $value.to_string();
        if !val.is_empty() {
            $attrs.insert($name.to_owned(), val);
        }
    }};
}

mod nel;

/// Extracts the domain or IP address from a server address string.
///
/// e.g. 123.123.123.123 -> 123.123.123.123
/// e.g. https://example.com/foo?bar=1 -> example.com
/// e.g. http://localhost:8080/foo?bar=1 -> localhost
/// e.g. http://\[::1\]:8080/foo -> \[::1\]
#[allow(rustdoc::bare_urls)]
fn extract_server_address(server_address: &str) -> String {
    // Try to parse as URL and extract host
    if let Ok(url) = Url::parse(server_address)
        && let Some(host) = url.host_str()
    {
        return host.to_owned();
    }
    // Fallback: URL parsing failed or no host found, return original
    server_address.to_owned()
}

/// Creates a [`OurLog`] from the provided [`BrowserReport`].
pub fn browser_report_to_sentry_log(
    report: BrowserReport,
    received_at: DateTime<Utc>,
) -> Option<OurLog> {
    create_log_with_trace_id(report, received_at, None)
}

/// Creates a [`OurLog`] from the provided [`BrowserReport`] with an optional trace ID.
/// If trace_id is `None`, a random one will be generated.
fn create_log_with_trace_id(
    report: BrowserReport,
    received_at: DateTime<Utc>,
    trace_id: Option<TraceId>,
) -> Option<OurLog> {
    let BrowserReport {
        age,
        url,
        user_agent,
        body,
    } = report;

    let timestamp = received_at
        .checked_sub_signed(Duration::milliseconds(age as i64))
        .unwrap_or(received_at);

    let mut attributes: Attributes = Default::default();

    add_string_attribute!(attributes, SENTRY__ORIGIN, "auto.http.browser_report");
    add_string_attribute!(attributes, BROWSER__REPORT__TYPE, body.report_type());
    add_string_attribute!(attributes, USER_AGENT__ORIGINAL, user_agent);
    add_string_attribute!(attributes, URL__DOMAIN, extract_server_address(&url));
    add_string_attribute!(attributes, URL__FULL, url);

    let log = match body {
        BrowserReportBody::NetworkError(body) => nel::create_log_from_nel_body(body, attributes),
        BrowserReportBody::Unsupported => return None,
    };

    Some(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(trace_id.unwrap_or_else(TraceId::random)),
        ..log
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use relay_protocol::{Annotated, assert_annotated_snapshot};

    #[test]
    fn test_create_log_basic() {
        // Use a fixed timestamp for deterministic testing
        let received_at = DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let report: BrowserReport = serde_json::from_str(
            r#"{
                "age": 5000,
                "type": "network-error",
                "url": "https://example.com/api",
                "user_agent": "Mozilla/5.0",
                "body": {
                    "type": "http.error",
                    "elapsed_time": 1000,
                    "phase": "application",
                    "method": "GET",
                    "protocol": "http/1.1",
                    "referrer": "https://example.com/referer",
                    "server_ip": "192.168.1.1",
                    "status_code": 500,
                    "sampling_fraction": 1.0
                }
        }"#,
        )
        .unwrap();

        let trace_id: TraceId = "de4e189601a342c6bee991645300852e".parse().unwrap();
        let log = create_log_with_trace_id(report, received_at, Some(trace_id)).unwrap();

        assert_annotated_snapshot!(&Annotated::new(log), @r#"
        {
          "timestamp": 1619424000.0,
          "trace_id": "de4e189601a342c6bee991645300852e",
          "level": "warn",
          "body": "The user agent successfully received a response, but it had a 500 status code",
          "attributes": {
            "browser.report.type": {
              "type": "string",
              "value": "network-error"
            },
            "http.request.duration": {
              "type": "integer",
              "value": 1000
            },
            "http.request.header.referer": {
              "type": "string",
              "value": "https://example.com/referer"
            },
            "http.request.method": {
              "type": "string",
              "value": "GET"
            },
            "http.response.status_code": {
              "type": "integer",
              "value": 500
            },
            "nel.phase": {
              "type": "string",
              "value": "application"
            },
            "nel.referrer": {
              "type": "string",
              "value": "https://example.com/referer"
            },
            "nel.sampling_fraction": {
              "type": "double",
              "value": 1.0
            },
            "nel.type": {
              "type": "string",
              "value": "http.error"
            },
            "network.protocol.name": {
              "type": "string",
              "value": "http"
            },
            "network.protocol.version": {
              "type": "string",
              "value": "1.1"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.http.browser_report"
            },
            "server.address": {
              "type": "string",
              "value": "192.168.1.1"
            },
            "url.domain": {
              "type": "string",
              "value": "example.com"
            },
            "url.full": {
              "type": "string",
              "value": "https://example.com/api"
            },
            "user_agent.original": {
              "type": "string",
              "value": "Mozilla/5.0"
            }
          }
        }
        "#);
    }

    #[test]
    fn test_create_log_dns_error() {
        let received_at = DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let report: BrowserReport = serde_json::from_str(
            r#"{
                "age": 1000,
                "type": "network-error",
                "url": "https://api.example.com/v1/users",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "body": {
                    "type": "dns.unreachable",
                    "elapsed_time": 2000,
                    "phase": "dns",
                    "sampling_fraction": 0.5
                }
            }"#,
        )
        .unwrap();

        let trace_id: TraceId = "825d2fca3bcd40d390a0b39fe7102e90".parse().unwrap();
        let log = create_log_with_trace_id(report, received_at, Some(trace_id)).unwrap();
        assert_annotated_snapshot!(
            &Annotated::new(log),
            @r#"
        {
          "timestamp": 1619424004.0,
          "trace_id": "825d2fca3bcd40d390a0b39fe7102e90",
          "level": "warn",
          "body": "DNS server is unreachable",
          "attributes": {
            "browser.report.type": {
              "type": "string",
              "value": "network-error"
            },
            "http.request.duration": {
              "type": "integer",
              "value": 2000
            },
            "nel.phase": {
              "type": "string",
              "value": "dns"
            },
            "nel.sampling_fraction": {
              "type": "double",
              "value": 0.5
            },
            "nel.type": {
              "type": "string",
              "value": "dns.unreachable"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.http.browser_report"
            },
            "url.domain": {
              "type": "string",
              "value": "api.example.com"
            },
            "url.full": {
              "type": "string",
              "value": "https://api.example.com/v1/users"
            },
            "user_agent.original": {
              "type": "string",
              "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            }
          }
        }
        "#
        );
    }

    #[test]
    fn test_create_log_unsupported_report() {
        let received_at = DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let report: BrowserReport = serde_json::from_str(
            r#"{
                "age": 1000,
                "type": "future-magic",
                "url": "https://api.example.com/v1/users",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "body": {
                    "frobnicated": true,
                    "mana_used": 477
                }
            }"#,
        )
        .unwrap();

        let trace_id: TraceId = "825d2fca3bcd40d390a0b39fe7102e90".parse().unwrap();
        assert!(create_log_with_trace_id(report, received_at, Some(trace_id)).is_none());
    }
}
