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

mod csp;
mod integrity;
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
        BrowserReportBody::IntegrityViolation(body) => {
            integrity::create_log_from_integrity_body(body, attributes)
        }
        BrowserReportBody::CspViolation(body) => csp::create_log_from_csp_body(body, attributes),
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
    fn test_nel_application() {
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
                    "sampling_fraction": 1.0,
                    "request_headers": {"Accept-Language": ["de-DE"]},
                    "response_headers": {"X-Served-By": ["cache-fra-1", "cache-fra-2"]}
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
            "http.request.header.accept-language": {
              "type": "array",
              "value": [
                "de-DE"
              ]
            },
            "http.request.header.referer": {
              "type": "array",
              "value": [
                "https://example.com/referer"
              ]
            },
            "http.request.method": {
              "type": "string",
              "value": "GET"
            },
            "http.response.header.x-served-by": {
              "type": "array",
              "value": [
                "cache-fra-1",
                "cache-fra-2"
              ]
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
    fn test_nel_dns() {
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
    fn test_integrity_block() {
        let received_at = DateTime::parse_from_rfc3339("2026-10-06T12:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let report: BrowserReport = serde_json::from_str(
            r#"{
                "age": 5000,
                "type": "integrity-violation",
                "url": "https://example.com/index.html",
                "user_agent": "Mozilla/5.0",
                "body": {
                    "documentURL": "https://example.com/index.html",
                    "blockedURL": "https://cdn.example.com/app.js",
                    "destination": "script",
                    "reportOnly": false
                }
        }"#,
        )
        .unwrap();

        let trace_id: TraceId = "de4e189601a342c6bee991645300852e".parse().unwrap();
        let log = create_log_with_trace_id(report, received_at, Some(trace_id)).unwrap();

        assert_annotated_snapshot!(
            &Annotated::new(log), @r#"
        {
          "timestamp": 1791288000.0,
          "trace_id": "de4e189601a342c6bee991645300852e",
          "level": "warn",
          "body": "Integrity policy blocked script from 'https://cdn.example.com/app.js'",
          "attributes": {
            "browser.report.type": {
              "type": "string",
              "value": "integrity-violation"
            },
            "integrity.blocked_url": {
              "type": "string",
              "value": "https://cdn.example.com/app.js"
            },
            "integrity.destination": {
              "type": "string",
              "value": "script"
            },
            "integrity.document_url": {
              "type": "string",
              "value": "https://example.com/index.html"
            },
            "integrity.report_only": {
              "type": "boolean",
              "value": false
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.http.browser_report"
            },
            "url.domain": {
              "type": "string",
              "value": "example.com"
            },
            "url.full": {
              "type": "string",
              "value": "https://example.com/index.html"
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
    fn test_integrity_report_only() {
        let received_at = DateTime::parse_from_rfc3339("2026-10-06T12:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let report: BrowserReport = serde_json::from_str(
            r#"{
                "age": 5000,
                "type": "integrity-violation",
                "url": "https://example.com/index.html",
                "user_agent": "Mozilla/5.0",
                "body": {
                    "documentURL": "https://example.com/index.html",
                    "blockedURL": "https://cdn.example.com/app.js",
                    "destination": "script",
                    "reportOnly": true
                }
        }"#,
        )
        .unwrap();

        let trace_id: TraceId = "de4e189601a342c6bee991645300852e".parse().unwrap();
        let log = create_log_with_trace_id(report, received_at, Some(trace_id)).unwrap();

        assert_annotated_snapshot!(
            &Annotated::new(log), @r#"
        {
          "timestamp": 1791288000.0,
          "trace_id": "de4e189601a342c6bee991645300852e",
          "level": "warn",
          "body": "Integrity policy would block script from 'https://cdn.example.com/app.js', but does not due to report only configuration",
          "attributes": {
            "browser.report.type": {
              "type": "string",
              "value": "integrity-violation"
            },
            "integrity.blocked_url": {
              "type": "string",
              "value": "https://cdn.example.com/app.js"
            },
            "integrity.destination": {
              "type": "string",
              "value": "script"
            },
            "integrity.document_url": {
              "type": "string",
              "value": "https://example.com/index.html"
            },
            "integrity.report_only": {
              "type": "boolean",
              "value": true
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.http.browser_report"
            },
            "url.domain": {
              "type": "string",
              "value": "example.com"
            },
            "url.full": {
              "type": "string",
              "value": "https://example.com/index.html"
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
    fn test_csp_real() {
        let received_at = DateTime::parse_from_rfc3339("2026-10-06T12:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let report: BrowserReport = serde_json::from_str(r#"{
            "age": 0,
            "url": "https://sentry.io/",
            "user_agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            "type": "csp-violation",
            "body": {
                "documentURL": "https://sentry.io/sentry/csp/issues/88513416/",
                "referrer": "https://sentry.io/sentry/sentry/releases/7329107476ff14cfa19cf013acd8ce47781bb93a/",
                "effectiveDirective": "script-src",
                "originalPolicy": "default-src *; script-src 'make_csp_snapshot' 'unsafe-eval' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net cdn.ravenjs.com assets.zendesk.com ajax.googleapis.com ssl.google-analytics.com www.googleadservices.com analytics.twitter.com platform.twitter.com *.pingdom.net js.stripe.com api.stripe.com statuspage-production.s3.amazonaws.com s3.amazonaws.com *.google.com www.gstatic.com aui-cdn.atlassian.com *.atlassian.net *.jira.com *.zopim.com; font-src * data:; connect-src * wss://*.zopim.com; style-src 'make_csp_snapshot' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net s3.amazonaws.com aui-cdn.atlassian.com fonts.googleapis.com; img-src * data: blob:; report-uri https://sentry.io/api/54785/csp-report/?sentry_key=f724a8a027db45f5b21507e7142ff78e&sentry_release=39662eb9734f68e56b7f202260bb706be2f4cee7",
                "disposition": "enforce",
                "blockedURL": "http://baddomain.com/test.js?_=1515535030116",
                "lineNumber": 24,
                "columnNumber": 66270,
                "sourceFile": "https://e90d271df3e973c7.global.ssl.fastly.net/_static/f0c7c026a4b2a3d2b287ae2d012c9924/sentry/dist/vendor.js",
                "statusCode": 0,
                "script-sample": ""
            }
        }"#).unwrap();

        let trace_id: TraceId = "de4e189601a342c6bee991645300852e".parse().unwrap();
        let log = create_log_with_trace_id(report, received_at, Some(trace_id)).unwrap();

        assert_annotated_snapshot!(
            &Annotated::new(log), @r#"
        {
          "timestamp": 1791288005.0,
          "trace_id": "de4e189601a342c6bee991645300852e",
          "level": "warn",
          "body": "Blocked 'script' from 'baddomain.com'",
          "attributes": {
            "browser.report.type": {
              "type": "string",
              "value": "csp-violation"
            },
            "csp.blocked_url": {
              "type": "string",
              "value": "http://baddomain.com/test.js?_=1515535030116"
            },
            "csp.column_number": {
              "type": "integer",
              "value": 66270
            },
            "csp.document_url": {
              "type": "string",
              "value": "https://sentry.io/sentry/csp/issues/88513416/"
            },
            "csp.effective_directive": {
              "type": "string",
              "value": "script-src"
            },
            "csp.line_number": {
              "type": "integer",
              "value": 24
            },
            "csp.original_policy": {
              "type": "string",
              "value": "default-src *; script-src 'make_csp_snapshot' 'unsafe-eval' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net cdn.ravenjs.com assets.zendesk.com ajax.googleapis.com ssl.google-analytics.com www.googleadservices.com analytics.twitter.com platform.twitter.com *.pingdom.net js.stripe.com api.stripe.com statuspage-production.s3.amazonaws.com s3.amazonaws.com *.google.com www.gstatic.com aui-cdn.atlassian.com *.atlassian.net *.jira.com *.zopim.com; font-src * data:; connect-src * wss://*.zopim.com; style-src 'make_csp_snapshot' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net s3.amazonaws.com aui-cdn.atlassian.com fonts.googleapis.com; img-src * data: blob:; report-uri https://sentry.io/api/54785/csp-report/?sentry_key=f724a8a027db45f5b21507e7142ff78e&sentry_release=39662eb9734f68e56b7f202260bb706be2f4cee7"
            },
            "csp.report_only": {
              "type": "boolean",
              "value": false
            },
            "csp.source_file": {
              "type": "string",
              "value": "https://e90d271df3e973c7.global.ssl.fastly.net/_static/f0c7c026a4b2a3d2b287ae2d012c9924/sentry/dist/vendor.js"
            },
            "http.request.header.referer": {
              "type": "string",
              "value": "https://sentry.io/sentry/sentry/releases/7329107476ff14cfa19cf013acd8ce47781bb93a/"
            },
            "http.response.status_code": {
              "type": "integer",
              "value": 0
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.http.browser_report"
            },
            "url.domain": {
              "type": "string",
              "value": "sentry.io"
            },
            "url.full": {
              "type": "string",
              "value": "https://sentry.io/"
            },
            "user_agent.original": {
              "type": "string",
              "value": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"
            }
          }
        }
        "#);
    }

    #[test]
    fn test_unsupported_report() {
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
