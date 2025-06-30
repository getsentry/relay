//! Contains helper function for NEL reports.

use chrono::{DateTime, Duration, Utc};
use relay_event_schema::protocol::{
    Attributes, NetworkReportRaw, OurLog, OurLogLevel, Timestamp, TraceId,
};
use relay_protocol::Annotated;
use std::collections::HashMap;
use std::sync::LazyLock;
use url::Url;

/// Mapping of NEL error types to their human-readable descriptions
/// Based on W3C Network Error Logging specification and Chromium-specific extensions
static NEL_CULPRITS: &[(&str, &str)] = &[
    // https://w3c.github.io/network-error-logging/#predefined-network-error-types
    ("dns.unreachable", "DNS server is unreachable"),
    (
        "dns.name_not_resolved",
        "DNS server responded but is unable to resolve the address",
    ),
    (
        "dns.failed",
        "Request to the DNS server failed due to reasons not covered by previous errors",
    ),
    (
        "dns.address_changed",
        "Indicates that the resolved IP address for a request's origin has changed since the corresponding NEL policy was received",
    ),
    ("tcp.timed_out", "TCP connection to the server timed out"),
    ("tcp.closed", "The TCP connection was closed by the server"),
    ("tcp.reset", "The TCP connection was reset"),
    (
        "tcp.refused",
        "The TCP connection was refused by the server",
    ),
    ("tcp.aborted", "The TCP connection was aborted"),
    ("tcp.address_invalid", "The IP address is invalid"),
    ("tcp.address_unreachable", "The IP address is unreachable"),
    (
        "tcp.failed",
        "The TCP connection failed due to reasons not covered by previous errors",
    ),
    (
        "tls.version_or_cipher_mismatch",
        "The TLS connection was aborted due to version or cipher mismatch",
    ),
    (
        "tls.bad_client_auth_cert",
        "The TLS connection was aborted due to invalid client certificate",
    ),
    (
        "tls.cert.name_invalid",
        "The TLS connection was aborted due to invalid name",
    ),
    (
        "tls.cert.date_invalid",
        "The TLS connection was aborted due to invalid certificate date",
    ),
    (
        "tls.cert.authority_invalid",
        "The TLS connection was aborted due to invalid issuing authority",
    ),
    (
        "tls.cert.invalid",
        "The TLS connection was aborted due to invalid certificate",
    ),
    (
        "tls.cert.revoked",
        "The TLS connection was aborted due to revoked server certificate",
    ),
    (
        "tls.cert.pinned_key_not_in_cert_chain",
        "The TLS connection was aborted due to a key pinning error",
    ),
    (
        "tls.protocol.error",
        "The TLS connection was aborted due to a TLS protocol error",
    ),
    (
        "tls.failed",
        "The TLS connection failed due to reasons not covered by previous errors",
    ),
    (
        "http.error",
        "The user agent successfully received a response, but it had a {} status code",
    ),
    (
        "http.protocol.error",
        "The connection was aborted due to an HTTP protocol error",
    ),
    (
        "http.response.invalid",
        "Response is empty, has a content-length mismatch, has improper encoding, and/or other conditions that prevent user agent from processing the response",
    ),
    (
        "http.response.redirect_loop",
        "The request was aborted due to a detected redirect loop",
    ),
    (
        "http.failed",
        "The connection failed due to errors in HTTP protocol not covered by previous errors",
    ),
    (
        "abandoned",
        "User aborted the resource fetch before it is complete",
    ),
    ("unknown", "error type is unknown"),
    // Chromium-specific errors, not documented in the spec
    // https://chromium.googlesource.com/chromium/src/+/HEAD/net/network_error_logging/network_error_logging_service.cc
    ("dns.protocol", "ERR_DNS_MALFORMED_RESPONSE"),
    ("dns.server", "ERR_DNS_SERVER_FAILED"),
    (
        "tls.unrecognized_name_alert",
        "ERR_SSL_UNRECOGNIZED_NAME_ALERT",
    ),
    ("h2.ping_failed", "ERR_HTTP2_PING_FAILED"),
    ("h2.protocol.error", "ERR_HTTP2_PROTOCOL_ERROR"),
    ("h3.protocol.error", "ERR_QUIC_PROTOCOL_ERROR"),
    ("http.response.invalid.empty", "ERR_EMPTY_RESPONSE"),
    (
        "http.response.invalid.content_length_mismatch",
        "ERR_CONTENT_LENGTH_MISMATCH",
    ),
    (
        "http.response.invalid.incomplete_chunked_encoding",
        "ERR_INCOMPLETE_CHUNKED_ENCODING",
    ),
    (
        "http.response.invalid.invalid_chunked_encoding",
        "ERR_INVALID_CHUNKED_ENCODING",
    ),
    (
        "http.request.range_not_satisfiable",
        "ERR_REQUEST_RANGE_NOT_SATISFIABLE",
    ),
    (
        "http.response.headers.truncated",
        "ERR_RESPONSE_HEADERS_TRUNCATED",
    ),
    (
        "http.response.headers.multiple_content_disposition",
        "ERR_RESPONSE_HEADERS_MULTIPLE_CONTENT_DISPOSITION",
    ),
    (
        "http.response.headers.multiple_content_length",
        "ERR_RESPONSE_HEADERS_MULTIPLE_CONTENT_LENGTH",
    ),
];

/// Lazy-initialized HashMap for fast NEL error type lookups
static NEL_CULPRITS_MAP: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| NEL_CULPRITS.iter().copied().collect());

/// Extracts the domain or IP address from a server address string
/// e.g. "123.123.123.123" -> "123.123.123.123"
/// e.g. "https://example.com/foo?bar=1" -> "example.com"
/// e.g. "http://localhost:8080/foo?bar=1" -> "localhost"
/// e.g. "http://[::1]:8080/foo" -> "[::1]"
fn extract_server_address(server_address: &str) -> String {
    // Try to parse as URL and extract host
    if let Ok(url) = Url::parse(server_address) {
        if let Some(host) = url.host_str() {
            return host.to_owned();
        }
    }
    // Fallback: URL parsing failed or no host found, return original
    server_address.to_owned()
}

/// Gets the human-readable description for a NEL error type
fn get_nel_culprit(error_type: &str) -> Option<&'static str> {
    NEL_CULPRITS_MAP.get(error_type).copied()
}

/// Gets the formatted human-readable description for a NEL error type with optional status code
fn get_nel_culprit_formatted(error_type: &str, status_code: Option<u16>) -> Option<String> {
    let template = get_nel_culprit(error_type)?;

    if error_type == "http.error" {
        let code = status_code.unwrap_or(0);
        Some(template.replace("{}", &code.to_string()))
    } else {
        Some(template.to_owned())
    }
}

/// Creates a human-readable message for a NEL report
fn create_message(error_type: &str, status_code: Option<u16>) -> String {
    get_nel_culprit_formatted(error_type, status_code).unwrap_or_else(|| error_type.to_owned())
}

/// Creates a [`OurLog`] from the provided [`NetworkReportRaw`].
pub fn create_log(nel: Annotated<NetworkReportRaw>, received_at: DateTime<Utc>) -> Option<OurLog> {
    let raw_report = nel.into_value()?;
    let body = raw_report.body.into_value()?;

    // Extract the error type string to avoid borrowing issues later
    let error_type = body.ty.as_str().unwrap_or("unknown");
    let message = create_message(
        error_type,
        body.status_code.value().map(|&code| code as u16),
    );

    let timestamp = received_at
        .checked_sub_signed(Duration::milliseconds(
            *raw_report.age.value().unwrap_or(&0),
        ))
        .unwrap_or(received_at);

    let mut attributes: Attributes = Default::default();

    macro_rules! add_attribute {
        ($name:literal, $value:expr) => {{
            if let Some(value) = $value.into_value() {
                attributes.insert($name.to_owned(), value);
            }
        }};
    }

    macro_rules! add_string_attribute {
        ($name:literal, $value:expr) => {{
            attributes.insert($name.to_owned(), $value.to_string());
        }};
    }

    let server_address = body
        .server_ip
        .map_value(|s| extract_server_address(s.as_ref()));
    let url = raw_report
        .url
        .clone()
        .map_value(|s| extract_server_address(&s));

    // sentry.origin: https://github.com/getsentry/sentry-docs/blob/1570dd4207d3d8996ca03198229579d36a980a6a/develop-docs/sdk/telemetry/logs.mdx?plain=1#L302-L310
    add_string_attribute!("sentry.origin", "auto.http.browser_report.nel");
    add_string_attribute!("browser.report.type", "network-error");
    add_attribute!("url.domain", url);
    add_attribute!("url.full", raw_report.url);
    add_attribute!("http.request.duration", body.elapsed_time);
    add_attribute!("http.request.method", body.method);
    add_attribute!("http.request.header.referer", body.referrer.clone());
    add_attribute!("http.response.status_code", body.status_code);
    // Split protocol into name and version components
    if let Some(protocol) = body.protocol.value() {
        let parts: Vec<&str> = protocol.split('/').collect();
        if !parts.is_empty() {
            // e.g. "http"
            add_string_attribute!("network.protocol.name", parts[0]);
            if parts.len() > 1 {
                // e.g. "1.1"
                add_string_attribute!("network.protocol.version", parts[1]);
            }
        }
    }
    // Server domain name if available without reverse DNS lookup; otherwise,
    // IP address or Unix domain socket name.
    add_attribute!("server.address", server_address);

    // NEL-specific attributes
    add_attribute!("nel.referrer", body.referrer);
    add_attribute!("nel.phase", body.phase.map_value(|s| s.to_string()));
    add_attribute!("nel.sampling_fraction", body.sampling_fraction);
    add_attribute!("nel.type", body.ty.clone());

    Some(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(TraceId::from(uuid::Uuid::nil())),
        level: Annotated::new(if error_type == "ok" {
            OurLogLevel::Info
        } else {
            OurLogLevel::Warn
        }),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use relay_event_schema::protocol::{BodyRaw, IpAddr, NetworkReportPhases};
    use relay_protocol::{Annotated, SerializableAnnotated};

    #[test]
    fn test_create_message() {
        // Test cases for create_message
        struct CreateMessageCase {
            error_type: &'static str,
            status_code: Option<u16>,
            expected: &'static str,
        }

        let message_cases = vec![
            CreateMessageCase {
                error_type: "dns.unreachable",
                status_code: None,
                expected: "DNS server is unreachable",
            },
            // This tests the status code replacement in the message
            CreateMessageCase {
                error_type: "http.error",
                status_code: Some(404),
                expected: "The user agent successfully received a response, but it had a 404 status code",
            },
            // Unknown errors do not get a human-friendly message, but the error type is preserved
            CreateMessageCase {
                error_type: "http.some_new_error",
                status_code: None,
                expected: "http.some_new_error",
            },
        ];

        for case in message_cases {
            assert_eq!(
                create_message(case.error_type, case.status_code),
                case.expected,
                "Failed for error_type: {}, status_code: {:?}",
                case.error_type,
                case.status_code
            );
        }
    }

    #[test]
    fn test_create_log_basic() {
        // Use a fixed timestamp for deterministic testing
        let received_at = DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let body = BodyRaw {
            ty: Annotated::new("http.error".to_owned()),
            status_code: Annotated::new(500),
            elapsed_time: Annotated::new(1000),
            method: Annotated::new("GET".to_owned()),
            protocol: Annotated::new("http/1.1".to_owned()),
            server_ip: Annotated::new(IpAddr("192.168.1.1".to_owned())),
            phase: Annotated::new(NetworkReportPhases::Application),
            sampling_fraction: Annotated::new(1.0),
            referrer: Annotated::new("https://example.com/referer".to_owned()),
            ..Default::default()
        };

        let report = NetworkReportRaw {
            age: Annotated::new(5000),
            ty: Annotated::new("network-error".to_owned()),
            url: Annotated::new("https://example.com/api".to_owned()),
            user_agent: Annotated::new("Mozilla/5.0".to_owned()),
            body: Annotated::new(body),
            ..Default::default()
        };

        let log = create_log(Annotated::new(report), received_at).unwrap();
        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }

    #[test]
    fn test_create_log_missing_body() {
        let received_at = DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let nel_missing_body = NetworkReportRaw {
            body: Annotated::empty(),
            ..Default::default()
        };

        let log = create_log(Annotated::new(nel_missing_body), received_at);
        // No log is created because the body is missing
        assert!(log.is_none());
    }

    #[test]
    fn test_create_log_empty_nel() {
        let received_at = DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let log = create_log(Annotated::empty(), received_at);
        // No log is created because the NEL is empty
        assert!(log.is_none());
    }

    #[test]
    fn test_create_log_dns_error() {
        let received_at = DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let body = BodyRaw {
            ty: Annotated::new("dns.unreachable".to_owned()),
            elapsed_time: Annotated::new(2000),
            method: Annotated::new("POST".to_owned()),
            protocol: Annotated::new("http/2".to_owned()),
            server_ip: Annotated::new(IpAddr("10.0.0.1".to_owned())),
            phase: Annotated::new(NetworkReportPhases::DNS),
            sampling_fraction: Annotated::new(0.5),
            ..Default::default()
        };

        let report = NetworkReportRaw {
            age: Annotated::new(1000),
            ty: Annotated::new("network-error".to_owned()),
            url: Annotated::new("https://api.example.com/v1/users".to_owned()),
            user_agent: Annotated::new(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)".to_owned(),
            ),
            body: Annotated::new(body),
            ..Default::default()
        };

        let log = create_log(Annotated::new(report), received_at).unwrap();
        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }
}
