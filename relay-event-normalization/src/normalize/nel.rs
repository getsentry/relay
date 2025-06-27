//! Contains helper function for NEL reports.

use chrono::{DateTime, Duration, Utc};
use relay_event_schema::protocol::{
    Attributes, NetworkReportRaw, OurLog, OurLogLevel, Timestamp, TraceId,
};
use relay_protocol::Annotated;

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

/// Extracts the domain or IP address from a server address string
/// e.g. "123.123.123.123" -> "123.123.123.123"
/// e.g. "https://example.com/foo?bar=1" -> "example.com"
/// e.g. "http://localhost:8080/foo?bar=1" -> "localhost"
/// e.g. "http://[::1]:8080/foo" -> "[::1]"
fn extract_server_address(server_address: &str) -> String {
    // If it looks like a URL, extract the host part
    if server_address.starts_with("http://") || server_address.starts_with("https://") {
        if let Some(url_part) = server_address.split("://").nth(1) {
            // If there's nothing after the protocol, return the original
            if url_part.is_empty() {
                return server_address.to_string();
            }

            // Extract host before any path, query, or fragment
            let host_port = url_part
                .split('/')
                .next()
                .and_then(|host_port| host_port.split('?').next())
                .and_then(|host_port| host_port.split('#').next())
                .unwrap_or(url_part);

            // Handle IPv6 addresses (enclosed in brackets) vs regular hosts
            if host_port.starts_with('[') {
                // IPv6 address like [::1]:8080 -> [::1]
                if let Some(bracket_end) = host_port.find(']') {
                    host_port[..=bracket_end].to_string()
                } else {
                    // Malformed IPv6, return the original url_part
                    url_part.to_string()
                }
            } else {
                // Regular host with potential port like localhost:8080 -> localhost
                host_port.split(':').next().unwrap_or(host_port).to_string()
            }
        } else {
            server_address.to_string()
        }
    } else {
        // Assume it's already an IP address or domain, keep as-is
        server_address.to_string()
    }
}

/// Gets the human-readable description for a NEL error type
fn get_nel_culprit(error_type: &str) -> Option<&'static str> {
    NEL_CULPRITS
        .iter()
        .find(|(key, _)| *key == error_type)
        .map(|(_, value)| *value)
}

/// Gets the formatted human-readable description for a NEL error type with optional status code
fn get_nel_culprit_formatted(error_type: &str, status_code: Option<u16>) -> Option<String> {
    let template = get_nel_culprit(error_type)?;

    if error_type == "http.error" {
        let code = status_code.unwrap_or(0);
        Some(template.replace("{}", &code.to_string()))
    } else {
        Some(template.to_string())
    }
}

/// Creates a human-readable message for a NEL report
fn create_message(error_type: &str, status_code: Option<u16>) -> String {
    get_nel_culprit_formatted(error_type, status_code).unwrap_or_else(|| error_type.to_string())
}

/// Creates a [`OurLog`] from the provided [`NetworkReportRaw`].
pub fn create_log(nel: Annotated<NetworkReportRaw>, received_at: DateTime<Utc>) -> Option<OurLog> {
    let raw_report = nel.into_value()?;
    let body = raw_report.body.into_value()?;

    let message = create_message(
        body.ty.as_str().unwrap_or("unknown"),
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
        .map_value(|s| extract_server_address(&s.to_string()));
    let url = raw_report
        .url
        .clone()
        .map_value(|s| extract_server_address(&s));

    // sentry.origin: https://github.com/getsentry/sentry-docs/blob/1570dd4207d3d8996ca03198229579d36a980a6a/develop-docs/sdk/telemetry/logs.mdx?plain=1#L302-L310
    add_string_attribute!("sentry.origin", "auto.http.browser_reports.nel");
    add_string_attribute!("report_type", "network-error");
    add_attribute!("url.domain", url);
    add_attribute!("url.full", raw_report.url);
    // TODO: Add to sentry-conventions
    add_attribute!("http.request.duration", body.elapsed_time);
    add_attribute!("http.request.method", body.method);
    // TODO: Add to sentry-conventions
    // TODO: Discuss if to split its different URL parts into different attributes
    add_attribute!("http.request.header.referer", body.referrer);
    add_attribute!("http.response.status_code", body.status_code);
    // TODO: Discuss if to split into network.protocol.name and network.protocol.version
    add_attribute!("network.protocol", body.protocol);
    // Server domain name if available without reverse DNS lookup; otherwise,
    // IP address or Unix domain socket name.
    add_attribute!("server.address", server_address);

    add_attribute!("nel.phase", body.phase.map_value(|s| s.to_string()));
    add_attribute!("nel.sampling_fraction", body.sampling_fraction);
    add_attribute!("nel.type", body.ty);

    Some(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(TraceId::random()),
        level: Annotated::new(OurLogLevel::Info),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use relay_event_schema::protocol::{BodyRaw, IpAddr, NetworkReportPhases};
    use relay_protocol::{Annotated, Val};

    #[test]
    fn test_get_nel_culprit() {
        assert_eq!(
            get_nel_culprit("dns.unreachable"),
            Some("DNS server is unreachable")
        );
        assert_eq!(
            get_nel_culprit("tcp.timed_out"),
            Some("TCP connection to the server timed out")
        );
        assert_eq!(
            get_nel_culprit("http.error"),
            Some("The user agent successfully received a response, but it had a {} status code")
        );
        assert_eq!(get_nel_culprit("unknown"), Some("error type is unknown"));
        assert_eq!(get_nel_culprit("nonexistent"), None);
    }

    #[test]
    fn test_get_nel_culprit_formatted() {
        // Test http.error with status code
        assert_eq!(
            get_nel_culprit_formatted("http.error", Some(500)),
            Some(
                "The user agent successfully received a response, but it had a 500 status code"
                    .to_string()
            )
        );

        // Test http.error without status code
        assert_eq!(
            get_nel_culprit_formatted("http.error", None),
            Some(
                "The user agent successfully received a response, but it had a 0 status code"
                    .to_string()
            )
        );

        // Test non-http.error types
        assert_eq!(
            get_nel_culprit_formatted("dns.unreachable", None),
            Some("DNS server is unreachable".to_string())
        );

        // Test unknown error type
        assert_eq!(get_nel_culprit_formatted("nonexistent", None), None);
    }

    #[test]
    fn test_extract_server_address_ip() {
        assert_eq!(extract_server_address("123.123.123.123"), "123.123.123.123");
        assert_eq!(extract_server_address("192.168.1.1"), "192.168.1.1");
    }

    #[test]
    fn test_extract_server_address_simple_urls() {
        assert_eq!(
            extract_server_address("https://example.com/foo?bar=1"),
            "example.com"
        );
        assert_eq!(
            extract_server_address("http://localhost:8080/foo?bar=1"),
            "localhost"
        );
        assert_eq!(
            extract_server_address("https://sub.example.com/path"),
            "sub.example.com"
        );
    }

    #[test]
    fn test_extract_server_address_ipv6() {
        assert_eq!(extract_server_address("http://[::1]:8080/foo"), "[::1]");
        assert_eq!(
            extract_server_address("https://[2001:db8::1]:443/path"),
            "[2001:db8::1]"
        );
        // Malformed IPv6 (missing closing bracket)
        assert_eq!(
            extract_server_address("http://[::1:8080/foo"),
            "[::1:8080/foo"
        );
    }

    #[test]
    fn test_extract_server_address_ports() {
        assert_eq!(
            extract_server_address("https://example.com:443/path"),
            "example.com"
        );
        assert_eq!(extract_server_address("http://localhost:3000"), "localhost");
    }

    #[test]
    fn test_extract_server_address_edge_cases() {
        // Empty URL part after protocol
        assert_eq!(extract_server_address("https://"), "https://");
        // No protocol
        assert_eq!(extract_server_address("example.com"), "example.com");
        // URL with fragments and queries
        assert_eq!(
            extract_server_address("https://example.com/path?query=value#fragment"),
            "example.com"
        );
    }

    #[test]
    fn test_create_message() {
        assert_eq!(
            create_message("dns.unreachable", None),
            "DNS server is unreachable"
        );
        assert_eq!(
            create_message("http.error", Some(404)),
            "The user agent successfully received a response, but it had a 404 status code"
        );
        assert_eq!(create_message("unknown_error", None), "unknown_error");
    }

    #[test]
    fn test_create_log_basic() {
        let received_at = Utc::now();

        let body = BodyRaw {
            ty: Annotated::new("http.error".to_string()),
            status_code: Annotated::new(500),
            elapsed_time: Annotated::new(1000),
            method: Annotated::new("GET".to_string()),
            protocol: Annotated::new("http/1.1".to_string()),
            server_ip: Annotated::new(IpAddr("192.168.1.1".to_string())),
            phase: Annotated::new(NetworkReportPhases::Application),
            sampling_fraction: Annotated::new(1.0),
            referrer: Annotated::new("https://example.com/referer".to_string()),
            ..Default::default()
        };

        let nel = NetworkReportRaw {
            age: Annotated::new(5000),
            ty: Annotated::new("network-error".to_string()),
            url: Annotated::new("https://example.com/api".to_string()),
            user_agent: Annotated::new("Mozilla/5.0".to_string()),
            body: Annotated::new(body),
            ..Default::default()
        };

        let log = create_log(Annotated::new(nel), received_at).unwrap();

        // Check basic fields
        assert_eq!(log.level.into_value(), Some(OurLogLevel::Info));
        assert_eq!(
            log.body.into_value(),
            Some(
                "The user agent successfully received a response, but it had a 500 status code"
                    .to_string()
            )
        );

        // Check timestamp adjustment (should be received_at - age)
        let expected_timestamp = received_at
            .checked_sub_signed(Duration::milliseconds(5000))
            .unwrap();
        assert_eq!(
            log.timestamp.into_value().unwrap().into_inner(),
            expected_timestamp
        );

        // Check attributes
        let attributes = log.attributes.into_value().unwrap();
        assert_eq!(
            attributes.get_value("sentry.origin").unwrap().as_str(),
            Some("auto.http.browser_reports.nel")
        );
        assert_eq!(
            attributes.get_value("report_type").unwrap().as_str(),
            Some("network-error")
        );
        assert_eq!(
            attributes.get_value("url.domain").unwrap().as_str(),
            Some("example.com")
        );
        assert_eq!(
            attributes.get_value("url.full").unwrap().as_str(),
            Some("https://example.com/api")
        );
        assert_eq!(
            Val::from(attributes.get_value("http.request.duration").unwrap()).as_i64(),
            Some(1000)
        );
        assert_eq!(
            attributes
                .get_value("http.request.method")
                .unwrap()
                .as_str(),
            Some("GET")
        );
        assert_eq!(
            Val::from(attributes.get_value("http.response.status_code").unwrap()).as_i64(),
            Some(500)
        );
        assert_eq!(
            attributes.get_value("network.protocol").unwrap().as_str(),
            Some("http/1.1")
        );
        assert_eq!(
            attributes.get_value("server.address").unwrap().as_str(),
            Some("192.168.1.1")
        );
        assert_eq!(
            attributes.get_value("nel.phase").unwrap().as_str(),
            Some("application")
        );
        assert_eq!(
            attributes
                .get_value("nel.sampling_fraction")
                .unwrap()
                .as_f64(),
            Some(1.0)
        );
        assert_eq!(
            attributes.get_value("nel.type").unwrap().as_str(),
            Some("http.error")
        );
    }

    #[test]
    fn test_create_log_minimal() {
        let received_at = Utc::now();

        let body = BodyRaw {
            ty: Annotated::new("unknown".to_string()),
            ..Default::default()
        };

        let nel = NetworkReportRaw {
            body: Annotated::new(body),
            ..Default::default()
        };

        let log = create_log(Annotated::new(nel), received_at).unwrap();

        assert_eq!(
            log.body.into_value(),
            Some("error type is unknown".to_string())
        );
        assert_eq!(log.level.into_value(), Some(OurLogLevel::Info));
    }

    #[test]
    fn test_create_log_missing_body() {
        let received_at = Utc::now();

        let nel = NetworkReportRaw {
            body: Annotated::empty(),
            ..Default::default()
        };

        let result = create_log(Annotated::new(nel), received_at);
        assert!(result.is_none());
    }

    #[test]
    fn test_create_log_empty_nel() {
        let received_at = Utc::now();
        let result = create_log(Annotated::empty(), received_at);
        assert!(result.is_none());
    }
}
