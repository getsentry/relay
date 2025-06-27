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

/// Gets the human-readable description for a NEL error type
fn get_nel_culprit(error_type: &str) -> Option<&'static str> {
    NEL_CULPRITS
        .iter()
        .find(|(key, _)| *key == error_type)
        .map(|(_, value)| *value)
}

/// Gets the formatted human-readable description for a NEL error type
/// For http.error, formats the message with the provided status code
fn get_nel_culprit_formatted(error_type: &str, status_code: Option<u16>) -> Option<String> {
    let template = get_nel_culprit(error_type)?;

    if error_type == "http.error" {
        let code = status_code.unwrap_or(0);
        Some(template.replace("{}", &code.to_string()))
    } else {
        Some(template.to_string())
    }
}

/// Extracts the domain or IP address from a server address string
/// e.g. "123.123.123.123" -> "123.123.123.123"
/// e.g. "https://example.com/foo?bar=1" -> "example.com"
/// e.g. "http://localhost:8080/foo?bar=1" -> "localhost"
/// e.g. "http://[::1]:8080/foo" -> "[::1]"
fn extract_server_address(server_address: &str) -> String {
    // If it looks like a URL, extract the host part
    if server_address.starts_with("http://") || server_address.starts_with("https://") {
        if let Some(url_part) = server_address.split("://").nth(1) {
            // Extract host before any path, query, or fragment
            let host_port = url_part
                .split('/')
                .next()
                .and_then(|host_port| host_port.split('?').next())
                .and_then(|host_port| host_port.split('#').next())
                .unwrap_or(server_address);

            // Handle IPv6 addresses (enclosed in brackets) vs regular hosts
            if host_port.starts_with('[') {
                // IPv6 address like [::1]:8080 -> [::1]
                if let Some(bracket_end) = host_port.find(']') {
                    host_port[..=bracket_end].to_string()
                } else {
                    host_port.to_string()
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
