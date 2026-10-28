use std::collections::HashMap;
use std::sync::LazyLock;

use relay_browser_reporting::nel::*;
use relay_conventions::{attributes::*, interpolate};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel};
use relay_protocol::Annotated;

/// Mapping of NEL error types to their human-readable descriptions.
///
/// Based on W3C Network Error Logging specification and Chromium-specific extensions.
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

/// Lazy-initialized HashMap for fast NEL error type lookups.
static NEL_CULPRITS_MAP: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| NEL_CULPRITS.iter().copied().collect());

/// Gets the human-readable description for a NEL error type.
fn get_nel_culprit(error_type: &str) -> Option<&'static str> {
    NEL_CULPRITS_MAP.get(error_type).copied()
}

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
fn create_message(report_type: &ReportType, status_code: Option<u16>) -> String {
    match report_type {
        ReportType::Ok => "ok".to_owned(),
        ReportType::Error(error_type) => get_nel_culprit_formatted(error_type, status_code)
            .unwrap_or_else(|| error_type.to_owned()),
    }
}

/// Creates a log from the provided NEL [`Body`].
pub fn create_log_from_nel_body(body: Body, mut attributes: Attributes) -> OurLog {
    let Body {
        elapsed_time,
        phase,
        sampling_fraction,
        ty,
    } = body;

    add_attribute!(attributes, HTTP__REQUEST__DURATION, elapsed_time as i64);
    add_attribute!(attributes, NEL__PHASE, phase.name());
    add_attribute!(attributes, NEL__TYPE, ty.to_string());

    let status_code = match &phase {
        NetworkReportPhase::Application { status_code, .. } => Some(*status_code),
        _ => None,
    };

    let message = create_message(&ty, status_code);

    match phase {
        NetworkReportPhase::Dns => {}
        NetworkReportPhase::Connection {
            protocol,
            server_ip,
        } => {
            if let Some((protocol, version)) = protocol.split_once('/') {
                add_string_attribute!(attributes, NETWORK__PROTOCOL__NAME, protocol);
                add_string_attribute!(attributes, NETWORK__PROTOCOL__VERSION, version);
            }

            if let Some(server_ip) = server_ip {
                add_string_attribute!(attributes, SERVER__ADDRESS, server_ip);
            }
        }
        NetworkReportPhase::Application {
            method,
            protocol,
            referrer,
            server_ip,
            status_code,
        } => {
            if let Some((protocol, version)) = protocol.split_once('/') {
                add_string_attribute!(attributes, NETWORK__PROTOCOL__NAME, protocol);
                add_string_attribute!(attributes, NETWORK__PROTOCOL__VERSION, version);
            }

            if let Some(server_ip) = server_ip {
                add_string_attribute!(attributes, SERVER__ADDRESS, server_ip);
            }

            add_attribute!(attributes, HTTP__REQUEST__METHOD, method);

            let referrer_key = interpolate::http__request__header__key("referer");
            add_attribute!(attributes, referrer_key, referrer.clone());
            add_attribute!(attributes, HTTP__RESPONSE__STATUS_CODE, status_code as i64);
            add_attribute!(attributes, NEL__REFERRER, referrer);
            add_attribute!(attributes, NEL__SAMPLING_FRACTION, sampling_fraction);
        }
    }

    let level = match ty {
        ReportType::Ok => OurLogLevel::Info,
        ReportType::Error(_) => OurLogLevel::Warn,
    };

    OurLog {
        level: Annotated::new(level),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                create_message(
                    &ReportType::Error(case.error_type.to_owned()),
                    case.status_code
                ),
                case.expected,
                "Failed for error_type: {}, status_code: {:?}",
                case.error_type,
                case.status_code
            );
        }
    }
}
