//! Contains helper function for Integrity reports.

use chrono::{DateTime, Duration, Utc};
use relay_event_schema::protocol::{
    Attributes, IntegrityBodyRaw, IntegrityReportRaw, OurLog, OurLogLevel, Timestamp, TraceId,
};
use relay_protocol::Annotated;
use url::Url;

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

/// Creates a [`OurLog`] from the provided [`IntegrityReportRaw`].
pub fn create_log(
    integrity: Annotated<IntegrityReportRaw>,
    received_at: DateTime<Utc>,
) -> Option<OurLog> {
    create_log_with_trace_id(integrity, received_at, None)
}

/// Integrity-specific human-readable text displayed
fn create_message(body: &IntegrityBodyRaw) -> String {
    let destination = body.destination.as_str().unwrap_or("resource");
    let blocked_url = body.blocked_url.as_str().unwrap_or("unknown");
    let report_only = body.report_only.value().copied().unwrap_or(false);

    if report_only {
        format!(
            "Integrity policy would block {destination} from '{blocked_url}', but does not due to report only configuration"
        )
    } else {
        format!("Integrity policy blocked {destination} from '{blocked_url}'")
    }
}

/// Creates a [`OurLog`] from the provided [`IntegrityReportRaw`] with an optional trace ID.
/// If trace_id is None, a random one will be generated.
pub fn create_log_with_trace_id(
    integrity: Annotated<IntegrityReportRaw>,
    received_at: DateTime<Utc>,
    trace_id: Option<TraceId>,
) -> Option<OurLog> {
    let raw_report = integrity.into_value()?;
    if raw_report.ty.as_str() != Some("integrity-violation") {
        return None;
    }
    let body = raw_report.body.into_value()?;
    let message = create_message(&body);

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
            let val = $value.to_string();
            if !val.is_empty() {
                attributes.insert($name.to_owned(), val);
            }
        }};
    }

    add_string_attribute!("sentry.origin", "auto.http.browser_report.integrity");
    add_string_attribute!("browser.report.type", "integrity-violation");

    // Handle URL and extract server address if available
    if let Some(url_str) = raw_report.url.value() {
        let url_domain = extract_server_address(url_str);
        add_string_attribute!("url.domain", &url_domain);
    }
    add_attribute!("url.full", raw_report.url);

    // Integrity-specific attributes
    add_attribute!("integrity.document_url", body.document_url);
    add_attribute!("integrity.blocked_url", body.blocked_url);
    add_attribute!("integrity.destination", body.destination);
    add_attribute!("integrity.report_only", body.report_only);

    Some(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(trace_id.unwrap_or_else(TraceId::random)),
        level: Annotated::new(OurLogLevel::Warn),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}
