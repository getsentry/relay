//! Contains helper function for Integrity reports.

use chrono::{DateTime, Duration, Utc};
use relay_conventions::attributes::{
    BROWSER__REPORT__TYPE, INTEGRITY__BLOCKED_URL, INTEGRITY__DESTINATION, INTEGRITY__DOCUMENT_URL,
    INTEGRITY__REPORT_ONLY, SENTRY__ORIGIN, URL__DOMAIN, URL__FULL,
};
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
        ($name:expr, $value:expr) => {{
            if let Some(value) = $value.into_value() {
                attributes.insert($name.to_owned(), value);
            }
        }};
    }

    macro_rules! add_string_attribute {
        ($name:expr, $value:expr) => {{
            let val = $value.to_string();
            if !val.is_empty() {
                attributes.insert($name.to_owned(), val);
            }
        }};
    }

    add_string_attribute!(SENTRY__ORIGIN, "auto.http.browser_report.integrity");
    add_string_attribute!(BROWSER__REPORT__TYPE, "integrity-violation");

    if let Some(url_str) = raw_report.url.value() {
        let url_domain = extract_server_address(url_str);
        add_string_attribute!(URL__DOMAIN, &url_domain);
    }

    add_attribute!(URL__FULL, raw_report.url);

    add_attribute!(INTEGRITY__DOCUMENT_URL, body.document_url);
    add_attribute!(INTEGRITY__BLOCKED_URL, body.blocked_url);
    add_attribute!(INTEGRITY__DESTINATION, body.destination);
    add_attribute!(INTEGRITY__REPORT_ONLY, body.report_only);

    Some(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(trace_id.unwrap_or_else(TraceId::random)),
        level: Annotated::new(OurLogLevel::Warn),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    fn make_report(report_only: bool) -> IntegrityReportRaw {
        IntegrityReportRaw {
            age: Annotated::new(5000),
            ty: Annotated::new("integrity-violation".to_owned()),
            url: Annotated::new("https://example.com/index.html".to_owned()),
            user_agent: Annotated::new("Mozilla/5.0".to_owned()),
            body: Annotated::new(IntegrityBodyRaw {
                document_url: Annotated::new("https://example.com/index.html".to_owned()),
                blocked_url: Annotated::new("https://cdn.example.com/app.js".to_owned()),
                destination: Annotated::new("script".to_owned()),
                report_only: Annotated::new(report_only),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_create_message_blocked() {
        let body = IntegrityBodyRaw {
            blocked_url: Annotated::new("https://cdn.example.com/app.js".to_owned()),
            destination: Annotated::new("script".to_owned()),
            report_only: Annotated::new(false),
            ..Default::default()
        };

        assert_eq!(
            create_message(&body),
            "Integrity policy blocked script from 'https://cdn.example.com/app.js'"
        );
    }

    #[test]
    fn test_create_message_report_only() {
        let body = IntegrityBodyRaw {
            blocked_url: Annotated::new("https://cdn.example.com/app.js".to_owned()),
            destination: Annotated::new("script".to_owned()),
            report_only: Annotated::new(true),
            ..Default::default()
        };

        assert_eq!(
            create_message(&body),
            "Integrity policy would block script from 'https://cdn.example.com/app.js', but does not due to report only configuration"
        );
    }

    #[test]
    fn test_create_log_basic() {
        let received_at = DateTime::parse_from_rfc3339("2026-10-06T12:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let fixed_trace_id: TraceId = "de4e189601a342c6bee991645300852e".parse().unwrap();

        let log = create_log_with_trace_id(
            Annotated::new(make_report(false)),
            received_at,
            Some(fixed_trace_id),
        )
        .unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }

    #[test]
    fn test_create_log_report_only() {
        let received_at = DateTime::parse_from_rfc3339("2026-10-06T12:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let fixed_trace_id: TraceId = "de4e189601a342c6bee991645300852e".parse().unwrap();

        let log = create_log_with_trace_id(
            Annotated::new(make_report(true)),
            received_at,
            Some(fixed_trace_id),
        )
        .unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }

    #[test]
    fn test_create_log_missing_body() {
        let received_at = Utc::now();

        let report = IntegrityReportRaw {
            ty: Annotated::new("integrity-violation".to_owned()),
            body: Annotated::empty(),
            ..Default::default()
        };

        assert!(create_log(Annotated::new(report), received_at).is_none());
    }

    #[test]
    fn test_create_log_wrong_type() {
        let received_at = Utc::now();

        let mut report = make_report(false);
        report.ty = Annotated::new("network-error".to_owned());

        assert!(create_log(Annotated::new(report), received_at).is_none());
    }
}
