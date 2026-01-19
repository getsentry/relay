//! Contains helper function for COOP reports.

use chrono::{DateTime, Duration, Utc};
use relay_event_schema::protocol::{
    Attributes, CoopReportRaw, OurLog, OurLogLevel, Timestamp, TraceId,
};
use relay_protocol::Annotated;
use std::collections::HashMap;
use std::sync::LazyLock;

/// Mapping of COOP violation types to their human-readable descriptions.
static COOP_VIOLATIONS: &[(&str, &str)] = &[
    (
        "navigate-to-document",
        "Navigating to a document with a different COOP policy caused a browsing context group switch",
    ),
    (
        "navigate-from-document",
        "Navigating from a document with COOP policy caused a browsing context group switch",
    ),
    (
        "access-from-coop-page-to-opener",
        "Blocked access from COOP page to its opener window",
    ),
    (
        "access-from-coop-page-to-openee",
        "Blocked access from COOP page to a window it opened",
    ),
    (
        "access-from-coop-page-to-other",
        "Blocked access from COOP page to another document",
    ),
    (
        "access-to-coop-page-from-opener",
        "Blocked access to COOP page from its opener window",
    ),
    (
        "access-to-coop-page-from-openee",
        "Blocked access to COOP page from a window it opened",
    ),
    (
        "access-to-coop-page-from-other",
        "Blocked access to COOP page from another document",
    ),
];

/// Lazy-initialized HashMap for fast COOP violation type lookups.
static COOP_VIOLATIONS_MAP: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| COOP_VIOLATIONS.iter().copied().collect());

/// Gets the human-readable description for a COOP violation type.
fn get_coop_violation_message(violation_type: &str) -> String {
    COOP_VIOLATIONS_MAP
        .get(violation_type)
        .map(|&msg| msg.to_owned())
        .unwrap_or_else(|| format!("COOP violation: {}", violation_type))
}

/// Creates a [`OurLog`] from the provided [`CoopReportRaw`].
pub fn create_log(coop: Annotated<CoopReportRaw>, received_at: DateTime<Utc>) -> Option<OurLog> {
    create_log_with_trace_id(coop, received_at, None)
}

/// Creates a [`OurLog`] from the provided [`CoopReportRaw`] with an optional trace ID.
/// If trace_id is None, a random one will be generated.
pub fn create_log_with_trace_id(
    coop: Annotated<CoopReportRaw>,
    received_at: DateTime<Utc>,
    trace_id: Option<TraceId>,
) -> Option<OurLog> {
    let raw_report = coop.into_value()?;
    let body = raw_report.body.into_value()?;

    // Extract the violation type
    let violation_type = body.violation.as_str().unwrap_or("unknown");
    let message = get_coop_violation_message(violation_type);

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

    add_string_attribute!("sentry.origin", "auto.http.browser_report.coop");
    add_string_attribute!("browser.report.type", "coop");

    // Add document URL
    add_attribute!("url.full", raw_report.url);

    // Add COOP-specific attributes
    add_attribute!("coop.disposition", body.disposition.map_value(|d| d.to_string()));
    add_attribute!("coop.effective_policy", body.effective_policy);
    add_attribute!("coop.violation", body.violation);

    // Add URL attributes based on violation type
    add_attribute!("coop.previous_response_url", body.previous_response_url);
    add_attribute!("coop.next_response_url", body.next_response_url);
    add_attribute!("coop.opener_url", body.opener_url);
    add_attribute!("coop.openee_url", body.openee_url);
    add_attribute!("coop.other_document_url", body.other_document_url);
    add_attribute!("coop.initial_popup_url", body.initial_popup_url);

    // Add referrer if present
    add_attribute!("http.request.header.referer", body.referrer);

    // Add property if present (for access violations)
    add_attribute!("coop.property", body.property);

    // Add source location if present
    add_attribute!("coop.source_file", body.source_file);
    add_attribute!("coop.line_number", body.line_number);
    add_attribute!("coop.column_number", body.column_number);

    // Determine log level based on disposition
    let level = if let Some(disposition) = body.disposition.value() {
        match disposition.as_str() {
            "enforce" => OurLogLevel::Error,
            "reporting" => OurLogLevel::Warn,
            _ => OurLogLevel::Warn,
        }
    } else {
        OurLogLevel::Warn
    };

    Some(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(trace_id.unwrap_or_else(TraceId::random)),
        level: Annotated::new(level),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use relay_event_schema::protocol::CoopDisposition;
    use relay_protocol::Annotated;

    #[test]
    fn test_create_log_navigate_to() {
        let json = r#"{
            "age": 1000,
            "type": "coop",
            "url": "https://example.com/",
            "user_agent": "Mozilla/5.0",
            "body": {
                "disposition": "enforce",
                "effectivePolicy": "same-origin",
                "previousResponseURL": "https://previous.com/",
                "referrer": "https://referrer.com/",
                "violation": "navigate-to-document"
            }
        }"#;

        let report: Annotated<CoopReportRaw> =
            Annotated::from_json_bytes(json.as_bytes()).unwrap();
        let received_at = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let log = create_log(report, received_at).expect("Failed to create log");

        assert_eq!(
            log.body.value().unwrap(),
            "Navigating to a document with a different COOP policy caused a browsing context group switch"
        );
        assert_eq!(log.level.value().unwrap(), &OurLogLevel::Error);

        let attrs = log.attributes.value().unwrap();
        assert_eq!(
            attrs.get("sentry.origin").unwrap(),
            "auto.http.browser_report.coop"
        );
        assert_eq!(attrs.get("browser.report.type").unwrap(), "coop");
        assert_eq!(attrs.get("coop.disposition").unwrap(), "enforce");
        assert_eq!(attrs.get("coop.effective_policy").unwrap(), "same-origin");
        assert_eq!(attrs.get("coop.violation").unwrap(), "navigate-to-document");
    }

    #[test]
    fn test_create_log_access_violation() {
        let json = r#"{
            "age": 0,
            "type": "coop",
            "url": "https://example.com/page",
            "user_agent": "Mozilla/5.0",
            "body": {
                "disposition": "reporting",
                "effectivePolicy": "same-origin-allow-popups",
                "openerURL": "https://opener.com/",
                "referrer": "",
                "violation": "access-from-coop-page-to-opener",
                "property": "postMessage",
                "sourceFile": "https://example.com/script.js",
                "lineNumber": 42,
                "columnNumber": 15
            }
        }"#;

        let report: Annotated<CoopReportRaw> =
            Annotated::from_json_bytes(json.as_bytes()).unwrap();
        let received_at = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let log = create_log(report, received_at).expect("Failed to create log");

        assert_eq!(
            log.body.value().unwrap(),
            "Blocked access from COOP page to its opener window"
        );
        assert_eq!(log.level.value().unwrap(), &OurLogLevel::Warn);

        let attrs = log.attributes.value().unwrap();
        assert_eq!(attrs.get("coop.property").unwrap(), "postMessage");
        assert_eq!(attrs.get("coop.source_file").unwrap(), "https://example.com/script.js");
        assert_eq!(attrs.get("coop.line_number").unwrap(), &42);
        assert_eq!(attrs.get("coop.column_number").unwrap(), &15);
    }
}
