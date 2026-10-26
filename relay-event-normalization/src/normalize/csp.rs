//! Contains helper functions to convert CSP reports into logs.

use chrono::{DateTime, Duration, Utc};
use relay_conventions::attributes::{
    BROWSER__REPORT__TYPE, CODE__FILE__PATH, CODE__LINE__NUMBER, HTTP__RESPONSE__STATUS_CODE,
    SENTRY__ORIGIN, URL__DOMAIN, URL__FULL,
};
use relay_event_schema::protocol::{
    Attributes, CspViolationReport, OurLog, OurLogLevel, Timestamp, TraceId,
};
use relay_protocol::Annotated;
use url::Url;

/// Creates an [`OurLog`] from the provided [`CspViolationReport`].
pub fn create_log(report: CspViolationReport, received_at: DateTime<Utc>) -> OurLog {
    create_log_with_trace_id(report, received_at, None)
}

/// Creates an [`OurLog`] from the provided [`CspViolationReport`] with an optional trace ID.
///
/// If `trace_id` is `None`, a random one will be generated.
pub fn create_log_with_trace_id(
    report: CspViolationReport,
    received_at: DateTime<Utc>,
    trace_id: Option<TraceId>,
) -> OurLog {
    let CspViolationReport {
        message,
        effective_directive,
        violated_directive,
        blocked_uri,
        blocked_host,
        document_uri,
        referrer,
        status_code,
        source_file,
        line_number,
        column_number,
        script_sample,
        disposition,
        age,
    } = report;

    let timestamp = age
        .and_then(|age| i64::try_from(age).ok())
        .and_then(|age| received_at.checked_sub_signed(Duration::milliseconds(age)))
        .unwrap_or(received_at);

    let document_domain = document_uri
        .as_deref()
        .and_then(|uri| Url::parse(uri).ok())
        .and_then(|url| url.host_str().map(str::to_owned));

    let mut attributes = Attributes::default();

    let strings = [
        (
            SENTRY__ORIGIN,
            Some("auto.http.browser_report.csp".to_owned()),
        ),
        (BROWSER__REPORT__TYPE, Some("csp-violation".to_owned())),
        (URL__DOMAIN, document_domain),
        (URL__FULL, document_uri),
        ("http.request.header.referer", referrer),
        (CODE__FILE__PATH, source_file),
        // TODO: The `csp.*` attributes must be defined in `sentry-conventions` before this ships.
        ("csp.effective_directive", Some(effective_directive)),
        ("csp.violated_directive", Some(violated_directive)),
        ("csp.blocked_uri", Some(blocked_uri)),
        ("csp.blocked_host", blocked_host),
        ("csp.script_sample", script_sample),
        ("csp.disposition", disposition),
    ];

    for (key, value) in strings {
        if let Some(value) = value.filter(|v| !v.is_empty()) {
            attributes.insert(key, value);
        }
    }

    let integers = [
        (HTTP__RESPONSE__STATUS_CODE, status_code),
        (CODE__LINE__NUMBER, line_number),
        ("csp.column_number", column_number),
    ];

    for (key, value) in integers {
        if let Some(value) = value.and_then(|v| i64::try_from(v).ok()) {
            attributes.insert(key, value);
        }
    }

    OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(trace_id.unwrap_or_else(TraceId::random)),
        level: Annotated::new(OurLogLevel::Warn),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    fn received_at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn trace_id() -> TraceId {
        "de4e189601a342c6bee991645300852e".parse().unwrap()
    }

    #[test]
    fn test_create_log_legacy_report() {
        let json = r#"{
            "csp-report": {
                "document-uri": "https://example.com/foo/bar",
                "referrer": "https://www.google.com/",
                "violated-directive": "script-src 'self'",
                "effective-directive": "script-src",
                "blocked-uri": "https://evil.com/script.js",
                "status-code": 200,
                "source-file": "https://example.com/foo/bar",
                "line-number": 12,
                "column-number": 34,
                "disposition": "enforce"
            }
        }"#;

        let report = CspViolationReport::from_json(json.as_bytes())
            .unwrap()
            .unwrap();
        let log = create_log_with_trace_id(report, received_at(), Some(trace_id()));

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }

    #[test]
    fn test_create_log_reporting_api_uses_age() {
        let json = r#"{
            "age": 5000,
            "body": {
                "blockedURL": "inline",
                "documentURL": "https://example.com/page",
                "effectiveDirective": "style-src-attr",
                "disposition": "report"
            },
            "type": "csp-violation",
            "url": "https://example.com/page"
        }"#;

        let report = CspViolationReport::from_json(json.as_bytes())
            .unwrap()
            .unwrap();
        let log = create_log_with_trace_id(report, received_at(), Some(trace_id()));

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }
}
