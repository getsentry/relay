//! Contains helper function for Integrity reports.

use relay_browser_reporting::integrity::Body;
use relay_conventions::attributes::{
    INTEGRITY__BLOCKED_URL, INTEGRITY__DESTINATION, INTEGRITY__DOCUMENT_URL, INTEGRITY__REPORT_ONLY,
};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel};
use relay_protocol::Annotated;

/// Integrity-specific human-readable text displayed
fn create_message(body: &Body) -> String {
    let Body {
        document_url: _,
        blocked_url,
        destination,
        report_only,
    } = body;

    if *report_only {
        format!(
            "Integrity policy would block {destination} from '{blocked_url}', but does not due to report only configuration"
        )
    } else {
        format!("Integrity policy blocked {destination} from '{blocked_url}'")
    }
}

/// Creates a log from the provided NEL [`Body`].
pub fn create_log_from_integrity_body(body: Body, mut attributes: Attributes) -> OurLog {
    let message = create_message(&body);

    add_attribute!(attributes, INTEGRITY__DOCUMENT_URL, body.document_url);
    add_attribute!(attributes, INTEGRITY__BLOCKED_URL, body.blocked_url);
    add_attribute!(attributes, INTEGRITY__DESTINATION, body.destination);
    add_attribute!(attributes, INTEGRITY__REPORT_ONLY, body.report_only);

    OurLog {
        level: Annotated::new(OurLogLevel::Warn),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    }
}
