//! Attachments processor code.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use relay_pii::{PiiAttachmentsProcessor, SelectorPathItem, SelectorSpec};
use relay_statsd::metric;

use crate::envelope::{AttachmentType, ContentType};
use crate::statsd::RelayTimers;

use crate::services::processor::payload;
use crate::services::projects::project::ProjectInfo;
use crate::utils::TypedEnvelope;
#[cfg(feature = "processing")]
use {
    crate::services::processor::{ErrorGroup, EventFullyNormalized},
    crate::utils,
    relay_event_schema::protocol::{Event, Metrics},
    relay_protocol::Annotated,
};

/// Adds processing placeholders for special attachments.
///
/// If special attachments are present in the envelope, this adds placeholder payloads to the
/// event. This indicates to the pipeline that the event needs special processing.
///
/// If the event payload was empty before, it is created.
#[cfg(feature = "processing")]
pub fn create_placeholders(
    mut payload: payload::WithEvent<ErrorGroup>,
    metrics: &mut Metrics,
) -> (payload::WithEvent<ErrorGroup>, Option<EventFullyNormalized>) {
    let envelope = payload.managed_envelope.envelope();
    let minidump_attachment =
        envelope.get_item_by(|item| item.attachment_type() == Some(&AttachmentType::Minidump));
    let apple_crash_report_attachment = envelope
        .get_item_by(|item| item.attachment_type() == Some(&AttachmentType::AppleCrashReport));

    if let Some(item) = minidump_attachment {
        let event = payload.event.get_or_insert_with(Event::default);
        metrics.bytes_ingested_event_minidump = Annotated::new(item.len() as u64);
        utils::process_minidump(event, &item.payload());
        return (payload, Some(EventFullyNormalized(false)));
    } else if let Some(item) = apple_crash_report_attachment {
        let event = payload.event.get_or_insert_with(Event::default);
        metrics.bytes_ingested_event_applecrashreport = Annotated::new(item.len() as u64);
        utils::process_apple_crash_report(event, &item.payload());
        return (payload, Some(EventFullyNormalized(false)));
    }

    (payload, None)
}

/// Apply data privacy rules to attachments in the envelope.
///
/// This only applies the new PII rules that explicitly select `ValueType::Binary` or one of the
/// attachment types. When special attachments are detected, these are scrubbed with custom
/// logic; otherwise the entire attachment is treated as a single binary blob.
pub fn scrub<'a, G: 'a>(
    payload: impl Into<payload::AnyRefMut<'a, G>>,
    project_info: Arc<ProjectInfo>,
) {
    let mut payload = payload.into();

    let envelope = payload.managed_envelope_mut().envelope_mut();
    if let Some(ref config) = project_info.config.pii_config {
        let minidump = envelope
            .get_item_by_mut(|item| item.attachment_type() == Some(&AttachmentType::Minidump));

        if let Some(item) = minidump {
            scrub_minidump(item, config);
        } else if has_simple_attachment_selector(config) {
            // We temporarily only scrub attachments to projects that have at least one simple attachment rule,
            // such as `$attachments.'foo.txt'`.
            // After we have assessed the impact on performance we can relax this condition.
            for item in envelope
                .items_mut()
                .filter(|item| item.attachment_type().is_some())
            {
                scrub_attachment(item, config);
            }
        }
    }
}

fn scrub_minidump(item: &mut crate::envelope::Item, config: &relay_pii::PiiConfig) {
    debug_assert_eq!(item.attachment_type(), Some(&AttachmentType::Minidump));
    let filename = item.filename().unwrap_or_default();
    let mut payload = item.payload().to_vec();

    let processor = PiiAttachmentsProcessor::new(config.compiled());

    // Minidump scrubbing can fail if the minidump cannot be parsed. In this case, we
    // must be conservative and treat it as a plain attachment. Under extreme
    // conditions, this could destroy stack memory.
    let start = Instant::now();
    match processor.scrub_minidump(filename, &mut payload) {
        Ok(modified) => {
            metric!(
                timer(RelayTimers::MinidumpScrubbing) = start.elapsed(),
                status = if modified { "ok" } else { "n/a" },
            );
        }
        Err(scrub_error) => {
            metric!(
                timer(RelayTimers::MinidumpScrubbing) = start.elapsed(),
                status = "error"
            );
            relay_log::warn!(
                error = &scrub_error as &dyn Error,
                "failed to scrub minidump",
            );
            metric!(
                timer(RelayTimers::AttachmentScrubbing),
                attachment_type = "minidump",
                {
                    processor.scrub_attachment(filename, &mut payload);
                }
            )
        }
    }

    let content_type = item
        .content_type()
        .unwrap_or(&ContentType::Minidump)
        .clone();

    item.set_payload(content_type, payload);
}

fn has_simple_attachment_selector(config: &relay_pii::PiiConfig) -> bool {
    for application in &config.applications {
        if let SelectorSpec::Path(vec) = &application.0 {
            let Some([a, b]) = vec.get(0..2) else {
                continue;
            };
            if matches!(
                a,
                SelectorPathItem::Type(relay_event_schema::processor::ValueType::Attachments)
            ) && matches!(b, SelectorPathItem::Key(_))
            {
                return true;
            }
        }
    }
    false
}

fn scrub_attachment(item: &mut crate::envelope::Item, config: &relay_pii::PiiConfig) {
    let filename = item.filename().unwrap_or_default();
    let mut payload = item.payload().to_vec();

    let processor = PiiAttachmentsProcessor::new(config.compiled());
    let attachment_type_tag = match item.attachment_type() {
        Some(t) => t.to_string(),
        None => "".to_owned(),
    };
    metric!(
        timer(RelayTimers::AttachmentScrubbing),
        attachment_type = &attachment_type_tag,
        {
            processor.scrub_attachment(filename, &mut payload);
        }
    );

    item.set_payload_without_content_type(payload);
}

#[cfg(test)]
mod tests {
    use relay_pii::PiiConfig;

    use super::*;

    #[test]
    fn matches_attachment_selector() {
        let config = r#"{
            "rules": {"0": {"type": "ip", "redaction": {"method": "remove"}}},
            "applications": {"$attachments.'foo.txt'": ["0"]}
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(has_simple_attachment_selector(&config));
    }

    #[test]
    fn does_not_match_wildcard() {
        let config = r#"{
            "rules": {},
            "applications": {"$attachments.**":["0"]}
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(!has_simple_attachment_selector(&config));
    }

    #[test]
    fn does_not_match_empty() {
        let config = r#"{
            "rules": {},
            "applications": {}
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(!has_simple_attachment_selector(&config));
    }

    #[test]
    fn does_not_match_something_else() {
        let config = r#"{
            "rules": {},
            "applications": {
                "**": ["0"]
            }
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(!has_simple_attachment_selector(&config));
    }
}
