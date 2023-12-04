use std::error::Error;
use std::time::Instant;

use relay_pii::PiiAttachmentsProcessor;
use relay_statsd::metric;

use crate::actors::processor::ProcessEnvelopeState;
use crate::envelope::{AttachmentType, ContentType};
use crate::statsd::RelayTimers;

#[cfg(feature = "processing")]
use {crate::utils, relay_event_schema::protocol::Event, relay_protocol::Annotated};

/// Adds processing placeholders for special attachments.
///
/// If special attachments are present in the envelope, this adds placeholder payloads to the
/// event. This indicates to the pipeline that the event needs special processing.
///
/// If the event payload was empty before, it is created.
#[cfg(feature = "processing")]
pub fn create_placeholders(state: &mut ProcessEnvelopeState) {
    let envelope = state.managed_envelope.envelope();
    let minidump_attachment =
        envelope.get_item_by(|item| item.attachment_type() == Some(&AttachmentType::Minidump));
    let apple_crash_report_attachment = envelope
        .get_item_by(|item| item.attachment_type() == Some(&AttachmentType::AppleCrashReport));

    if let Some(item) = minidump_attachment {
        let event = state.event.get_or_insert_with(Event::default);
        state.metrics.bytes_ingested_event_minidump = Annotated::new(item.len() as u64);
        utils::process_minidump(event, &item.payload());
    } else if let Some(item) = apple_crash_report_attachment {
        let event = state.event.get_or_insert_with(Event::default);
        state.metrics.bytes_ingested_event_applecrashreport = Annotated::new(item.len() as u64);
        utils::process_apple_crash_report(event, &item.payload());
    }
}

/// Apply data privacy rules to attachments in the envelope.
///
/// This only applies the new PII rules that explicitly select `ValueType::Binary` or one of the
/// attachment types. When special attachments are detected, these are scrubbed with custom
/// logic; otherwise the entire attachment is treated as a single binary blob.
pub fn scrub(state: &mut ProcessEnvelopeState) {
    let envelope = state.managed_envelope.envelope_mut();
    if let Some(ref config) = state.project_state.config.pii_config {
        let minidump = envelope
            .get_item_by_mut(|item| item.attachment_type() == Some(&AttachmentType::Minidump));

        if let Some(item) = minidump {
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
                    metric!(timer(RelayTimers::AttachmentScrubbing), {
                        processor.scrub_attachment(filename, &mut payload);
                    })
                }
            }

            let content_type = item
                .content_type()
                .unwrap_or(&ContentType::Minidump)
                .clone();

            item.set_payload(content_type, payload);
        }
    }
}
