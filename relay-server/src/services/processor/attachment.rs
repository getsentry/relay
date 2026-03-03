//! Attachments processor code.
#[cfg(feature = "processing")]
use {
    crate::{
        envelope::AttachmentType,
        managed::TypedEnvelope,
        services::processor::{ErrorGroup, EventFullyNormalized},
        utils,
    },
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
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    event: &mut Annotated<Event>,
    metrics: &mut Metrics,
) -> Option<EventFullyNormalized> {
    let envelope = managed_envelope.envelope();
    let minidump_attachment =
        envelope.get_item_by(|item| item.attachment_type() == Some(AttachmentType::Minidump));
    let apple_crash_report_attachment = envelope
        .get_item_by(|item| item.attachment_type() == Some(AttachmentType::AppleCrashReport));

    if let Some(item) = minidump_attachment {
        let event = event.get_or_insert_with(Event::default);
        metrics.bytes_ingested_event_minidump = Annotated::new(item.len() as u64);
        utils::process_minidump(event, &item.payload());
        return Some(EventFullyNormalized(false));
    } else if let Some(item) = apple_crash_report_attachment {
        let event = event.get_or_insert_with(Event::default);
        metrics.bytes_ingested_event_applecrashreport = Annotated::new(item.len() as u64);
        utils::process_apple_crash_report(event, &item.payload());
        return Some(EventFullyNormalized(false));
    }

    None
}
