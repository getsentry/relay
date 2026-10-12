use crate::envelope::{AttachmentType, Envelope, ItemType};
use crate::managed::Managed;
use crate::services::outcome::{DiscardReason, Outcome};

/// Validate transport before routing separates events from standalone attachments.
///
/// Invalid flame graphs must not discard their associated event or unrelated items.
pub fn validate_flamegraph_envelope(envelope: &mut Managed<Box<Envelope>>) {
    let has_event = envelope.event_id().is_some()
        && envelope
            .items()
            .any(|item| matches!(item.ty(), ItemType::Event | ItemType::Transaction));
    envelope.modify(|envelope, records| {
        envelope.retain_items(|item| {
            if item.attachment_type() == Some(AttachmentType::Flamegraph)
                && (!has_event || !item.has_valid_flamegraph_headers())
            {
                records.reject_err(Outcome::Invalid(DiscardReason::InvalidFlamegraph), &*item);
                return false;
            }
            true
        });
    });
}
