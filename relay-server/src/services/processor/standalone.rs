use crate::envelope::ItemType;
use crate::services::processor::StandaloneGroup;
use crate::utils::{ItemAction, TypedEnvelope};

/// Processes a standalone envelope by removing unnecessary items.
///
/// This function removes form data items from the envelope since they are not allowed in
/// standalone processing.
pub fn process(managed_envelope: &mut TypedEnvelope<StandaloneGroup>) {
    managed_envelope.retain_items(|i| match i.ty() {
        ItemType::FormData => ItemAction::DropSilently,
        _ => ItemAction::Keep,
    });
}
