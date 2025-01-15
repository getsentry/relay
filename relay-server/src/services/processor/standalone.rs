use crate::envelope::ItemType;
use crate::services::processor::{payload, StandaloneGroup};
use crate::utils::{ItemAction};

/// Processes a standalone envelope by removing unnecessary items.
///
/// This function removes form data items from the envelope since they are not allowed in
/// standalone processing.
pub fn process<'a>(payload: impl Into<payload::NoEventRefMut<'a, StandaloneGroup>>) {
    let payload = payload.into();

    payload.managed_envelope.retain_items(|i| match i.ty() {
        ItemType::FormData => ItemAction::DropSilently,
        _ => ItemAction::Keep,
    });
}
