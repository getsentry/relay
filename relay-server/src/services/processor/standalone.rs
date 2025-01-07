use crate::envelope::ItemType;
use crate::services::processor::StandaloneGroup;
use crate::utils::TypedEnvelope;

/// Processes a standalone envelope by removing unnecessary items.
///
/// This function removes form data items from the envelope since they are not needed for
/// standalone processing.
pub fn process(managed_envelope: &mut TypedEnvelope<StandaloneGroup>) {
    managed_envelope
        .envelope_mut()
        .take_items_by(|i| *i.ty() == ItemType::FormData);
}
