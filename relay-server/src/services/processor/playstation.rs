//! Playstation related code.
//!
//! These functions are included only in the processing mode.

use crate::envelope::{AttachmentType, ItemType};
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::utils::TypedEnvelope;
use relay_config::Config;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

pub fn expand(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    _config: &Config,
) -> Result<(), ProcessingError> {
    let envelope = &mut managed_envelope.envelope_mut();

    if let Some(item) = envelope.take_item_by(|item| item.ty() == &ItemType::Attachment) {
        if let Some(&AttachmentType::Prosperodump) = item.attachment_type() {
            // TODO: Do some work here
        }
    }

    Ok(())
}

// FIXME: Decide on weather we also want to keep the double function here to do the extraction work on the custom tags.
pub fn process(
    _managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    _event: &mut Annotated<Event>,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    Ok(None)
}
