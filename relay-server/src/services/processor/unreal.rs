//! Unreal 4 processor related code.
//!
//! These functions are included only in the processing mode.

use crate::envelope::ItemType;
use crate::managed::TypedEnvelope;
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::utils;

use relay_config::Config;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

/// Expands Unreal 4 items inside an envelope.
///
/// If the envelope does NOT contain an `UnrealReport` item, it doesn't do anything. If the
/// envelope contains an `UnrealReport` item, it removes it from the envelope and inserts new
/// items for each of its contents.
///
/// The envelope may be dropped if it exceeds size limits after decompression. Particularly,
/// this includes cases where a single attachment file exceeds the maximum file size. This is in
/// line with the behavior of the envelope endpoint.
///
/// After this, [`crate::services::processor::EnvelopeProcessorService`] should be able to process the envelope the same
/// way it processes any other envelopes.
pub fn expand(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    config: &Config,
) -> Result<(), ProcessingError> {
    let envelope = &mut managed_envelope.envelope_mut();

    if let Some(item) = envelope.take_item_by(|item| *item.ty() == ItemType::UnrealReport) {
        utils::expand_unreal_envelope(item, envelope, config)?;
    }

    Ok(())
}

/// Extracts event information from an unreal context.
///
/// If the event does not contain an unreal context, this function does not perform any action.
/// If there was no event payload prior to this function, it is created.
pub fn process(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    event: &mut Annotated<Event>,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    if utils::process_unreal_envelope(event, managed_envelope.envelope_mut())
        .map_err(ProcessingError::InvalidUnrealReport)?
    {
        return Ok(Some(EventFullyNormalized(false)));
    }

    Ok(None)
}
