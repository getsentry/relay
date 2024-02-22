//! Unreal 4 processor related code.
//!
//! These functions are included only in the processing mode.

use relay_config::Config;

use crate::envelope::ItemType;
use crate::services::processor::{ErrorGroup, ProcessEnvelopeState, ProcessError, ProcessingError};
use crate::utils;

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
pub fn expand<'a>(
    mut state: ProcessEnvelopeState<'a, ErrorGroup>,
    config: &'_ Config,
) -> Result<ProcessEnvelopeState<'a, ErrorGroup>, ProcessError<'a, ErrorGroup>> {
    let envelope = &mut state.envelope_mut();

    if let Some(item) = envelope.take_item_by(|item| item.ty() == &ItemType::UnrealReport) {
        if let Err(err) = utils::expand_unreal_envelope(item, envelope, config) {
            return Err((state, ProcessingError::InvalidUnrealReport(err)));
        };
    }

    Ok(state)
}

/// Extracts event information from an unreal context.
///
/// If the event does not contain an unreal context, this function does not perform any action.
/// If there was no event payload prior to this function, it is created.
pub fn process(
    mut state: ProcessEnvelopeState<ErrorGroup>,
) -> Result<ProcessEnvelopeState<ErrorGroup>, ProcessError<ErrorGroup>> {
    if let Err(err) =
        utils::process_unreal_envelope(&mut state.event, state.managed_envelope.envelope_mut())
    {
        return Err((state, ProcessingError::InvalidUnrealReport(err)));
    }
    Ok(state)
}
