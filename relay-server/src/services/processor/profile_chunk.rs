//! Profile chunks processor code.
use relay_dynamic_config::Feature;

use crate::envelope::ItemType;
use crate::services::processor::ProcessEnvelopeState;
use crate::utils::ItemAction;

#[cfg(feature = "processing")]
use {
    crate::envelope::ContentType,
    crate::services::outcome::{DiscardReason, Outcome},
    crate::services::processor::ProfileChunkGroup,
    relay_config::Config,
};

/// Removes profile chunks from the envelope if the feature is not enabled.
pub fn filter<G>(state: &mut ProcessEnvelopeState<G>) {
    let continuous_profiling_enabled = state
        .project_state
        .has_feature(Feature::ContinuousProfiling);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk if !continuous_profiling_enabled => ItemAction::DropSilently,
        _ => ItemAction::Keep,
    });
}

/// Processes profile chunks.
#[cfg(feature = "processing")]
pub fn process(state: &mut ProcessEnvelopeState<ProfileChunkGroup>, config: &Config) {
    let continuous_profiling_enabled = state
        .project_state
        .has_feature(Feature::ContinuousProfiling);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk => {
            if !continuous_profiling_enabled {
                return ItemAction::DropSilently;
            }
            match relay_profiling::expand_profile_chunk(&item.payload()) {
                Ok(payload) => {
                    if payload.len() <= config.max_profile_size() {
                        item.set_payload(ContentType::Json, payload);
                        ItemAction::Keep
                    } else {
                        ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                            relay_profiling::discard_reason(
                                relay_profiling::ProfileError::ExceedSizeLimit,
                            ),
                        )))
                    }
                }
                Err(err) => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(err),
                ))),
            }
        }
        _ => ItemAction::Keep,
    });
}
