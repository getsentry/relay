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
    relay_dynamic_config::GlobalConfig,
};

/// Removes profile chunks from the envelope if the feature is not enabled.
pub fn filter<G>(state: &mut ProcessEnvelopeState<G>) {
    let continuous_profiling_enabled = state.project_info.has_feature(Feature::ContinuousProfiling)
        && state
            .project_info
            .has_feature(Feature::ContinuousProfilingBeta);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk if !continuous_profiling_enabled => ItemAction::DropSilently,
        _ => ItemAction::Keep,
    });
}

/// Processes profile chunks.
#[cfg(feature = "processing")]
pub fn process(
    state: &mut ProcessEnvelopeState<ProfileChunkGroup>,
    global_config: &GlobalConfig,
    config: &Config,
) {
    let client_ip = state.managed_envelope.envelope().meta().client_addr();
    let filter_settings = &state.project_info.config.filter_settings;

    let continuous_profiling_enabled = state.project_info.has_feature(Feature::ContinuousProfiling)
        && state
            .project_info
            .has_feature(Feature::ContinuousProfilingBeta);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk => {
            if !continuous_profiling_enabled {
                return ItemAction::DropSilently;
            }

            match relay_profiling::expand_profile_chunk(
                &item.payload(),
                client_ip,
                filter_settings,
                global_config,
            ) {
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
                Err(relay_profiling::ProfileError::Filtered(filter_stat_key)) => {
                    ItemAction::Drop(Outcome::Filtered(filter_stat_key))
                }
                Err(err) => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(err),
                ))),
            }
        }
        _ => ItemAction::Keep,
    });
}
