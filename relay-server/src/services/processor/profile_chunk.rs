//! Profile chunks processor code.

use relay_dynamic_config::Feature;
use std::sync::Arc;

use crate::envelope::ItemType;
use crate::utils::{ItemAction};

use crate::services::processor::payload;
use crate::services::projects::project::ProjectInfo;
#[cfg(feature = "processing")]
use {
    crate::envelope::ContentType,
    crate::services::outcome::{DiscardReason, Outcome},
    crate::services::processor::ProfileChunkGroup,
    relay_config::Config,
    relay_dynamic_config::GlobalConfig,
};

/// Removes profile chunks from the envelope if the feature is not enabled.
pub fn filter<'a, G>(
    payload: impl Into<payload::NoEventRefMut<'a, G>>,
    project_info: Arc<ProjectInfo>,
) {
    let payload = payload.into();

    let continuous_profiling_enabled =
        if project_info.has_feature(Feature::ContinuousProfilingBetaIngest) {
            project_info.has_feature(Feature::ContinuousProfilingBeta)
        } else {
            project_info.has_feature(Feature::ContinuousProfiling)
        };

    payload
        .managed_envelope
        .retain_items(|item| match item.ty() {
            ItemType::ProfileChunk if !continuous_profiling_enabled => ItemAction::DropSilently,
            _ => ItemAction::Keep,
        });
}

/// Processes profile chunks.
#[cfg(feature = "processing")]
pub fn process<'a>(
    payload: impl Into<payload::NoEventRefMut<'a, ProfileChunkGroup>>,
    project_info: Arc<ProjectInfo>,
    global_config: &GlobalConfig,
    config: &Config,
) {
    let payload = payload.into();

    let client_ip = payload.managed_envelope.envelope().meta().client_addr();
    let filter_settings = &project_info.config.filter_settings;
    let continuous_profiling_enabled =
        if project_info.has_feature(Feature::ContinuousProfilingBetaIngest) {
            project_info.has_feature(Feature::ContinuousProfilingBeta)
        } else {
            project_info.has_feature(Feature::ContinuousProfiling)
        };
    payload
        .managed_envelope
        .retain_items(|item| match item.ty() {
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
