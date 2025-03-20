//! Profile chunks processor code.

use relay_dynamic_config::Feature;
use std::sync::Arc;

use crate::envelope::ItemType;
use crate::utils::{ItemAction, TypedEnvelope};

use crate::services::projects::project::ProjectInfo;
#[cfg(feature = "processing")]
use {
    crate::envelope::ContentType,
    crate::services::outcome::{DiscardReason, Outcome},
    crate::services::processor::ProfileChunkGroup,
    relay_config::Config,
    relay_dynamic_config::GlobalConfig,
    relay_profiling::ProfileError,
};

/// Removes profile chunks from the envelope if the feature is not enabled.
pub fn filter<Group>(managed_envelope: &mut TypedEnvelope<Group>, project_info: Arc<ProjectInfo>) {
    let continuous_profiling_enabled =
        if project_info.has_feature(Feature::ContinuousProfilingBetaIngest) {
            project_info.has_feature(Feature::ContinuousProfilingBeta)
        } else {
            project_info.has_feature(Feature::ContinuousProfiling)
        };
    managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk if !continuous_profiling_enabled => ItemAction::DropSilently,
        _ => ItemAction::Keep,
    });
}

/// Processes profile chunks.
#[cfg(feature = "processing")]
pub fn process(
    managed_envelope: &mut TypedEnvelope<ProfileChunkGroup>,
    project_info: Arc<ProjectInfo>,
    global_config: &GlobalConfig,
    config: &Config,
) {
    let client_ip = managed_envelope.envelope().meta().client_addr();
    let filter_settings = &project_info.config.filter_settings;

    let continuous_profiling_enabled =
        if project_info.has_feature(Feature::ContinuousProfilingBetaIngest) {
            project_info.has_feature(Feature::ContinuousProfilingBeta)
        } else {
            project_info.has_feature(Feature::ContinuousProfiling)
        };

    managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk => {
            if !continuous_profiling_enabled {
                return ItemAction::DropSilently;
            }

            let chunk = match relay_profiling::ProfileChunk::new(item.payload()) {
                Ok(chunk) => chunk,
                Err(err) => return error_to_action(err),
            };
            // Important: set the profile type to get outcomes in the correct category.
            item.set_profile_type(chunk.profile_type());

            if let Err(err) = chunk.filter(client_ip, filter_settings, global_config) {
                return error_to_action(err);
            }

            let payload = match chunk.expand() {
                Ok(expanded) => expanded,
                Err(err) => return error_to_action(err),
            };

            if payload.len() > config.max_profile_size() {
                return error_to_action(relay_profiling::ProfileError::ExceedSizeLimit);
            }

            item.set_payload(ContentType::Json, payload);
            ItemAction::Keep
        }
        _ => ItemAction::Keep,
    });
}

#[cfg(feature = "processing")]
fn error_to_action(err: ProfileError) -> ItemAction {
    match err {
        ProfileError::Filtered(filter_stat_key) => {
            ItemAction::Drop(Outcome::Filtered(filter_stat_key))
        }
        err => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(err),
        ))),
    }
}
