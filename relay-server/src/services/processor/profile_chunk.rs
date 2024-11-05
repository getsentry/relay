//! Profile chunks processor code.
use chrono::{DateTime, TimeZone, Utc};
use once_cell::sync::Lazy;
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

static START: Lazy<DateTime<Utc>> =
    Lazy::new(|| Utc.with_ymd_and_hms(2024, 11, 14, 0, 0, 0).unwrap());
static END: Lazy<DateTime<Utc>> =
    Lazy::new(|| Utc.with_ymd_and_hms(2024, 11, 19, 0, 0, 0).unwrap());

fn within_date_range() -> bool {
    let now = Utc::now();
    now >= *START && now < *END
}

fn allow_ingest<G>(state: &ProcessEnvelopeState<G>) -> bool {
    if within_date_range() {
        return state
            .project_info
            .has_feature(Feature::ContinuousProfilingBetaOrg);
    }
    true
}

/// Removes profile chunks from the envelope if the feature is not enabled.
pub fn filter<G>(state: &mut ProcessEnvelopeState<G>) {
    let continuous_profiling_enabled = state.project_info.has_feature(Feature::ContinuousProfiling);
    let allowed_ingest = allow_ingest(state);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk if !continuous_profiling_enabled || !allowed_ingest => {
            ItemAction::DropSilently
        }
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

    let continuous_profiling_enabled = state.project_info.has_feature(Feature::ContinuousProfiling);
    let allowed_ingest = allow_ingest(state);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ProfileChunk => {
            if !continuous_profiling_enabled || !allowed_ingest {
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
