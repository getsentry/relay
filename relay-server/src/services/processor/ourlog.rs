//! Log processing code.

use std::sync::Arc;

use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_event_schema::protocol::OurLog;

use crate::envelope::ItemType;
use crate::services::projects::project::ProjectInfo;
use crate::utils::{ItemAction, TypedEnvelope};

use relay_ourlogs::OtelLog;
use relay_protocol::Annotated;

#[cfg(feature = "processing")]
use {
    crate::envelope::ContentType,
    crate::services::outcome::{DiscardReason, Outcome},
    crate::services::processor::LogGroup,
};

/// Removes logs from the envelope if the feature is not enabled.
pub fn filter<Group>(managed_envelope: &mut TypedEnvelope<Group>) {
    // All log types are currently kept
    managed_envelope.retain_items(|_| ItemAction::Keep);
}

/// Processes logs.
#[cfg(feature = "processing")]
pub fn process(
    managed_envelope: &mut TypedEnvelope<LogGroup>,
    _project_info: Arc<ProjectInfo>,
    _global_config: &GlobalConfig,
    _config: &Config,
) {
    use crate::envelope::Item;

    managed_envelope.retain_items(|item| {
        let annotated_log = match item.ty() {
            ItemType::OtelLog => match serde_json::from_slice::<OtelLog>(&item.payload()) {
                Ok(otel_log) => Annotated::new(relay_ourlogs::otel_to_sentry_log(otel_log)),
                Err(err) => {
                    relay_log::debug!("failed to parse OTel Log: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidLog));
                }
            },
            ItemType::Log => match Annotated::<OurLog>::from_json_bytes(&item.payload()) {
                Ok(our_log) => our_log,
                Err(err) => {
                    relay_log::debug!("failed to parse Sentry Log: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidLog));
                }
            },

            _ => return ItemAction::Keep,
        };

        let mut new_item = Item::new(ItemType::Log);
        let payload = match annotated_log.to_json() {
            Ok(payload) => payload,
            Err(err) => {
                relay_log::debug!("failed to serialize log: {}", err);
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            }
        };
        new_item.set_payload(ContentType::Json, payload);

        *item = new_item;

        ItemAction::Keep
    });
}
