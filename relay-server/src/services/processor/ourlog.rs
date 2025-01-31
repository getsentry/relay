//! Log processing code.
use std::sync::Arc;

use crate::services::processor::LogGroup;
use relay_config::Config;
use relay_dynamic_config::Feature;

use crate::services::processor::should_filter;
use crate::services::projects::project::ProjectInfo;
use crate::utils::{ItemAction, TypedEnvelope};

#[cfg(feature = "processing")]
use {
    crate::envelope::ContentType,
    crate::envelope::{Item, ItemType},
    crate::services::outcome::{DiscardReason, Outcome},
    crate::services::processor::{EventProcessing, ProcessingError},
    crate::utils::{self},
    relay_dynamic_config::{GlobalConfig, ProjectConfig},
    relay_event_schema::processor::{process_value, ProcessingState},
    relay_event_schema::protocol::{Event, OurLog},
    relay_ourlogs::{breadcrumbs_to_ourlogs, OtelLog},
    relay_pii::PiiProcessor,
    relay_protocol::Annotated,
};

/// Removes logs from the envelope if the feature is not enabled.
pub fn filter(
    managed_envelope: &mut TypedEnvelope<LogGroup>,
    config: Arc<Config>,
    project_info: Arc<ProjectInfo>,
) {
    let logging_disabled = should_filter(&config, &project_info, Feature::OurLogsIngestion);
    managed_envelope.retain_items(|_| {
        if logging_disabled {
            ItemAction::DropSilently
        } else {
            ItemAction::Keep
        }
    });
}

/// Processes logs.
#[cfg(feature = "processing")]
pub fn process(managed_envelope: &mut TypedEnvelope<LogGroup>, project_info: Arc<ProjectInfo>) {
    managed_envelope.retain_items(|item| {
        let mut annotated_log = match item.ty() {
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

        if let Err(e) = scrub(&mut annotated_log, &project_info.config) {
            relay_log::error!("failed to scrub pii from log: {}", e);
            return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
        }

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

#[cfg(feature = "processing")]
fn scrub(
    annotated_log: &mut Annotated<OurLog>,
    project_config: &ProjectConfig,
) -> Result<(), ProcessingError> {
    if let Some(ref config) = project_config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(annotated_log, &mut processor, ProcessingState::root())?;
    }
    let pii_config = project_config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;
    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(annotated_log, &mut processor, ProcessingState::root())?;
    }

    Ok(())
}

/// Extract breadcrumbs from an event and convert them to logs.
#[cfg(feature = "processing")]
pub fn extract_from_event<Group: EventProcessing>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &Annotated<Event>,
    global_config: &GlobalConfig,
) {
    let Some(event) = event.value() else {
        return;
    };

    let convert_breadcrumbs_to_logs = utils::sample(
        global_config
            .options
            .ourlogs_breadcrumb_extraction_sample_rate
            .unwrap_or(0.0),
    );

    if convert_breadcrumbs_to_logs {
        relay_log::trace!("extracting breadcrumbs to logs");
        let ourlogs = breadcrumbs_to_ourlogs(
            event,
            global_config
                .options
                .ourlogs_breadcrumb_extraction_max_breadcrumbs_converted,
        );

        if let Some(ourlogs) = ourlogs {
            for ourlog in ourlogs {
                let mut log_item = Item::new(ItemType::Log);
                if let Ok(payload) = Annotated::new(ourlog).to_json() {
                    log_item.set_payload(ContentType::Json, payload);
                    relay_log::trace!("Adding log to envelope");
                    managed_envelope.envelope_mut().add_item(log_item);
                }
            }
        }
    }
}
