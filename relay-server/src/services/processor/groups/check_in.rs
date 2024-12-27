use crate::envelope::{ContentType, ItemType};
use crate::if_processing;
#[cfg(feature = "processing")]
use crate::services::processor::enforce_quotas;
use crate::services::processor::groups::payload::DefaultPayload;
use crate::services::processor::groups::{Group, GroupParams, GroupPayload};
use crate::services::processor::{InnerProcessor, ProcessingError, ProcessingExtractedMetrics};
use crate::services::projects::project::ProjectInfo;
use crate::utils::ItemAction;
use relay_base_schema::project::ProjectId;
use relay_quotas::RateLimits;
use std::error::Error;
use std::sync::Arc;

pub struct CheckInGroup<'a> {
    payload: DefaultPayload<'a>,
    processor: Arc<InnerProcessor>,
    rate_limits: Arc<RateLimits>,
    project_info: Arc<ProjectInfo>,
    project_id: ProjectId,
}

impl<'a> CheckInGroup<'a> {
    /// Normalize monitor check-ins and remove invalid ones.
    #[cfg(feature = "processing")]
    fn normalize_check_ins(&mut self) {
        self.payload.managed_envelope_mut().retain_items(|item| {
            if item.ty() != &ItemType::CheckIn {
                return ItemAction::Keep;
            }

            match relay_monitors::process_check_in(&item.payload(), self.project_id) {
                Ok(result) => {
                    item.set_routing_hint(result.routing_hint);
                    item.set_payload(ContentType::Json, result.payload);
                    ItemAction::Keep
                }
                Err(error) => {
                    // TODO: Track an outcome.
                    relay_log::debug!(
                        error = &error as &dyn Error,
                        "dropped invalid monitor check-in"
                    );
                    ItemAction::DropSilently
                }
            }
        })
    }
}

impl<'a> Group<'a> for CheckInGroup<'a> {
    type Payload = DefaultPayload<'a>;

    fn create(params: GroupParams<'a>) -> Self {
        Self {
            payload: Self::Payload::no_event(params.managed_envelope),
            processor: params.processor,
            rate_limits: params.rate_limits,
            project_info: params.project_info,
            project_id: params.project_id,
        }
    }

    fn process(mut self) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        #[allow(unused_mut)]
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        if_processing!(self.processor.config, {
            enforce_quotas::<Self>(
                &mut self.payload,
                &mut extracted_metrics,
                self.processor.global_config.current().as_ref(),
                self.processor.rate_limiter.as_ref(),
                self.rate_limits.as_ref(),
                &self.processor.project_cache,
                self.project_info.as_ref(),
            )?;
            self.normalize_check_ins();
        });

        Ok(Some(extracted_metrics))
    }
}
