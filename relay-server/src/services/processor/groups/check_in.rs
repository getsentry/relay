use std::sync::Arc;

use relay_base_schema::project::ProjectId;
use relay_quotas::RateLimits;

use crate::services::processor::groups::payload::BasePayload;
use crate::services::processor::groups::{Group, GroupParams, ProcessGroup};
use crate::services::processor::GroupTypeError;
use crate::services::processor::{
    InnerProcessor, ProcessingError, ProcessingExtractedMetrics, ProcessingGroup,
};
use crate::services::projects::project::ProjectInfo;
use crate::{group, if_processing};
#[cfg(feature = "processing")]
use {
    crate::envelope::ContentType, crate::envelope::ItemType,
    crate::services::processor::enforce_quotas, crate::services::processor::groups::GroupPayload,
    crate::utils::ItemAction, std::error::Error,
};

group!(CheckIn, CheckIn);

pub struct ProcessCheckIn<'a> {
    #[allow(dead_code)]
    payload: BasePayload<'a, CheckIn>,
    #[allow(dead_code)]
    processor: Arc<InnerProcessor>,
    #[allow(dead_code)]
    rate_limits: Arc<RateLimits>,
    #[allow(dead_code)]
    project_info: Arc<ProjectInfo>,
    #[allow(dead_code)]
    project_id: ProjectId,
}

impl ProcessCheckIn<'_> {
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

impl<'a> ProcessGroup<'a, CheckIn> for ProcessCheckIn<'a> {
    type Payload = BasePayload<'a, CheckIn>;

    fn create(params: GroupParams<'a, CheckIn>) -> Self {
        Self {
            payload: Self::Payload::no_event(params.managed_envelope),
            processor: params.processor,
            rate_limits: params.rate_limits,
            project_info: params.project_info,
            project_id: params.project_id,
        }
    }

    fn process(
        #[allow(unused_mut)] mut self,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        #[allow(unused_mut)]
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        if_processing!(self.processor.config, {
            enforce_quotas(
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
