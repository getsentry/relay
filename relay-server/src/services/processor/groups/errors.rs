use crate::if_processing;
use crate::services::processor::groups::{ProcGroup, ProcGroupParams, ProcGroupResult};
use crate::services::processor::{
    attachment, dynamic_sampling, event, report, unreal, ErrorGroup, InnerProcessor,
    ProcessingError,
};
use crate::utils::TypedEnvelope;
use std::sync::Arc;

pub struct ErrorsProcGroup {
    inner: Arc<InnerProcessor>,
    managed_envelope: TypedEnvelope<ErrorGroup>,
}

impl ProcGroup<ErrorGroup> for ErrorsProcGroup {
    fn create(inner: Arc<InnerProcessor>, params: ProcGroupParams<ErrorGroup>) -> Self {
        Self {
            inner,
            managed_envelope: params.managed_envelope,
        }
    }

    fn process(mut self) -> Result<ProcGroupResult, ProcessingError> {
        // Events can also contain user reports.
        report::process_user_reports(&mut self.managed_envelope);

        if_processing!(self.inner.config, {
            unreal::expand(state, &self.inner.config)?;
        });

        event::extract(state, &self.inner.config)?;

        if_processing!(self.inner.config, {
            unreal::process(state)?;
            attachment::create_placeholders(state);
        });

        event::finalize(state, &self.inner.config)?;
        self.normalize_event(state)?;
        let filter_run = event::filter(state, &self.inner.global_config.current())?;

        if self.inner.config.processing_enabled() || matches!(filter_run, FiltersStatus::Ok) {
            dynamic_sampling::tag_error_with_sampling_decision(state, &self.inner.config);
        }

        if_processing!(self.inner.config, {
            self.enforce_quotas(state)?;
        });

        if state.has_event() {
            event::scrub(state)?;
            event::serialize(state)?;
            event::emit_feedback_metrics(state.envelope());
        }

        attachment::scrub(state);

        if self.inner.config.processing_enabled() && !state.event_fully_normalized {
            relay_log::error!(
                tags.project = %state.project_id,
                tags.ty = state.event_type().map(|e| e.to_string()).unwrap_or("none".to_owned()),
                "ingested event without normalizing"
            );
        }

        Ok(())
    }
}
