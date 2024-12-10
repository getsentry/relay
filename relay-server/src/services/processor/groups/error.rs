use crate::services::processor::groups::{
    ProcGroup, ProcGroupError, ProcGroupParams, ProcGroupResult,
};
#[cfg(feature = "processing")]
use crate::services::processor::{attachment, unreal};
use crate::services::processor::{
    event, process_event, report, ErrorGroup, InnerProcessor, ProcessingError,
};
use crate::services::projects::project::ProjectInfo;
use crate::utils::TypedEnvelope;
use crate::{if_processing, try_err};
use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::Metrics;
use relay_protocol::Empty;
use std::sync::Arc;

pub struct ErrorProcGroup {
    inner: Arc<InnerProcessor>,
    managed_envelope: TypedEnvelope<ErrorGroup>,
    metrics: Metrics,
    project_info: ProjectInfo,
    project_id: ProjectId,
    event_fully_normalized: bool,
    metrics_extracted: bool,
    spans_extracted: bool,
}

impl ProcGroup<ErrorGroup> for ErrorProcGroup {
    fn create(inner: Arc<InnerProcessor>, params: ProcGroupParams<ErrorGroup>) -> Self {
        Self {
            inner,
            managed_envelope: params.managed_envelope,
            metrics: params.metrics,
            event_fully_normalized: params.event_fully_normalized,
        }
    }

    fn process(mut self) -> Result<ProcGroupResult, ProcGroupError> {
        // Events can also contain user reports.
        report::process_user_reports(&mut self.managed_envelope);

        if_processing!(self.inner.config, {
            try_err!(
                unreal::expand(&mut self.managed_envelope, &self.inner.config),
                self.managed_envelope
            );
        });

        let (mut event, metrics_extracted, spans_extracted) = try_err!(
            event::extract(
                &mut self.managed_envelope,
                &mut self.metrics,
                self.event_fully_normalized,
                &self.inner.config
            ),
            self.managed_envelope
        );
        self.metrics_extracted = metrics_extracted;
        self.spans_extracted = spans_extracted;

        if_processing!(self.inner.config, {
            try_err!(
                unreal::process(&mut event, &mut self.managed_envelope),
                self.managed_envelope
            );

            let event_fully_normalized = attachment::create_placeholders(
                &mut event,
                &mut self.managed_envelope,
                &mut self.metrics,
            );
            self.event_fully_normalized = event_fully_normalized;
        });

        try_err!(
            event::finalize(
                &mut event,
                &mut self.managed_envelope,
                &mut self.metrics,
                &self.inner.config
            ),
            self.managed_envelope
        );

        if !event.is_empty() {
            try_err!(
                process_event(
                    &mut event,
                    &mut self.managed_envelope,
                    self.event_fully_normalized,
                    &self.inner.geoip_lookup,
                    &self.inner.global_config,
                    &self.inner.config,
                    &self.project_info,
                    &self.project_id
                ),
                self.managed_envelope
            );
        }

        // let filter_run = event::filter(state, &self.inner.global_config.current())?;
        //
        // if self.inner.config.processing_enabled() || matches!(filter_run, FiltersStatus::Ok) {
        //     dynamic_sampling::tag_error_with_sampling_decision(state, &self.inner.config);
        // }
        //
        // if_processing!(self.inner.config, {
        //     self.enforce_quotas(state)?;
        // });
        //
        // if state.has_event() {
        //     event::scrub(state)?;
        //     event::serialize(state)?;
        //     event::emit_feedback_metrics(state.envelope());
        // }
        //
        // attachment::scrub(state);
        //
        // if self.inner.config.processing_enabled() && !state.event_fully_normalized {
        //     relay_log::error!(
        //         tags.project = %state.project_id,
        //         tags.ty = state.event_type().map(|e| e.to_string()).unwrap_or("none".to_owned()),
        //         "ingested event without normalizing"
        //     );
        // }

        Err(ProcGroupError {
            managed_envelope: self.managed_envelope.into_processed(),
            error: ProcessingError::InvalidTimestamp,
        })
    }
}
