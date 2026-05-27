use std::sync::Arc;

use relay_dynamic_config::Feature;
use relay_event_normalization::GeoIpLookup;
use relay_system::Addr;

use crate::managed::{ManagedEnvelope, Rejected};
use crate::processing::attachments::AttachmentProcessor;
use crate::processing::check_ins::CheckInsProcessor;
use crate::processing::client_reports::ClientReportsProcessor;
use crate::processing::errors::ErrorsProcessor;
use crate::processing::forward_unknown::ForwardUnknownProcessor;
use crate::processing::legacy_spans::LegacySpansProcessor;
use crate::processing::logs::LogsProcessor;
use crate::processing::profile_chunks::ProfileChunksProcessor;
use crate::processing::profiles::ProfilesProcessor;
use crate::processing::replays::ReplaysProcessor;
use crate::processing::sessions::SessionsProcessor;
use crate::processing::spans::SpansProcessor;
use crate::processing::trace_attachments::TraceAttachmentsProcessor;
use crate::processing::trace_metrics::TraceMetricsProcessor;
use crate::processing::transactions::TransactionProcessor;
use crate::processing::user_reports::UserReportsProcessor;
use crate::processing::{Context, Errors, Output, Outputs, Processor, QuotaRateLimiter};
use crate::services::outcome::TrackOutcome;

/// Implementation of Relays processing pipeline.
///
/// The processor is able to fully process an envelope and return the processed results.
pub struct RelayProcessor {
    attachments: AttachmentProcessor,
    check_ins: CheckInsProcessor,
    client_reports: ClientReportsProcessor,
    errors: ErrorsProcessor,
    forward_unknown: ForwardUnknownProcessor,
    legacy_spans: LegacySpansProcessor,
    logs: LogsProcessor,
    profile_chunks: ProfileChunksProcessor,
    profiles: ProfilesProcessor,
    replays: ReplaysProcessor,
    sessions: SessionsProcessor,
    spans: SpansProcessor,
    trace_attachments: TraceAttachmentsProcessor,
    trace_metrics: TraceMetricsProcessor,
    transactions: TransactionProcessor,
    user_reports: UserReportsProcessor,
}

impl RelayProcessor {
    /// Creates a new [`Self`].
    pub fn new(
        quota_limiter: &Arc<QuotaRateLimiter>,
        geoip_lookup: &GeoIpLookup,
        outcome_aggregator: Addr<TrackOutcome>,
    ) -> Self {
        // Just so everything fits in a single line.
        let ql = || Arc::clone(quota_limiter);

        Self {
            attachments: AttachmentProcessor::new(ql()),
            check_ins: CheckInsProcessor::new(ql()),
            client_reports: ClientReportsProcessor::new(outcome_aggregator),
            errors: ErrorsProcessor::new(ql(), geoip_lookup.clone()),
            forward_unknown: ForwardUnknownProcessor::new(),
            legacy_spans: LegacySpansProcessor::new(ql(), geoip_lookup.clone()),
            logs: LogsProcessor::new(ql()),
            profile_chunks: ProfileChunksProcessor::new(ql()),
            profiles: ProfilesProcessor::new(ql()),
            replays: ReplaysProcessor::new(ql(), geoip_lookup.clone()),
            sessions: SessionsProcessor::new(ql()),
            spans: SpansProcessor::new(ql(), geoip_lookup.clone()),
            trace_attachments: TraceAttachmentsProcessor::new(ql()),
            trace_metrics: TraceMetricsProcessor::new(ql()),
            transactions: TransactionProcessor::new(ql(), geoip_lookup.clone()),
            user_reports: UserReportsProcessor::new(ql()),
        }
    }

    /// Fully processes an envelope and returns the resulting outputs.
    ///
    /// Since an envelope may contain payloads which need to be processed independently, the
    /// function may return multiple results at once.
    pub async fn run(
        &self,
        mut envelope: ManagedEnvelope,
        ctx: Context<'_>,
    ) -> Vec<Output<Outputs>> {
        let mut outputs = Vec::with_capacity(5);

        let pi = ctx.project_info;

        macro_rules! run {
            ($processor:expr) => {
                if let Some(item) = $processor.prepare_envelope(&mut envelope) {
                    relay_log::debug!(
                        processor = std::any::type_name_of_val(&$processor),
                        "processing {item:?}"
                    );
                    let output = $processor.process(item, ctx).await;

                    match output {
                        Ok(output) => outputs.push(output.map(Into::into)),
                        // Can't bubble up here, because `Filtered`
                        Err(err) => {
                            // TODO: needs a way to separate between fatal or non
                            relay_log::debug!("{err:?}");
                        }
                    }
                }
            };
        }

        run!(self.replays);
        run!(self.sessions);
        if !pi.has_feature(Feature::SpanV2ExperimentalProcessing) {
            run!(self.legacy_spans);
        }
        run!(self.spans);
        run!(self.logs);
        run!(self.trace_metrics);
        run!(self.profile_chunks);
        run!(self.trace_attachments);

        run!(self.check_ins);
        run!(self.client_reports);

        run!(self.errors);
        run!(self.transactions);

        run!(self.attachments);
        run!(self.user_reports);
        run!(self.profiles);

        run!(self.forward_unknown);

        // relay_log::error!(
        //     tags.project = %project_id,
        //     items = ?managed_envelope.envelope().items().next().map(Item::ty),
        //     "could not identify the processing group based on the envelope's items"
        // );
        //
        // Err(ProcessingError::NoProcessingGroup)

        // TODO: this needs to check for empty
        envelope.accept();

        outputs
    }
}
