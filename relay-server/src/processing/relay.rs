use std::fmt::Debug;
use std::sync::Arc;

use relay_cogs::{Cogs, ResourceId};
use relay_dynamic_config::Feature;
use relay_event_normalization::GeoIpLookup;
use relay_system::Addr;

use crate::envelope::Item;
use crate::managed::ManagedEnvelope;
use crate::processing::attachments::AttachmentProcessor;
use crate::processing::check_ins::CheckInsProcessor;
use crate::processing::client_reports::ClientReportsProcessor;
use crate::processing::errors::ErrorsProcessor;
use crate::processing::forward_unknown::ForwardUnknownProcessor;
use crate::processing::invalid::InvalidUnhandledProcessor;
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
use crate::processing::{Context, Output, Outputs, Processor, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};

/// Implementation of Relays processing pipeline.
///
/// The processor is able to fully process an envelope and return the processed results.
pub struct RelayProcessor {
    cogs: Cogs,

    attachments: AttachmentProcessor,
    check_ins: CheckInsProcessor,
    client_reports: ClientReportsProcessor,
    errors: ErrorsProcessor,
    forward_unknown: ForwardUnknownProcessor,
    invalid: InvalidUnhandledProcessor,
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
        cogs: Cogs,
        quota_limiter: &Arc<QuotaRateLimiter>,
        geoip_lookup: &GeoIpLookup,
        outcome_aggregator: Addr<TrackOutcome>,
    ) -> Self {
        // Just so everything fits in a single line.
        let ql = || Arc::clone(quota_limiter);

        Self {
            cogs,

            attachments: AttachmentProcessor::new(ql()),
            check_ins: CheckInsProcessor::new(ql()),
            client_reports: ClientReportsProcessor::new(outcome_aggregator),
            errors: ErrorsProcessor::new(ql(), geoip_lookup.clone()),
            forward_unknown: ForwardUnknownProcessor::new(),
            invalid: InvalidUnhandledProcessor::new(),
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
            ($processor:expr) => {{
                if let Some(output) = self.run_one(&$processor, &mut envelope, ctx).await {
                    outputs.push(output.map(Into::into));
                }
            }};
        }

        // The order of processors is deliberate and must not be changed lightly!
        //
        // Processors may partially consume items of the envelope and different processors may
        // process the same item types.
        //
        // Currently this allows for never intended combinations of different item types in envelopes,
        // ideally we make Relay slowly and slowly more restrictive and properly define and follow
        // the rules of item combinations allowed in a single envelope.

        // Item types which can be mixed into any envelope.
        run!(self.sessions);
        run!(self.client_reports);

        // Primary item types.
        run!(self.replays);
        if !pi.has_feature(Feature::SpanV2ExperimentalProcessing) {
            // To be fully replaced with the npn-legacy span processor.
            run!(self.legacy_spans);
        }
        run!(self.spans);
        run!(self.logs);
        run!(self.trace_metrics);
        run!(self.profile_chunks);
        run!(self.check_ins);

        // Event based envelopes.
        run!(self.errors);
        run!(self.transactions);

        // Item types which can be sent with a primary item type and can also be sent standalone.
        //
        // These need to be processed after their respective primary type.
        run!(self.trace_attachments);
        run!(self.attachments);
        run!(self.user_reports);
        run!(self.profiles);

        // Fallback for forward compatibility.
        run!(self.forward_unknown);
        // All remaining items which make no sense.
        run!(self.invalid);

        // After processing there must not be any items in the original envelope left.
        match envelope.envelope().is_empty() {
            true => envelope.accept(),
            false => {
                relay_log::error!(
                    items = ?envelope.envelope().items().map(Item::ty).collect::<Vec<_>>(),
                    "Processed envelope has items remaining"
                );
                envelope.update();
                envelope.reject(Outcome::Invalid(DiscardReason::Internal));
            }
        }

        outputs
    }

    async fn run_one<T: Processor>(
        &self,
        processor: &T,
        envelope: &mut ManagedEnvelope,
        ctx: Context<'_>,
    ) -> Option<Output<T::Output>>
    where
        T::Input: Debug,
    {
        let item = processor.prepare_envelope(envelope)?;

        relay_log::trace!(
            processor = std::any::type_name::<T>(),
            "processing item: {:?}",
            item.as_ref(),
        );

        let _token = self.cogs.timed(ResourceId::Relay, T::cogs());
        let output = processor.process(item, ctx).await;

        match output {
            Ok(output) => Some(output),
            Err(error) => {
                // This is not a fatal error case. For example a processor may reject an
                // item because it was filtered by an inbound filter or rate limit.
                // This means, other items from the same original envelope must still be processed.
                relay_log::debug!(
                    error = &error as &dyn std::error::Error,
                    processor = std::any::type_name::<T>(),
                    "item rejected by processor"
                );

                None
            }
        }
    }
}
