use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_cogs::{AppFeature, Cogs, FeatureWeights, ResourceId, Token};
use relay_common::time::UnixTimestamp;
use relay_config::Config;
use relay_event_normalization::{ClockDriftProcessor, GeoIpLookup};
use relay_event_schema::processor::ProcessingAction;
use relay_filter::FilterStatKey;
use relay_metrics::{Bucket, BucketMetadata};
use relay_quotas::RateLimits;
use relay_sampling::evaluation::SamplingDecision;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, MessageHandler, NoResponse, Service};
use relay_threading::{AsyncPoolService, ServicePool};
use symbolic_unreal::{Unreal4Error, Unreal4ErrorKind};

use super::common::{self, SubmitClientReports};
use crate::envelope::{ItemType, MetricBucketsItem};
use crate::extractors::{RequestMeta, RequestTrust};
use crate::managed::ManagedEnvelope;
use crate::metrics::MetricOutcomes;
use crate::metrics_extraction::ExtractedMetrics;
use crate::processing::errors::SwitchProcessingError;
use crate::processing::relay::RelayProcessor;
use crate::processing::{self, Forward as _, Output, Outputs, QuotaRateLimiter};
use crate::service::ServiceError;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::metrics::{Aggregator, MergeBuckets};
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome, TrackOutcome};
use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::projects::project::{ProjectInfo, ProjectState};
use crate::services::upstream::UpstreamRelay;
use crate::statsd::{RelayCounters, RelayTimers};
#[cfg(feature = "processing")]
use {
    crate::services::objectstore::Objectstore,
    crate::services::store::Store,
    relay_quotas::{RateLimitingError, RedisRateLimiter},
};

/// The minimum clock drift for correction to apply.
pub const MINIMUM_CLOCK_DRIFT: Duration = Duration::from_secs(55 * 60);

/// An error returned when handling [`ProcessEnvelope`].
#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("invalid json in event")]
    InvalidJson(#[source] serde_json::Error),

    #[error("invalid message pack event payload")]
    InvalidMsgpack(#[from] rmp_serde::decode::Error),

    #[error("event data too deeply nested")]
    NestingTooDeep,

    #[error("invalid unreal crash report")]
    InvalidUnrealReport(#[source] Unreal4Error),

    #[error("event payload too large")]
    PayloadTooLarge(DiscardItemType),

    #[error("invalid transaction event")]
    InvalidTransaction,

    #[error("the item is not allowed/supported in this envelope")]
    UnsupportedItem,

    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),

    #[error("duplicate {0} in event")]
    DuplicateItem(ItemType),

    #[error("failed to extract event payload")]
    NoEventPayload,

    #[error("invalid security report type: {0:?}")]
    InvalidSecurityType(Bytes),

    #[error("unsupported security report type")]
    UnsupportedSecurityType,

    #[error("invalid security report")]
    InvalidSecurityReport(#[source] serde_json::Error),

    #[error("event filtered with reason: {0:?}")]
    EventFiltered(FilterStatKey),

    #[error("could not serialize event payload")]
    SerializeFailed(#[source] serde_json::Error),

    #[cfg(feature = "processing")]
    #[error("failed to apply quotas")]
    QuotasFailed(#[from] RateLimitingError),

    #[error("nintendo switch dying message processing failed {0:?}")]
    InvalidNintendoDyingMessage(#[source] SwitchProcessingError),

    #[cfg(all(sentry, feature = "processing"))]
    #[error("playstation dump processing failed: {0}")]
    InvalidPlaystationDump(String),

    #[error("invalid flame graph attachment")]
    InvalidFlamegraph,

    #[cfg(feature = "processing")]
    #[error("invalid attachment reference")]
    InvalidAttachmentRef,
}

impl ProcessingError {
    pub fn to_outcome(&self) -> Option<Outcome> {
        match self {
            Self::PayloadTooLarge(payload_type) => {
                Some(Outcome::Invalid(DiscardReason::ItemTooLarge(*payload_type)))
            }
            Self::InvalidJson(_) => Some(Outcome::Invalid(DiscardReason::InvalidJson)),
            Self::InvalidMsgpack(_) => Some(Outcome::Invalid(DiscardReason::InvalidMsgpack)),
            Self::NestingTooDeep => Some(Outcome::Invalid(DiscardReason::NestingTooDeep)),
            Self::InvalidSecurityType(_) => {
                Some(Outcome::Invalid(DiscardReason::SecurityReportType))
            }
            Self::UnsupportedItem => Some(Outcome::Invalid(DiscardReason::InvalidEnvelope)),
            Self::InvalidSecurityReport(_) => Some(Outcome::Invalid(DiscardReason::SecurityReport)),
            Self::UnsupportedSecurityType => Some(Outcome::Filtered(FilterStatKey::InvalidCsp)),
            Self::InvalidTransaction => Some(Outcome::Invalid(DiscardReason::InvalidTransaction)),
            Self::InvalidFlamegraph => Some(Outcome::Invalid(DiscardReason::InvalidFlamegraph)),
            Self::DuplicateItem(_) => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::NoEventPayload => Some(Outcome::Invalid(DiscardReason::NoEventPayload)),
            Self::InvalidNintendoDyingMessage(_) => Some(Outcome::Invalid(DiscardReason::Payload)),
            #[cfg(all(sentry, feature = "processing"))]
            Self::InvalidPlaystationDump(_) => Some(Outcome::Invalid(DiscardReason::Payload)),
            Self::InvalidUnrealReport(err) if err.kind() == Unreal4ErrorKind::BadCompression => {
                Some(Outcome::Invalid(DiscardReason::InvalidCompression))
            }
            Self::InvalidUnrealReport(_) => Some(Outcome::Invalid(DiscardReason::ProcessUnreal)),
            Self::SerializeFailed(_) | Self::ProcessingFailed(_) => {
                Some(Outcome::Invalid(DiscardReason::Internal))
            }
            #[cfg(feature = "processing")]
            Self::QuotasFailed(_) => Some(Outcome::Invalid(DiscardReason::Internal)),
            Self::EventFiltered(key) => Some(Outcome::Filtered(key.clone())),

            #[cfg(feature = "processing")]
            Self::InvalidAttachmentRef => {
                Some(Outcome::Invalid(DiscardReason::InvalidAttachmentRef))
            }
        }
    }
}

impl From<Unreal4Error> for ProcessingError {
    fn from(err: Unreal4Error) -> Self {
        match err.kind() {
            Unreal4ErrorKind::TooLarge => Self::PayloadTooLarge(ItemType::UnrealReport.into()),
            _ => ProcessingError::InvalidUnrealReport(err),
        }
    }
}

/// A container for extracted metrics during processing.
///
/// The container enforces that the extracted metrics are correctly tagged
/// with the dynamic sampling decision.
#[derive(Debug)]
pub struct ProcessingExtractedMetrics {
    metrics: ExtractedMetrics,
}

impl ProcessingExtractedMetrics {
    pub fn new() -> Self {
        Self {
            metrics: ExtractedMetrics::default(),
        }
    }

    pub fn into_inner(self) -> ExtractedMetrics {
        self.metrics
    }

    /// Extends the contained metrics with [`ExtractedMetrics`].
    pub fn extend(
        &mut self,
        extracted: ExtractedMetrics,
        sampling_decision: Option<SamplingDecision>,
    ) {
        self.extend_project_metrics(extracted.0, sampling_decision);
    }

    /// Extends the contained project metrics.
    pub fn extend_project_metrics<I>(
        &mut self,
        buckets: I,
        sampling_decision: Option<SamplingDecision>,
    ) where
        I: IntoIterator<Item = Bucket>,
    {
        self.metrics.0.extend(buckets.into_iter().map(|mut bucket| {
            bucket.metadata.extracted_from_indexed =
                sampling_decision == Some(SamplingDecision::Keep);
            bucket
        }));
    }
}

fn send_metrics(metrics: ExtractedMetrics, project_key: ProjectKey, aggregator: &Addr<Aggregator>) {
    let ExtractedMetrics(project_metrics) = metrics;

    if !project_metrics.is_empty() {
        aggregator.send(MergeBuckets {
            project_key,
            buckets: project_metrics,
        });
    }
}

/// Applies processing to all contents of the given envelope.
///
/// Depending on the contents of the envelope and Relay's mode, this includes:
///
///  - Basic normalization and validation for all item types.
///  - Clock drift correction if the required `sent_at` header is present.
///  - Expansion of certain item types (e.g. unreal).
///  - Store normalization for event payloads in processing mode.
///  - Rate limiters and inbound filters on events in processing mode.
#[derive(Debug)]
pub struct ProcessEnvelope {
    /// Envelope to process.
    pub envelope: ManagedEnvelope,
    /// The project info.
    pub project_info: Arc<ProjectInfo>,
    /// Currently active cached rate limits for this project.
    pub rate_limits: Arc<RateLimits>,
    /// Root sampling project info.
    pub sampling_project_info: Option<Arc<ProjectInfo>>,
}

/// Parses metric buckets and pushes them to the project's aggregator.
///
/// Each [`MetricBuckets`](ItemType::MetricBuckets) item contains a JSON list of buckets. The entire
/// list is dropped on parsing failure. Other envelope items are ignored with an error message.
///
/// Additionally, processing applies clock drift correction using the system clock of this Relay, if
/// the Envelope specifies the [`sent_at`](crate::envelope::Envelope::sent_at) header.
#[derive(Debug)]
pub struct ProcessMetrics {
    /// A list of metric items.
    pub data: MetricData,
    /// The target project.
    pub project_key: ProjectKey,
    /// Whether to keep or reset the metric metadata.
    pub source: BucketSource,
    /// The wall clock time at which the request was received.
    pub received_at: DateTime<Utc>,
    /// The value of the Envelope's [`sent_at`](crate::envelope::Envelope::sent_at) header for clock drift
    /// correction.
    pub sent_at: Option<DateTime<Utc>>,
}

/// Raw unparsed metric data.
#[derive(Debug)]
pub enum MetricData {
    /// Raw data, unparsed metric bucket items.
    Raw(Vec<MetricBucketsItem>),
    /// Already parsed buckets but unprocessed.
    Parsed(Vec<Bucket>),
}

impl MetricData {
    /// Consumes the metric data and parses the contained buckets.
    ///
    /// If the contained data is already parsed the buckets are returned unchanged.
    fn into_buckets(self) -> Vec<Bucket> {
        let items = match self {
            Self::Parsed(buckets) => return buckets,
            Self::Raw(items) => items,
        };

        let mut buckets = Vec::new();
        for item in items {
            let payload = item.payload();
            match serde_json::from_slice::<Vec<Bucket>>(&payload) {
                Ok(parsed_buckets) => {
                    // Re-use the allocation of `b` if possible.
                    if buckets.is_empty() {
                        buckets = parsed_buckets;
                    } else {
                        buckets.extend(parsed_buckets);
                    }
                }
                Err(error) => {
                    relay_log::debug!(
                        error = &error as &dyn Error,
                        "failed to parse metric bucket",
                    );
                    metric!(counter(RelayCounters::MetricBucketsParsingFailed) += 1);
                }
            }
        }
        buckets
    }
}

#[derive(Debug)]
pub struct ProcessBatchedMetrics {
    /// Metrics payload in JSON format.
    pub payload: Bytes,
    /// Whether to keep or reset the metric metadata.
    pub source: BucketSource,
    /// The wall clock time at which the request was received.
    pub received_at: DateTime<Utc>,
    /// The wall clock time at which the request was received.
    pub sent_at: Option<DateTime<Utc>>,
}

/// Source information where a metric bucket originates from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BucketSource {
    /// The metric bucket originated from an internal Relay use case.
    ///
    /// The metric bucket originates either from within the same Relay
    /// or was accepted coming from another Relay which is registered as
    /// an internal Relay via Relay's configuration.
    Internal,
    /// The bucket source originated from an untrusted source.
    ///
    /// Managed Relays sending extracted metrics are considered external,
    /// it's a project use case but it comes from an untrusted source.
    External,
}

impl BucketSource {
    /// Infers the bucket source from [`RequestMeta::request_trust`].
    pub fn from_meta(meta: &RequestMeta) -> Self {
        match meta.request_trust() {
            RequestTrust::Trusted => Self::Internal,
            RequestTrust::Untrusted => Self::External,
        }
    }
}

/// CPU-intensive processing tasks for envelopes.
#[derive(Debug)]
pub enum EnvelopeProcessor {
    ProcessEnvelope(Box<ProcessEnvelope>),
    ProcessProjectMetrics(Box<ProcessMetrics>),
    ProcessBatchedMetrics(Box<ProcessBatchedMetrics>),
    SubmitClientReports(Box<SubmitClientReports>),
}

impl EnvelopeProcessor {
    /// Returns the name of the message variant.
    pub fn variant(&self) -> &'static str {
        match self {
            EnvelopeProcessor::ProcessEnvelope(_) => "ProcessEnvelope",
            EnvelopeProcessor::ProcessProjectMetrics(_) => "ProcessProjectMetrics",
            EnvelopeProcessor::ProcessBatchedMetrics(_) => "ProcessBatchedMetrics",
            EnvelopeProcessor::SubmitClientReports(_) => "SubmitClientReports",
        }
    }
}

impl relay_system::Interface for EnvelopeProcessor {}

impl FromMessage<ProcessEnvelope> for EnvelopeProcessor {
    type Response = relay_system::NoResponse;

    fn from_message(message: ProcessEnvelope, _sender: ()) -> Self {
        Self::ProcessEnvelope(Box::new(message))
    }
}

impl FromMessage<ProcessMetrics> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: ProcessMetrics, _: ()) -> Self {
        Self::ProcessProjectMetrics(Box::new(message))
    }
}

impl FromMessage<ProcessBatchedMetrics> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: ProcessBatchedMetrics, _: ()) -> Self {
        Self::ProcessBatchedMetrics(Box::new(message))
    }
}

impl FromMessage<SubmitClientReports> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: SubmitClientReports, _: ()) -> Self {
        Self::SubmitClientReports(Box::new(message))
    }
}

/// Creates a multi-threaded envelope processor.
#[cfg_attr(feature = "processing", expect(clippy::too_many_arguments))]
pub fn create_service(
    pool: ServicePool,
    config: Arc<Config>,
    global_config: GlobalConfigHandle,
    project_cache: ProjectCacheHandle,
    cogs: Cogs,
    #[cfg(feature = "processing")] rate_limiter: Option<RedisRateLimiter>,
    addrs: Addrs,
    metric_outcomes: MetricOutcomes,
) -> impl Service<Interface = EnvelopeProcessor> {
    AsyncPoolService::new(
        Arc::new(InnerProcessor::new(
            config,
            global_config,
            project_cache,
            cogs,
            #[cfg(feature = "processing")]
            rate_limiter,
            addrs,
            metric_outcomes,
        )),
        pool,
    )
}

/// Contains the addresses of services that the processor publishes to.
pub struct Addrs {
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub upstream_relay: Addr<UpstreamRelay>,
    #[cfg(feature = "processing")]
    pub objectstore: Option<Addr<Objectstore>>,
    #[cfg(feature = "processing")]
    pub store_forwarder: Option<Addr<Store>>,
    pub aggregator: Addr<Aggregator>,
}

impl Default for Addrs {
    fn default() -> Self {
        Addrs {
            outcome_aggregator: Addr::dummy(),
            upstream_relay: Addr::dummy(),
            #[cfg(feature = "processing")]
            objectstore: None,
            #[cfg(feature = "processing")]
            store_forwarder: None,
            aggregator: Addr::dummy(),
        }
    }
}

/// The message handler for [`EnvelopeProcessor`] messages.
struct InnerProcessor {
    config: Arc<Config>,
    global_config: GlobalConfigHandle,
    project_cache: ProjectCacheHandle,
    cogs: Cogs,
    addrs: Addrs,
    metric_outcomes: MetricOutcomes,
    processor: RelayProcessor,
}

impl InnerProcessor {
    fn new(
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        project_cache: ProjectCacheHandle,
        cogs: Cogs,
        #[cfg(feature = "processing")] rate_limiter: Option<RedisRateLimiter>,
        addrs: Addrs,
        metric_outcomes: MetricOutcomes,
    ) -> Self {
        let c = config.current();

        let geoip_lookup = c
            .geoip_path()
            .and_then(
                |p| match GeoIpLookup::open(p).context(ServiceError::GeoIp) {
                    Ok(geoip) => Some(geoip),
                    Err(err) => {
                        relay_log::error!("failed to open GeoIP db {p:?}: {err:?}");
                        None
                    }
                },
            )
            .unwrap_or_else(GeoIpLookup::empty);

        if let Some(build_epoch) = geoip_lookup.build_epoch() {
            relay_log::info!("Loaded GeoIP database (build: {build_epoch})");
        }

        let quota_limiter = Arc::new(QuotaRateLimiter::new(
            #[cfg(feature = "processing")]
            project_cache.clone(),
            #[cfg(feature = "processing")]
            rate_limiter,
        ));
        Self {
            global_config,
            project_cache,
            processor: RelayProcessor::new(
                cogs.clone(),
                &quota_limiter,
                &geoip_lookup,
                addrs.outcome_aggregator.clone(),
            ),
            cogs,
            addrs,
            metric_outcomes,
            config,
        }
    }

    async fn process_envelope(
        &self,
        project_id: ProjectId,
        mut envelope: ManagedEnvelope,
        ctx: processing::Context<'_>,
    ) -> Vec<Output<Outputs>> {
        // Pre-process the envelope headers.
        if let Some(sampling_state) = ctx.sampling_project_info {
            // Both transactions and standalone span envelopes need a normalized DSC header
            // to make sampling rules based on the segment/transaction name work correctly.
            envelope
                .envelope_mut()
                .parametrize_dsc_transaction(&sampling_state.config.tx_name_rules);
        }

        // Ensure the project ID is updated to the stored instance for this project cache. This can
        // differ in two cases:
        //  1. The envelope was sent to the legacy `/store/` endpoint without a project ID.
        //  2. The DSN was moved and the envelope sent to the old project ID.
        envelope
            .envelope_mut()
            .meta_mut()
            .set_project_id(project_id);

        self.processor.run(envelope, ctx).await
    }

    /// Processes the envelope and returns the processed envelope back.
    ///
    /// Returns `Some` if the envelope passed inbound filtering and rate limiting. Invalid items are
    /// removed from the envelope. Otherwise, if the envelope is empty or the entire envelope needs
    /// to be dropped, this is `None`.
    async fn process<'a>(
        &self,
        mut envelope: ManagedEnvelope,
        ctx: processing::Context<'a>,
    ) -> Vec<Output<Outputs>> {
        // Prefer the project's project ID, and fall back to the stated project id from the
        // envelope. The project ID is available in all modes, other than in proxy mode, where
        // envelopes for unknown projects are forwarded blindly.
        //
        // Neither ID can be available in proxy mode on the /store/ endpoint. This is not supported,
        // since we cannot process an envelope without project ID, so drop it.
        let Some(project_id) = ctx
            .project_info
            .project_id
            .or_else(|| envelope.envelope().meta().project_id())
        else {
            relay_log::error!(
                tags.project_key = %envelope.envelope().meta().public_key(),
                "project info does not contain project id"
            );
            envelope.reject(Outcome::Invalid(DiscardReason::Internal));
            return Vec::new();
        };

        relay_log::configure_scope(|scope| {
            scope.set_tag("project_id", project_id);
        });

        self.process_envelope(project_id, envelope, ctx).await
    }

    async fn handle_process_envelope(&self, cogs: &mut Token, message: ProcessEnvelope) {
        let wait_time = message.envelope.age();
        metric!(timer(RelayTimers::EnvelopeWaitTime) = wait_time);

        // This COGS handling may need an overhaul in the future:
        // Cancel the passed in token, to start individual measurements per processor instead.
        cogs.cancel();

        let global_config = self.global_config.current().unwrap_or_default();
        let config = self.config.current();

        let ctx = processing::Context {
            config: &config,
            global_config: &global_config,
            project_info: &message.project_info,
            sampling_project_info: message.sampling_project_info.as_deref(),
            rate_limits: &message.rate_limits,
        };

        let project_key = message.envelope.meta().public_key();
        // Only allow sending to the sampling key, if we successfully loaded a sampling project
        // info relating to it. This filters out unknown/invalid project keys as well as project
        // keys from different organizations.
        let sampling_key = ctx
            .sampling_project_info
            .and_then(|p| p.get_public_key_config())
            .map(|pkc| pkc.public_key);

        relay_log::configure_scope(|scope| {
            scope.set_tag("project_key", project_key);
            if let Some(sampling_key) = sampling_key {
                scope.set_tag("sampling_key", sampling_key);
            }
            let meta = message.envelope.envelope().meta();
            scope.set_tag("sdk_name", meta.client_name());
            if let Some(client) = meta.client() {
                scope.set_tag("sdk", client);
            }
            if let Some(user_agent) = meta.user_agent() {
                scope.set_extra("user_agent", user_agent.into());
            }
        });

        let mut envelopes: smallvec::SmallVec<[ManagedEnvelope; 1]> =
            smallvec::smallvec![message.envelope];

        // The first envelope we process is not an intermediate.
        let mut is_intermediate = false;

        while let Some(envelope) = envelopes.pop() {
            let outputs = metric!(
                timer(RelayTimers::EnvelopeProcessingTime),
                is_intermediate = if is_intermediate { "true" } else { "false" },
                { self.process(envelope, ctx).await }
            );

            let ctx = ctx.to_forward();
            for Output {
                main,
                metrics,
                intermediates,
            } in outputs
            {
                if let Some(metrics) = metrics {
                    let agg = &self.addrs.aggregator;
                    metrics.accept(|metrics| {
                        send_metrics(metrics, project_key, agg);
                    });
                }

                if let Some(output) = main {
                    self.submit_upstream(output, ctx);
                }

                if let Some(intermediates) = intermediates {
                    envelopes.extend(intermediates.split(|i| i).map(ManagedEnvelope::from));
                }

                // Every envelope past the first is an intermediate.
                is_intermediate = true;
            }
        }
    }

    fn handle_process_metrics(&self, cogs: &mut Token, message: ProcessMetrics) {
        let ProcessMetrics {
            data,
            project_key,
            received_at,
            sent_at,
            source,
        } = message;

        let received_timestamp =
            UnixTimestamp::from_datetime(received_at).unwrap_or(UnixTimestamp::now());

        let mut buckets = data.into_buckets();
        if buckets.is_empty() {
            return;
        };
        cogs.update(relay_metrics::cogs::BySize(&buckets));

        let clock_drift_processor =
            ClockDriftProcessor::new(sent_at, received_at).at_least(MINIMUM_CLOCK_DRIFT);

        buckets.retain_mut(|bucket| {
            if let Err(error) = relay_metrics::normalize_bucket(bucket) {
                relay_log::debug!(error = &error as &dyn Error, "dropping bucket {bucket:?}");
                return false;
            }

            if !super::metrics::is_valid_namespace(bucket, source) {
                relay_log::debug!("dropping bucket in invalid namespace {bucket:?}");
                return false;
            }

            clock_drift_processor.process_timestamp(&mut bucket.timestamp);

            if !matches!(source, BucketSource::Internal) {
                bucket.metadata = BucketMetadata::new(received_timestamp);
            }

            true
        });

        let project = self.project_cache.get(project_key);

        // Best effort check to filter and rate limit buckets, if there is no project state
        // available at the current time, we will check again after flushing.
        let buckets = match project.state() {
            ProjectState::Enabled(project_info) => {
                let rate_limits = project.rate_limits().current_limits();
                common::check_buckets(
                    project_key,
                    project_info,
                    &rate_limits,
                    buckets,
                    &self.metric_outcomes,
                )
            }
            _ => buckets,
        };

        relay_log::trace!("merging metric buckets into the aggregator");
        self.addrs
            .aggregator
            .send(MergeBuckets::new(project_key, buckets));
    }

    fn handle_process_batched_metrics(&self, cogs: &mut Token, message: ProcessBatchedMetrics) {
        let ProcessBatchedMetrics {
            payload,
            source,
            received_at,
            sent_at,
        } = message;

        #[derive(serde::Deserialize)]
        struct Wrapper {
            buckets: HashMap<ProjectKey, Vec<Bucket>>,
        }

        let buckets = match serde_json::from_slice(&payload) {
            Ok(Wrapper { buckets }) => buckets,
            Err(error) => {
                relay_log::debug!(
                    error = &error as &dyn Error,
                    "failed to parse batched metrics",
                );
                metric!(counter(RelayCounters::MetricBucketsParsingFailed) += 1);
                return;
            }
        };

        for (project_key, buckets) in buckets {
            self.handle_process_metrics(
                cogs,
                ProcessMetrics {
                    data: MetricData::Parsed(buckets),
                    project_key,
                    source,
                    received_at,
                    sent_at,
                },
            )
        }
    }

    /// Submits a processor [`Output`] to the appropriate upstream.
    ///
    /// If processing is enabled, the upstream is Kafka.
    fn submit_upstream(&self, output: Outputs, ctx: processing::ForwardContext<'_>) {
        #[cfg(feature = "processing")]
        if ctx.config.processing_enabled()
            && let Some(store_forwarder) = &self.addrs.store_forwarder
        {
            use crate::processing::StoreHandle;

            let objectstore = self.addrs.objectstore.as_ref();
            let handle = StoreHandle::new(store_forwarder, objectstore);

            output
                .forward_store(handle, ctx)
                .unwrap_or_else(|err| err.into_inner());

            return;
        }

        match output.serialize_envelope(ctx) {
            Ok(envelope) => {
                let envelope = ManagedEnvelope::from(envelope);
                common::submit_envelope_upstream(
                    envelope,
                    ctx.config,
                    ctx.project_info.upstream.clone(),
                    &self.addrs.upstream_relay,
                    &self.project_cache,
                );
            }
            Err(_) => relay_log::error!("failed to serialize output to an envelope"),
        };
    }

    fn feature_weights(&self, message: &EnvelopeProcessor) -> FeatureWeights {
        match message {
            // Envelope is split later and tokens are attributed then.
            EnvelopeProcessor::ProcessEnvelope(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::ProcessProjectMetrics(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::ProcessBatchedMetrics(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::SubmitClientReports(_) => AppFeature::ClientReports.into(),
        }
    }
}

impl MessageHandler for InnerProcessor {
    type Interface = EnvelopeProcessor;

    async fn handle_message(&self, message: Self::Interface) {
        let ty = message.variant();
        let feature_weights = self.feature_weights(&message);

        metric!(timer(RelayTimers::ProcessMessageDuration), message = ty, {
            let mut cogs = self.cogs.timed(ResourceId::Relay, feature_weights);

            match message {
                EnvelopeProcessor::ProcessEnvelope(m) => {
                    self.handle_process_envelope(&mut cogs, *m).await
                }
                EnvelopeProcessor::ProcessProjectMetrics(m) => {
                    self.handle_process_metrics(&mut cogs, *m)
                }
                EnvelopeProcessor::ProcessBatchedMetrics(m) => {
                    self.handle_process_batched_metrics(&mut cogs, *m)
                }
                EnvelopeProcessor::SubmitClientReports(m) => common::submit_client_reports(
                    *m,
                    &self.config.current(),
                    &self.addrs.outcome_aggregator,
                    &self.addrs.upstream_relay,
                    &self.project_cache,
                ),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use relay_common::glob2::LazyGlob;
    use relay_dynamic_config::ProjectConfig;
    use relay_event_normalization::{
        NormalizationConfig, RedactionRule, TransactionNameConfig, TransactionNameRule,
    };
    use relay_event_schema::protocol::{Event, EventId, TransactionSource};
    use relay_pii::DataScrubbingConfig;
    use relay_protocol::Annotated;
    use relay_test::mock_service;
    use similar_asserts::assert_eq;

    #[cfg(feature = "processing")]
    use crate::service::create_redis_clients;

    use crate::envelope::{ContentType, Envelope};
    use crate::utils;

    use super::*;

    async fn create_test_processor(config: Config) -> InnerProcessor {
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (aggregator, _) = mock_service("aggregator", (), |&mut (), _| {});
        let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});

        #[cfg(feature = "processing")]
        let redis_clients = config
            .current()
            .redis()
            .map(|c| create_redis_clients(c))
            .transpose()
            .unwrap();

        let metric_outcomes = MetricOutcomes::new(outcome_aggregator.clone());

        let config = Arc::new(config);
        InnerProcessor::new(
            Arc::clone(&config),
            GlobalConfigHandle::fixed(Default::default()),
            ProjectCacheHandle::for_test(),
            Cogs::noop(),
            #[cfg(feature = "processing")]
            common::create_rate_limiter(&config.current(), redis_clients),
            Addrs {
                outcome_aggregator,
                upstream_relay,
                #[cfg(feature = "processing")]
                store_forwarder: None,
                #[cfg(feature = "processing")]
                objectstore: None,
                aggregator,
            },
            metric_outcomes,
        )
    }

    async fn create_test_processor_with_addrs(config: Config, addrs: Addrs) -> InnerProcessor {
        #[cfg(feature = "processing")]
        let redis_clients = config
            .current()
            .redis()
            .map(|c| create_redis_clients(c))
            .transpose()
            .unwrap();
        let metric_outcomes = MetricOutcomes::new(addrs.outcome_aggregator.clone());

        let config = Arc::new(config);
        InnerProcessor::new(
            Arc::clone(&config),
            GlobalConfigHandle::fixed(Default::default()),
            ProjectCacheHandle::for_test(),
            Cogs::noop(),
            #[cfg(feature = "processing")]
            common::create_rate_limiter(&config.current(), redis_clients),
            addrs,
            metric_outcomes,
        )
    }

    async fn process_to_single_envelope<'a>(
        processor: &InnerProcessor,
        envelope: ManagedEnvelope,
        ctx: processing::Context<'a>,
    ) -> Box<Envelope> {
        let mut outputs = processor.process(envelope, ctx).await;
        assert_eq!(outputs.len(), 1);

        let Output {
            main,
            metrics,
            intermediates: _,
        } = outputs.pop().unwrap();

        if let Some(metrics) = metrics {
            metrics.accept(drop);
        }

        main.unwrap()
            .serialize_envelope(ctx.to_forward())
            .unwrap()
            .accept(|envelope| envelope)
    }

    #[tokio::test]
    async fn test_browser_version_extraction_with_pii_like_data() {
        let processor = create_test_processor(Default::default()).await;
        let outcome_aggregator = Addr::dummy();
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
                let mut item = crate::envelope::EventItem::new();
                item.set_payload(
                    ContentType::Json,
                    r#"
                    {
                        "request": {
                            "headers": [
                                ["User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"]
                            ]
                        }
                    }
                "#,
                );
                item
            });

        let mut datascrubbing_settings = DataScrubbingConfig::default();
        // enable all the default scrubbing
        datascrubbing_settings.scrub_data = true;
        datascrubbing_settings.scrub_defaults = true;
        datascrubbing_settings.scrub_ip_addresses = true;

        // Make sure to mask any IP-like looking data
        let pii_config = serde_json::from_str(r#"{"applications": {"**": ["@ip:mask"]}}"#).unwrap();

        let config = ProjectConfig {
            datascrubbing_settings,
            pii_config: Some(pii_config),
            ..Default::default()
        };

        let project_info = ProjectInfo {
            config,
            ..Default::default()
        };

        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator);

        let ctx = processing::Context {
            project_info: &project_info,
            ..processing::Context::for_test()
        };

        let new_envelope = process_to_single_envelope(&processor, envelope, ctx).await;

        let event_item = new_envelope.items().last().unwrap();
        let annotated_event: Annotated<Event> =
            Annotated::from_json_bytes(&event_item.payload()).unwrap();
        let event = annotated_event.into_value().unwrap();
        let headers = event
            .request
            .into_value()
            .unwrap()
            .headers
            .into_value()
            .unwrap();

        // IP-like data must be masked
        assert_eq!(
            Some(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/********* Safari/537.36"
            ),
            headers.get_header("User-Agent")
        );
        // But we still get correct browser and version number
        let contexts = event.contexts.into_value().unwrap();
        let browser = contexts.0.get("browser").unwrap();
        assert_eq!(
            r#"{"browser":"Chrome 103.0.0","name":"Chrome","version":"103.0.0","type":"browser"}"#,
            browser.to_json().unwrap()
        );
    }

    #[tokio::test]
    #[cfg(feature = "processing")]
    async fn test_materialize_dsc() {
        use crate::services::projects::project::PublicKeyConfig;

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        let dsc = r#"{
            "trace_id": "00000000-0000-0000-0000-000000000001",
            "public_key": "e12d836b15bb49d7bbf99e64295d995b",
            "sample_rate": "0.2"
        }"#;
        envelope.set_dsc(serde_json::from_str(dsc).unwrap());

        let mut item = crate::envelope::EventItem::new();
        item.set_payload(ContentType::Json, r#"{}"#);
        envelope.add_item(item);

        let outcome_aggregator = Addr::dummy();
        let managed_envelope = ManagedEnvelope::new(envelope, outcome_aggregator);

        let mut project_info = ProjectInfo::default();
        project_info.public_keys.push(PublicKeyConfig {
            public_key: ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
            numeric_id: Some(1),
        });

        let config = serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        });

        let processor =
            create_test_processor(Config::from_json_value(config.clone()).unwrap()).await;
        let config = Config::from_json_value(config).unwrap().current();
        let ctx = processing::Context {
            config: &config,
            project_info: &project_info,
            sampling_project_info: Some(&project_info),
            ..processing::Context::for_test()
        };

        let envelope = process_to_single_envelope(&processor, managed_envelope, ctx).await;
        let event = envelope
            .get_item_by(|item| item.ty() == &ItemType::Event)
            .unwrap();

        let event = Annotated::<Event>::from_json_bytes(&event.payload()).unwrap();
        insta::assert_debug_snapshot!(event.value().unwrap()._dsc, @r###"
        Object(
            {
                "environment": ~,
                "public_key": String(
                    "e12d836b15bb49d7bbf99e64295d995b",
                ),
                "release": ~,
                "replay_id": ~,
                "sample_rate": String(
                    "0.2",
                ),
                "trace_id": String(
                    "00000000000000000000000000000001",
                ),
                "transaction": ~,
            },
        )
        "###);
    }

    fn capture_test_event(transaction_name: &str, source: TransactionSource) -> Vec<String> {
        let mut event = Annotated::<Event>::from_json(
            r#"
            {
                "type": "transaction",
                "transaction": "/foo/",
                "timestamp": 946684810.0,
                "start_timestamp": 946684800.0,
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053",
                        "op": "http.server",
                        "type": "trace"
                    }
                },
                "transaction_info": {
                    "source": "url"
                }
            }
            "#,
        )
        .unwrap();
        let e = event.value_mut().as_mut().unwrap();
        e.transaction.set_value(Some(transaction_name.into()));

        e.transaction_info
            .value_mut()
            .as_mut()
            .unwrap()
            .source
            .set_value(Some(source));

        relay_statsd::with_capturing_test_client(|| {
            utils::log_transaction_name_metrics(&mut event, |event| {
                let config = NormalizationConfig {
                    transaction_name_config: TransactionNameConfig {
                        rules: &[TransactionNameRule {
                            pattern: LazyGlob::new("/foo/*/**".to_owned()),
                            expiry: DateTime::<Utc>::MAX_UTC,
                            redaction: RedactionRule::Replace {
                                substitution: "*".to_owned(),
                            },
                        }],
                    },
                    ..Default::default()
                };
                relay_event_normalization::normalize_event(event, &config)
            });
        })
    }

    #[test]
    fn test_log_transaction_metrics_none() {
        let captures = capture_test_event("/nothing", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r###"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:none,source_out:sanitized,is_404:false",
        ]
        "###);
    }

    #[test]
    fn test_log_transaction_metrics_rule() {
        let captures = capture_test_event("/foo/john/denver", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r###"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:rule,source_out:sanitized,is_404:false",
        ]
        "###);
    }

    #[test]
    fn test_log_transaction_metrics_pattern() {
        let captures = capture_test_event("/something/12345", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r###"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:pattern,source_out:sanitized,is_404:false",
        ]
        "###);
    }

    #[test]
    fn test_log_transaction_metrics_both() {
        let captures = capture_test_event("/foo/john/12345", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r###"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:both,source_out:sanitized,is_404:false",
        ]
        "###);
    }

    #[test]
    fn test_log_transaction_metrics_no_match() {
        let captures = capture_test_event("/foo/john/12345", TransactionSource::Route);
        insta::assert_debug_snapshot!(captures, @r###"
        [
            "event.transaction_name_changes:1|c|#source_in:route,changes:none,source_out:route,is_404:false",
        ]
        "###);
    }

    #[tokio::test]
    async fn test_process_metrics_bucket_metadata() {
        let mut token = Cogs::noop().timed(ResourceId::Relay, AppFeature::Unattributed);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let received_at = Utc::now();
        let config = Config::default();

        let (aggregator, mut aggregator_rx) = Addr::custom();
        let processor = create_test_processor_with_addrs(
            config,
            Addrs {
                aggregator,
                ..Default::default()
            },
        )
        .await;

        let mut item = crate::envelope::MetricBucketsItem::new();
        item.set_payload(
            ContentType::Json,
            serde_json::json!([{
                "timestamp": received_at.timestamp(),
                "width": 0,
                "name": "s:sessions/foo@none",
                "type": "s",
                "value": [3182887624u32, 4267882815u32],
            }])
            .to_string(),
        );
        for (source, expected_received_at) in [
            (
                BucketSource::External,
                Some(UnixTimestamp::from_datetime(received_at).unwrap()),
            ),
            (BucketSource::Internal, None),
        ] {
            let message = ProcessMetrics {
                data: MetricData::Raw(vec![item.clone()]),
                project_key,
                source,
                received_at,
                sent_at: Some(Utc::now()),
            };
            processor.handle_process_metrics(&mut token, message);

            let Aggregator::MergeBuckets(merge_buckets) = aggregator_rx.recv().await.unwrap();
            let buckets = merge_buckets.buckets;
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].metadata.received_at, expected_received_at);
        }
    }

    #[tokio::test]
    async fn test_process_batched_metrics() {
        let mut token = Cogs::noop().timed(ResourceId::Relay, AppFeature::Unattributed);
        let received_at = Utc::now();
        let config = Config::default();

        let (aggregator, mut aggregator_rx) = Addr::custom();
        let processor = create_test_processor_with_addrs(
            config,
            Addrs {
                aggregator,
                ..Default::default()
            },
        )
        .await;

        let payload = r#"{
    "buckets": {
        "11111111111111111111111111111111": [
            {
                "timestamp": 1615889440,
                "width": 0,
                "name": "d:transactions/endpoint.response_time@millisecond",
                "type": "d",
                "value": [
                  68.0
                ],
                "tags": {
                  "route": "user_index"
                }
            }
        ],
        "22222222222222222222222222222222": [
            {
                "timestamp": 1615889440,
                "width": 0,
                "name": "d:transactions/endpoint.cache_rate@none",
                "type": "d",
                "value": [
                  36.0
                ]
            }
        ]
    }
}
"#;
        let message = ProcessBatchedMetrics {
            payload: Bytes::from(payload),
            source: BucketSource::Internal,
            received_at,
            sent_at: Some(Utc::now()),
        };
        processor.handle_process_batched_metrics(&mut token, message);

        let Aggregator::MergeBuckets(mb1) = aggregator_rx.recv().await.unwrap();
        let Aggregator::MergeBuckets(mb2) = aggregator_rx.recv().await.unwrap();

        let mut messages = vec![mb1, mb2];
        messages.sort_by_key(|mb| mb.project_key);

        let actual = messages
            .into_iter()
            .map(|mb| (mb.project_key, mb.buckets))
            .collect::<Vec<_>>();

        assert_debug_snapshot!(actual, @r###"
        [
            (
                ProjectKey("11111111111111111111111111111111"),
                [
                    Bucket {
                        timestamp: UnixTimestamp(1615889440),
                        width: 0,
                        name: MetricName(
                            "d:transactions/endpoint.response_time@millisecond",
                        ),
                        value: Distribution(
                            [
                                68.0,
                            ],
                        ),
                        tags: {
                            "route": "user_index",
                        },
                        metadata: BucketMetadata {
                            merges: 1,
                            received_at: None,
                            extracted_from_indexed: false,
                        },
                    },
                ],
            ),
            (
                ProjectKey("22222222222222222222222222222222"),
                [
                    Bucket {
                        timestamp: UnixTimestamp(1615889440),
                        width: 0,
                        name: MetricName(
                            "d:transactions/endpoint.cache_rate@none",
                        ),
                        value: Distribution(
                            [
                                36.0,
                            ],
                        ),
                        tags: {},
                        metadata: BucketMetadata {
                            merges: 1,
                            received_at: None,
                            extracted_from_indexed: false,
                        },
                    },
                ],
            ),
        ]
        "###);
    }
}
