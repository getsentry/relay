use std::error::Error;
use std::io::Write;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use brotli::CompressorWriter as BrotliEncoder;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_common::time::UnixTimestamp;
use relay_config::{Config, HttpEncoding};
use relay_dynamic_config::{ErrorBoundary, Feature, GlobalConfig};
use relay_event_normalization::{
    ClockDriftProcessor, DynamicMeasurementsConfig, MeasurementsConfig, NormalizeProcessorConfig,
    TransactionNameConfig,
};
use relay_event_normalization::{GeoIpLookup, RawUserAgentInfo};
use relay_event_schema::processor::{self, process_value, ProcessingAction, ProcessingState};
use relay_event_schema::protocol::{
    Contexts, Event, EventType, IpAddr, Metrics, NetworkReportError, TraceContext,
};
use relay_filter::FilterStatKey;
use relay_metrics::aggregator::partition_buckets;
use relay_metrics::aggregator::AggregatorConfig;
use relay_metrics::{Bucket, BucketsView, MergeBuckets, MetricMeta, MetricNamespace};
use relay_pii::{scrub_graphql, PiiAttachmentsProcessor, PiiConfigError, PiiProcessor};
use relay_profiling::ProfileId;
use relay_protocol::{Annotated, Empty, Value};
use relay_quotas::{DataCategory, Scoping};
use relay_sampling::config::{RuleType, SamplingMode};
use relay_sampling::evaluation::{
    MatchedRuleIds, ReservoirCounters, ReservoirEvaluator, SamplingEvaluator,
};
use relay_sampling::{DynamicSamplingContext, SamplingConfig};
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse, Service};
use tokio::sync::Semaphore;

#[cfg(feature = "processing")]
use {
    crate::actors::project_cache::UpdateRateLimits,
    crate::utils::{EnvelopeLimiter, ItemAction, MetricsLimiter},
    relay_event_normalization::{span, StoreConfig, StoreProcessor},
    relay_event_schema::protocol::Span,
    relay_metrics::{Aggregator, RedisMetricMetaStore},
    relay_quotas::{RateLimitingError, RedisRateLimiter},
    relay_redis::RedisPool,
    symbolic_unreal::{Unreal4Error, Unreal4ErrorKind},
};

use crate::actors::envelopes::{EnvelopeManager, SendEnvelope, SendEnvelopeError, SubmitEnvelope};
use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::project::ProjectState;
use crate::actors::project_cache::{AddMetricMeta, ProjectCache};
use crate::actors::test_store::TestStore;
use crate::actors::upstream::{SendRequest, UpstreamRelay};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType, SourceQuantities};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::transactions::{ExtractedMetrics, TransactionExtractor};
use crate::service::ServiceError;
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{
    self, extract_transaction_count, ExtractionMode, ManagedEnvelope, SamplingResult,
};

mod event;
mod profile;
mod replay;
mod report;
mod session;

/// The minimum clock drift for correction to apply.
const MINIMUM_CLOCK_DRIFT: Duration = Duration::from_secs(55 * 60);

/// An error returned when handling [`ProcessEnvelope`].
#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("invalid json in event")]
    InvalidJson(#[source] serde_json::Error),

    #[error("invalid message pack event payload")]
    InvalidMsgpack(#[from] rmp_serde::decode::Error),

    #[cfg(feature = "processing")]
    #[error("invalid unreal crash report")]
    InvalidUnrealReport(#[source] Unreal4Error),

    #[error("event payload too large")]
    PayloadTooLarge,

    #[error("invalid transaction event")]
    InvalidTransaction,

    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),

    #[error("duplicate {0} in event")]
    DuplicateItem(ItemType),

    #[error("failed to extract event payload")]
    NoEventPayload,

    #[error("missing project id in DSN")]
    MissingProjectId,

    #[error("invalid security report type: {0:?}")]
    InvalidSecurityType(Bytes),

    #[error("invalid security report")]
    InvalidSecurityReport(#[source] serde_json::Error),

    #[error("invalid nel report")]
    InvalidNelReport(#[source] NetworkReportError),

    #[error("event filtered with reason: {0:?}")]
    EventFiltered(FilterStatKey),

    #[error("missing or invalid required event timestamp")]
    InvalidTimestamp,

    #[error("could not serialize event payload")]
    SerializeFailed(#[source] serde_json::Error),

    #[cfg(feature = "processing")]
    #[error("failed to apply quotas")]
    QuotasFailed(#[from] RateLimitingError),

    #[error("event dropped by sampling rule {0}")]
    Sampled(MatchedRuleIds),

    #[error("invalid pii config")]
    PiiConfigError(PiiConfigError),
}

impl ProcessingError {
    fn to_outcome(&self) -> Option<Outcome> {
        match *self {
            // General outcomes for invalid events
            Self::PayloadTooLarge => Some(Outcome::Invalid(DiscardReason::TooLarge)),
            Self::InvalidJson(_) => Some(Outcome::Invalid(DiscardReason::InvalidJson)),
            Self::InvalidMsgpack(_) => Some(Outcome::Invalid(DiscardReason::InvalidMsgpack)),
            Self::InvalidSecurityType(_) => {
                Some(Outcome::Invalid(DiscardReason::SecurityReportType))
            }
            Self::InvalidSecurityReport(_) => Some(Outcome::Invalid(DiscardReason::SecurityReport)),
            Self::InvalidNelReport(_) => Some(Outcome::Invalid(DiscardReason::InvalidJson)),
            Self::InvalidTransaction => Some(Outcome::Invalid(DiscardReason::InvalidTransaction)),
            Self::InvalidTimestamp => Some(Outcome::Invalid(DiscardReason::Timestamp)),
            Self::DuplicateItem(_) => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::NoEventPayload => Some(Outcome::Invalid(DiscardReason::NoEventPayload)),

            // Processing-only outcomes (Sentry-internal Relays)
            #[cfg(feature = "processing")]
            Self::InvalidUnrealReport(ref err)
                if err.kind() == Unreal4ErrorKind::BadCompression =>
            {
                Some(Outcome::Invalid(DiscardReason::InvalidCompression))
            }
            #[cfg(feature = "processing")]
            Self::InvalidUnrealReport(_) => Some(Outcome::Invalid(DiscardReason::ProcessUnreal)),

            // Internal errors
            Self::SerializeFailed(_) | Self::ProcessingFailed(_) => {
                Some(Outcome::Invalid(DiscardReason::Internal))
            }
            #[cfg(feature = "processing")]
            Self::QuotasFailed(_) => Some(Outcome::Invalid(DiscardReason::Internal)),
            Self::PiiConfigError(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),

            // These outcomes are emitted at the source.
            Self::MissingProjectId => None,
            Self::EventFiltered(_) => None,
            Self::Sampled(_) => None,
        }
    }

    fn is_unexpected(&self) -> bool {
        self.to_outcome()
            .map_or(false, |outcome| outcome.is_unexpected())
    }

    fn should_keep_metrics(&self) -> bool {
        matches!(self, Self::Sampled(_))
    }
}

#[cfg(feature = "processing")]
impl From<Unreal4Error> for ProcessingError {
    fn from(err: Unreal4Error) -> Self {
        match err.kind() {
            Unreal4ErrorKind::TooLarge => Self::PayloadTooLarge,
            _ => ProcessingError::InvalidUnrealReport(err),
        }
    }
}

impl From<ExtractMetricsError> for ProcessingError {
    fn from(error: ExtractMetricsError) -> Self {
        match error {
            ExtractMetricsError::MissingTimestamp | ExtractMetricsError::InvalidTimestamp => {
                Self::InvalidTimestamp
            }
        }
    }
}

type ExtractedEvent = (Annotated<Event>, usize);

/// Checks if the Event includes unprintable fields.
#[cfg(feature = "processing")]
fn has_unprintable_fields(event: &Annotated<Event>) -> bool {
    fn is_unprintable(value: &&str) -> bool {
        value.chars().any(|c| {
            c == '\u{fffd}' // unicode replacement character
                || (c.is_control() && !c.is_whitespace()) // non-whitespace control characters
        })
    }
    if let Some(event) = event.value() {
        let env = event.environment.as_str().filter(is_unprintable);
        let release = event.release.as_str().filter(is_unprintable);
        env.is_some() || release.is_some()
    } else {
        false
    }
}

impl ExtractedMetrics {
    // TODO(ja): Move
    fn send_metrics(self, envelope: &Envelope, project_cache: Addr<ProjectCache>) {
        let project_key = envelope.meta().public_key();

        if !self.project_metrics.is_empty() {
            project_cache.send(MergeBuckets::new(project_key, self.project_metrics));
        }

        if !self.sampling_metrics.is_empty() {
            // If no sampling project state is available, we associate the sampling
            // metrics with the current project.
            //
            // project_without_tracing         -> metrics goes to self
            // dependent_project_with_tracing  -> metrics goes to root
            // root_project_with_tracing       -> metrics goes to root == self
            let sampling_project_key = utils::get_sampling_key(envelope).unwrap_or(project_key);
            project_cache.send(MergeBuckets::new(
                sampling_project_key,
                self.sampling_metrics,
            ));
        }
    }
}

/// A state container for envelope processing.
#[derive(Debug)]
struct ProcessEnvelopeState<'a> {
    /// The extracted event payload.
    ///
    /// For Envelopes without event payloads, this contains `Annotated::empty`. If a single item has
    /// `creates_event`, the event is required and the pipeline errors if no payload can be
    /// extracted.
    event: Annotated<Event>,

    /// Track whether transaction metrics were already extracted.
    event_metrics_extracted: bool,

    /// Partial metrics of the Event during construction.
    ///
    /// The pipeline stages can add to this metrics objects. In `finalize_event`, the metrics are
    /// persisted into the Event. All modifications afterwards will have no effect.
    metrics: Metrics,

    /// A list of cumulative sample rates applied to this event.
    ///
    /// This element is obtained from the event or transaction item and re-serialized into the
    /// resulting item.
    sample_rates: Option<Value>,

    /// The result of a dynamic sampling operation on this envelope.
    ///
    /// The event will be kept if there's either no match, or there's a match and it was sampled.
    sampling_result: SamplingResult,

    /// Metrics extracted from items in the envelope.
    ///
    /// Relay can extract metrics for sessions and transactions, which is controlled by
    /// configuration objects in the project config.
    extracted_metrics: ExtractedMetrics,

    /// The state of the project that this envelope belongs to.
    project_state: Arc<ProjectState>,

    /// The state of the project that initiated the current trace.
    /// This is the config used for trace-based dynamic sampling.
    sampling_project_state: Option<Arc<ProjectState>>,

    /// The id of the project that this envelope is ingested into.
    ///
    /// This identifier can differ from the one stated in the Envelope's DSN if the key was moved to
    /// a new project or on the legacy endpoint. In that case, normalization will update the project
    /// ID.
    project_id: ProjectId,

    /// The managed envelope before processing.
    managed_envelope: ManagedEnvelope,

    /// The ID of the profile in the envelope, if a valid profile exists.
    profile_id: Option<ProfileId>,

    /// Reservoir evaluator that we use for dynamic sampling.
    reservoir: ReservoirEvaluator<'a>,

    /// Global config used for envelope processing.
    global_config: Arc<GlobalConfig>,
}

impl<'a> ProcessEnvelopeState<'a> {
    /// Returns a reference to the contained [`Envelope`].
    fn envelope(&self) -> &Envelope {
        self.managed_envelope.envelope()
    }

    /// Returns a mutable reference to the contained [`Envelope`].
    fn envelope_mut(&mut self) -> &mut Envelope {
        self.managed_envelope.envelope_mut()
    }

    /// Returns whether any item in the envelope creates an event in any relay.
    ///
    /// This is used to branch into the processing pipeline. If this function returns false, only
    /// rate limits are executed. If this function returns true, an event is created either in the
    /// current relay or in an upstream processing relay.
    fn creates_event(&self) -> bool {
        self.envelope().items().any(Item::creates_event)
    }

    /// Returns true if there is an event in the processing state.
    ///
    /// The event was previously removed from the Envelope. This returns false if there was an
    /// invalid event item.
    fn has_event(&self) -> bool {
        self.event.value().is_some()
    }

    /// Returns the event type if there is an event.
    ///
    /// If the event does not have a type, `Some(EventType::Default)` is assumed. If, in contrast, there
    /// is no event, `None` is returned.
    fn event_type(&self) -> Option<EventType> {
        self.event
            .value()
            .map(|event| event.ty.value().copied().unwrap_or_default())
    }

    /// Returns the data category if there is an event.
    ///
    /// The data category is computed from the event type. Both `Default` and `Error` events map to
    /// the `Error` data category. If there is no Event, `None` is returned.
    fn event_category(&self) -> Option<DataCategory> {
        self.event_type().map(DataCategory::from)
    }

    /// Removes the event payload from this processing state.
    #[cfg(feature = "processing")]
    fn remove_event(&mut self) {
        self.event = Annotated::empty();
    }
}

/// Response of the [`ProcessEnvelope`] message.
#[cfg_attr(not(feature = "processing"), allow(dead_code))]
pub struct ProcessEnvelopeResponse {
    /// The processed envelope.
    ///
    /// This is `Some` if the envelope passed inbound filtering and rate limiting. Invalid items are
    /// removed from the envelope. Otherwise, if the envelope is empty or the entire envelope needs
    /// to be dropped, this is `None`.
    pub envelope: Option<ManagedEnvelope>,
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
    pub envelope: ManagedEnvelope,
    pub project_state: Arc<ProjectState>,
    pub sampling_project_state: Option<Arc<ProjectState>>,
    pub reservoir_counters: ReservoirCounters,
    pub global_config: Arc<GlobalConfig>,
}

/// Parses a list of metrics or metric buckets and pushes them to the project's aggregator.
///
/// This parses and validates the metrics:
///  - For [`Metrics`](ItemType::Statsd), each metric is parsed separately, and invalid metrics are
///    ignored independently.
///  - For [`MetricBuckets`](ItemType::MetricBuckets), the entire list of buckets is parsed and
///    dropped together on parsing failure.
///  - Other items will be ignored with an error message.
///
/// Additionally, processing applies clock drift correction using the system clock of this Relay, if
/// the Envelope specifies the [`sent_at`](Envelope::sent_at) header.
#[derive(Debug)]
pub struct ProcessMetrics {
    /// A list of metric items.
    pub items: Vec<Item>,

    /// The target project.
    pub project_key: ProjectKey,

    /// The instant at which the request was received.
    pub start_time: Instant,

    /// The value of the Envelope's [`sent_at`](Envelope::sent_at) header for clock drift
    /// correction.
    pub sent_at: Option<DateTime<Utc>>,
}

/// Parses a list of metric meta items and pushes them to the project cache for aggregation.
#[derive(Debug)]
pub struct ProcessMetricMeta {
    /// A list of metric meta items.
    pub items: Vec<Item>,
    /// The target project.
    pub project_key: ProjectKey,
}

/// Applies HTTP content encoding to an envelope's payload.
///
/// This message is a workaround for a single-threaded upstream service.
#[derive(Debug)]
pub struct EncodeEnvelope {
    request: SendEnvelope,
}

impl EncodeEnvelope {
    /// Creates a new `EncodeEnvelope` message from `SendEnvelope` request.
    pub fn new(request: SendEnvelope) -> Self {
        Self { request }
    }
}

/// Encodes metrics into an envelope ready to be sent upstream.
#[derive(Debug)]
pub struct EncodeMetrics {
    /// The metric buckets to encode.
    pub buckets: Vec<Bucket>,
    /// Scoping for metric buckets.
    pub scoping: Scoping,
    /// Transaction metrics extraction mode.
    pub extraction_mode: ExtractionMode,
}

/// Encodes metric meta into an envelope and sends it upstream.
///
/// Upstream means directly into redis for processing relays
/// and otherwise submitting the envelope with the envelope manager.
#[derive(Debug)]
pub struct EncodeMetricMeta {
    /// Scoping of the meta.
    pub scoping: Scoping,
    /// The metric meta.
    pub meta: MetricMeta,
}

/// Applies rate limits to metrics buckets and forwards them to the envelope manager.
#[cfg(feature = "processing")]
#[derive(Debug)]
pub struct RateLimitBuckets {
    pub bucket_limiter: MetricsLimiter,
}

/// CPU-intensive processing tasks for envelopes.
#[derive(Debug)]
pub enum EnvelopeProcessor {
    ProcessEnvelope(Box<ProcessEnvelope>),
    ProcessMetrics(Box<ProcessMetrics>),
    ProcessMetricMeta(Box<ProcessMetricMeta>),
    EncodeEnvelope(Box<EncodeEnvelope>),
    EncodeMetrics(Box<EncodeMetrics>),
    EncodeMetricMeta(Box<EncodeMetricMeta>),
    #[cfg(feature = "processing")]
    RateLimitBuckets(RateLimitBuckets),
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
        Self::ProcessMetrics(Box::new(message))
    }
}

impl FromMessage<ProcessMetricMeta> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: ProcessMetricMeta, _: ()) -> Self {
        Self::ProcessMetricMeta(Box::new(message))
    }
}

impl FromMessage<EncodeEnvelope> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: EncodeEnvelope, _: ()) -> Self {
        Self::EncodeEnvelope(Box::new(message))
    }
}

impl FromMessage<EncodeMetrics> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: EncodeMetrics, _: ()) -> Self {
        Self::EncodeMetrics(Box::new(message))
    }
}

impl FromMessage<EncodeMetricMeta> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: EncodeMetricMeta, _: ()) -> Self {
        Self::EncodeMetricMeta(Box::new(message))
    }
}

#[cfg(feature = "processing")]
impl FromMessage<RateLimitBuckets> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: RateLimitBuckets, _: ()) -> Self {
        Self::RateLimitBuckets(message)
    }
}

/// Service implementing the [`EnvelopeProcessor`] interface.
///
/// This service handles messages in a worker pool with configurable concurrency.
#[derive(Clone)]
pub struct EnvelopeProcessorService {
    inner: Arc<InnerProcessor>,
}

struct InnerProcessor {
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    redis_pool: Option<RedisPool>,
    envelope_manager: Addr<EnvelopeManager>,
    project_cache: Addr<ProjectCache>,
    outcome_aggregator: Addr<TrackOutcome>,
    #[cfg(feature = "processing")]
    aggregator: Addr<Aggregator>,
    upstream_relay: Addr<UpstreamRelay>,
    test_store: Addr<TestStore>,
    #[cfg(feature = "processing")]
    rate_limiter: Option<RedisRateLimiter>,
    geoip_lookup: Option<GeoIpLookup>,
    #[cfg(feature = "processing")]
    metric_meta_store: Option<RedisMetricMetaStore>,
}

impl EnvelopeProcessorService {
    /// Creates a multi-threaded envelope processor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        #[cfg(feature = "processing")] redis: Option<RedisPool>,
        envelope_manager: Addr<EnvelopeManager>,
        outcome_aggregator: Addr<TrackOutcome>,
        project_cache: Addr<ProjectCache>,
        upstream_relay: Addr<UpstreamRelay>,
        test_store: Addr<TestStore>,
        #[cfg(feature = "processing")] aggregator: Addr<Aggregator>,
    ) -> Self {
        let geoip_lookup = config.geoip_path().and_then(|p| {
            match GeoIpLookup::open(p).context(ServiceError::GeoIp) {
                Ok(geoip) => Some(geoip),
                Err(err) => {
                    relay_log::error!("failed to open GeoIP db {p:?}: {err:?}");
                    None
                }
            }
        });

        let inner = InnerProcessor {
            #[cfg(feature = "processing")]
            redis_pool: redis.clone(),
            #[cfg(feature = "processing")]
            rate_limiter: redis
                .clone()
                .map(|pool| RedisRateLimiter::new(pool).max_limit(config.max_rate_limit())),
            envelope_manager,
            project_cache,
            outcome_aggregator,
            upstream_relay,
            test_store,
            geoip_lookup,
            #[cfg(feature = "processing")]
            aggregator,
            #[cfg(feature = "processing")]
            metric_meta_store: redis.clone().map(|pool| {
                RedisMetricMetaStore::new(pool, config.metrics_meta_locations_expiry())
            }),
            config,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Normalize monitor check-ins and remove invalid ones.
    #[cfg(feature = "processing")]
    fn process_check_ins(&self, state: &mut ProcessEnvelopeState) {
        state.managed_envelope.retain_items(|item| {
            if item.ty() != &ItemType::CheckIn {
                return ItemAction::Keep;
            }

            match relay_monitors::process_check_in(&item.payload(), state.project_id) {
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

    /// Creates and initializes the processing state.
    ///
    /// This applies defaults to the envelope and initializes empty rate limits.
    fn prepare_state(
        &self,
        message: ProcessEnvelope,
    ) -> Result<ProcessEnvelopeState, ProcessingError> {
        let ProcessEnvelope {
            envelope: mut managed_envelope,
            project_state,
            sampling_project_state,
            reservoir_counters,
            global_config,
        } = message;

        let envelope = managed_envelope.envelope_mut();

        // Set the event retention. Effectively, this value will only be available in processing
        // mode when the full project config is queried from the upstream.
        if let Some(retention) = project_state.config.event_retention {
            envelope.set_retention(retention);
        }

        // Prefer the project's project ID, and fall back to the stated project id from the
        // envelope. The project ID is available in all modes, other than in proxy mode, where
        // envelopes for unknown projects are forwarded blindly.
        //
        // Neither ID can be available in proxy mode on the /store/ endpoint. This is not supported,
        // since we cannot process an envelope without project ID, so drop it.
        let project_id = match project_state
            .project_id
            .or_else(|| envelope.meta().project_id())
        {
            Some(project_id) => project_id,
            None => {
                managed_envelope.reject(Outcome::Invalid(DiscardReason::Internal));
                return Err(ProcessingError::MissingProjectId);
            }
        };

        // Ensure the project ID is updated to the stored instance for this project cache. This can
        // differ in two cases:
        //  1. The envelope was sent to the legacy `/store/` endpoint without a project ID.
        //  2. The DSN was moved and the envelope sent to the old project ID.
        envelope.meta_mut().set_project_id(project_id);

        #[allow(unused_mut)]
        let mut reservoir = ReservoirEvaluator::new(reservoir_counters);
        #[cfg(feature = "processing")]
        if let Some(redis_pool) = self.inner.redis_pool.as_ref() {
            let org_id = managed_envelope.scoping().organization_id;
            reservoir.set_redis(org_id, redis_pool);
        }

        Ok(ProcessEnvelopeState {
            event: Annotated::empty(),
            event_metrics_extracted: false,
            metrics: Metrics::default(),
            sample_rates: None,
            sampling_result: SamplingResult::Pending,
            extracted_metrics: Default::default(),
            project_state,
            sampling_project_state,
            project_id,
            managed_envelope,
            profile_id: None,
            reservoir,
            global_config,
        })
    }

    /// Expands Unreal 4 items inside an envelope.
    ///
    /// If the envelope does NOT contain an `UnrealReport` item, it doesn't do anything. If the
    /// envelope contains an `UnrealReport` item, it removes it from the envelope and inserts new
    /// items for each of its contents.
    ///
    /// The envelope may be dropped if it exceeds size limits after decompression. Particularly,
    /// this includes cases where a single attachment file exceeds the maximum file size. This is in
    /// line with the behavior of the envelope endpoint.
    ///
    /// After this, [`EnvelopeProcessorService`] should be able to process the envelope the same
    /// way it processes any other envelopes.
    #[cfg(feature = "processing")]
    fn expand_unreal(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let envelope = &mut state.envelope_mut();

        if let Some(item) = envelope.take_item_by(|item| item.ty() == &ItemType::UnrealReport) {
            utils::expand_unreal_envelope(item, envelope, &self.inner.config)?;
        }

        Ok(())
    }

    /// Extracts event information from an unreal context.
    ///
    /// If the event does not contain an unreal context, this function does not perform any action.
    /// If there was no event payload prior to this function, it is created.
    #[cfg(feature = "processing")]
    fn process_unreal(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        utils::process_unreal_envelope(&mut state.event, state.managed_envelope.envelope_mut())
            .map_err(ProcessingError::InvalidUnrealReport)
    }

    /// Adds processing placeholders for special attachments.
    ///
    /// If special attachments are present in the envelope, this adds placeholder payloads to the
    /// event. This indicates to the pipeline that the event needs special processing.
    ///
    /// If the event payload was empty before, it is created.
    #[cfg(feature = "processing")]
    fn create_placeholders(&self, state: &mut ProcessEnvelopeState) {
        let envelope = state.managed_envelope.envelope();
        let minidump_attachment =
            envelope.get_item_by(|item| item.attachment_type() == Some(&AttachmentType::Minidump));
        let apple_crash_report_attachment = envelope
            .get_item_by(|item| item.attachment_type() == Some(&AttachmentType::AppleCrashReport));

        if let Some(item) = minidump_attachment {
            let event = state.event.get_or_insert_with(Event::default);
            state.metrics.bytes_ingested_event_minidump = Annotated::new(item.len() as u64);
            utils::process_minidump(event, &item.payload());
        } else if let Some(item) = apple_crash_report_attachment {
            let event = state.event.get_or_insert_with(Event::default);
            state.metrics.bytes_ingested_event_applecrashreport = Annotated::new(item.len() as u64);
            utils::process_apple_crash_report(event, &item.payload());
        }
    }

    #[cfg(feature = "processing")]
    fn store_process_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let ProcessEnvelopeState {
            ref mut event,
            ref project_state,
            ref managed_envelope,
            ..
        } = *state;

        let key_id = project_state
            .get_public_key_config()
            .and_then(|k| Some(k.numeric_id?.to_string()));

        let envelope = state.managed_envelope.envelope();

        if key_id.is_none() {
            relay_log::error!(
                "project state for key {} is missing key id",
                envelope.meta().public_key()
            );
        }

        let store_config = StoreConfig {
            project_id: Some(state.project_id.value()),
            client_ip: envelope.meta().client_addr().map(IpAddr::from),
            client: envelope.meta().client().map(str::to_owned),
            key_id,
            protocol_version: Some(envelope.meta().version().to_string()),
            grouping_config: project_state.config.grouping_config.clone(),
            user_agent: envelope.meta().user_agent().map(str::to_owned),
            max_secs_in_future: Some(self.inner.config.max_secs_in_future()),
            max_secs_in_past: Some(self.inner.config.max_secs_in_past()),
            enable_trimming: Some(true),
            is_renormalize: Some(false),
            remove_other: Some(true),
            normalize_user_agent: Some(true),
            sent_at: envelope.sent_at(),
            received_at: Some(managed_envelope.received_at()),
            breakdowns: project_state.config.breakdowns_v2.clone(),
            span_attributes: project_state.config.span_attributes.clone(),
            client_sample_rate: envelope.dsc().and_then(|ctx| ctx.sample_rate),
            replay_id: envelope.dsc().and_then(|ctx| ctx.replay_id),
            client_hints: envelope.meta().client_hints().to_owned(),
        };

        let mut store_processor =
            StoreProcessor::new(store_config, self.inner.geoip_lookup.as_ref());
        metric!(timer(RelayTimers::EventProcessingProcess), {
            processor::process_value(event, &mut store_processor, ProcessingState::root())
                .map_err(|_| ProcessingError::InvalidTransaction)?;
            if has_unprintable_fields(event) {
                metric!(counter(RelayCounters::EventCorrupted) += 1);
            }
        });

        Ok(())
    }

    /// Ensures there is a valid dynamic sampling context and corresponding project state.
    ///
    /// The dynamic sampling context (DSC) specifies the project_key of the project that initiated
    /// the trace. That project state should have been loaded previously by the project cache and is
    /// available on the `ProcessEnvelopeState`. Under these conditions, this cannot happen:
    ///
    ///  - There is no DSC in the envelope headers. This occurs with older or third-party SDKs.
    ///  - The project key does not exist. This can happen if the project key was disabled, the
    ///    project removed, or in rare cases when a project from another Sentry instance is referred
    ///    to.
    ///  - The project key refers to a project from another organization. In this case the project
    ///    cache does not resolve the state and instead leaves it blank.
    ///  - The project state could not be fetched. This is a runtime error, but in this case Relay
    ///    should fall back to the next-best sampling rule set.
    ///
    /// In all of the above cases, this function will compute a new DSC using information from the
    /// event payload, similar to how SDKs do this. The `sampling_project_state` is also switched to
    /// the main project state.
    ///
    /// If there is no transaction event in the envelope, this function will do nothing.
    fn normalize_dsc(&self, state: &mut ProcessEnvelopeState) {
        if state.envelope().dsc().is_some() && state.sampling_project_state.is_some() {
            return;
        }

        // The DSC can only be computed if there's a transaction event. Note that `from_transaction`
        // below already checks for the event type.
        let Some(event) = state.event.value() else {
            return;
        };
        let Some(key_config) = state.project_state.get_public_key_config() else {
            return;
        };

        if let Some(dsc) = utils::dsc_from_event(key_config.public_key, event) {
            state.envelope_mut().set_dsc(dsc);
            state.sampling_project_state = Some(state.project_state.clone());
        }
    }

    fn filter_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let event = match state.event.value_mut() {
            Some(event) => event,
            // Some events are created by processing relays (e.g. unreal), so they do not yet
            // exist at this point in non-processing relays.
            None => return Ok(()),
        };

        let client_ip = state.managed_envelope.envelope().meta().client_addr();
        let filter_settings = &state.project_state.config.filter_settings;

        metric!(timer(RelayTimers::EventProcessingFiltering), {
            relay_filter::should_filter(event, client_ip, filter_settings).map_err(|err| {
                state
                    .managed_envelope
                    .reject(Outcome::Filtered(err.clone()));
                ProcessingError::EventFiltered(err)
            })
        })
    }

    #[cfg(feature = "processing")]
    fn enforce_quotas(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let rate_limiter = match self.inner.rate_limiter.as_ref() {
            Some(rate_limiter) => rate_limiter,
            None => return Ok(()),
        };

        let project_state = &state.project_state;
        let quotas = project_state.config.quotas.as_slice();
        if quotas.is_empty() {
            return Ok(());
        }

        let event_category = state.event_category();

        // When invoking the rate limiter, capture if the event item has been rate limited to also
        // remove it from the processing state eventually.
        let mut envelope_limiter =
            EnvelopeLimiter::new(Some(&project_state.config), |item_scope, quantity| {
                rate_limiter.is_rate_limited(quotas, item_scope, quantity, false)
            });

        // Tell the envelope limiter about the event, since it has been removed from the Envelope at
        // this stage in processing.
        if let Some(category) = event_category {
            envelope_limiter.assume_event(category, state.event_metrics_extracted);
        }

        let scoping = state.managed_envelope.scoping();
        let (enforcement, limits) = metric!(timer(RelayTimers::EventProcessingRateLimiting), {
            envelope_limiter.enforce(state.managed_envelope.envelope_mut(), &scoping)?
        });

        if limits.is_limited() {
            self.inner
                .project_cache
                .send(UpdateRateLimits::new(scoping.project_key, limits));
        }

        if enforcement.event_active() {
            state.remove_event();
            debug_assert!(state.envelope().is_empty());
        }

        enforcement.track_outcomes(
            state.envelope(),
            &state.managed_envelope.scoping(),
            self.inner.outcome_aggregator.clone(),
        );

        Ok(())
    }

    /// Extract metrics from all envelope items.
    ///
    /// Caveats:
    ///  - This functionality is incomplete. At this point, extraction is implemented only for
    ///    transaction events.
    fn extract_metrics(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        // NOTE: This function requires a `metric_extraction` in the project config. Legacy configs
        // will upsert this configuration from transaction and conditional tagging fields, even if
        // it is not present in the actual project config payload. Once transaction metric
        // extraction is moved to generic metrics, this can be converted into an early return.
        let config = match state.project_state.config.metric_extraction {
            ErrorBoundary::Ok(ref config) if config.is_enabled() => Some(config),
            _ => None,
        };

        // TODO: Make span metrics extraction immutable
        if let Some(event) = state.event.value() {
            if state.event_metrics_extracted {
                return Ok(());
            }

            if let Some(config) = config {
                let metrics = crate::metrics_extraction::event::extract_metrics(event, config);
                state.event_metrics_extracted |= !metrics.is_empty();
                state.extracted_metrics.project_metrics.extend(metrics);
            }

            match state.project_state.config.transaction_metrics {
                Some(ErrorBoundary::Ok(ref tx_config)) if tx_config.is_enabled() => {
                    let transaction_from_dsc = state
                        .managed_envelope
                        .envelope()
                        .dsc()
                        .and_then(|dsc| dsc.transaction.as_deref());

                    let extractor = TransactionExtractor {
                        config: tx_config,
                        generic_tags: config.map(|c| c.tags.as_slice()).unwrap_or_default(),
                        transaction_from_dsc,
                        sampling_result: &state.sampling_result,
                        has_profile: state.profile_id.is_some(),
                    };

                    state.extracted_metrics.extend(extractor.extract(event)?);
                    state.event_metrics_extracted |= true;
                }
                _ => (),
            }

            if state.event_metrics_extracted {
                state.managed_envelope.set_event_metrics_extracted();
            }
        }

        // NB: Other items can be added here.
        Ok(())
    }

    /// Apply data privacy rules to the event payload.
    ///
    /// This uses both the general `datascrubbing_settings`, as well as the the PII rules.
    fn scrub_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let event = &mut state.event;
        let config = &state.project_state.config;

        if config.datascrubbing_settings.scrub_data {
            if let Some(event) = event.value_mut() {
                scrub_graphql(event);
            }
        }

        metric!(timer(RelayTimers::EventProcessingPii), {
            if let Some(ref config) = config.pii_config {
                let mut processor = PiiProcessor::new(config.compiled());
                processor::process_value(event, &mut processor, ProcessingState::root())?;
            }
            let pii_config = config
                .datascrubbing_settings
                .pii_config()
                .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;
            if let Some(config) = pii_config {
                let mut processor = PiiProcessor::new(config.compiled());
                processor::process_value(event, &mut processor, ProcessingState::root())?;
            }
        });

        Ok(())
    }

    /// Apply data privacy rules to attachments in the envelope.
    ///
    /// This only applies the new PII rules that explicitly select `ValueType::Binary` or one of the
    /// attachment types. When special attachments are detected, these are scrubbed with custom
    /// logic; otherwise the entire attachment is treated as a single binary blob.
    fn scrub_attachments(&self, state: &mut ProcessEnvelopeState) {
        let envelope = state.managed_envelope.envelope_mut();
        if let Some(ref config) = state.project_state.config.pii_config {
            let minidump = envelope
                .get_item_by_mut(|item| item.attachment_type() == Some(&AttachmentType::Minidump));

            if let Some(item) = minidump {
                let filename = item.filename().unwrap_or_default();
                let mut payload = item.payload().to_vec();

                let processor = PiiAttachmentsProcessor::new(config.compiled());

                // Minidump scrubbing can fail if the minidump cannot be parsed. In this case, we
                // must be conservative and treat it as a plain attachment. Under extreme
                // conditions, this could destroy stack memory.
                let start = Instant::now();
                match processor.scrub_minidump(filename, &mut payload) {
                    Ok(modified) => {
                        metric!(
                            timer(RelayTimers::MinidumpScrubbing) = start.elapsed(),
                            status = if modified { "ok" } else { "n/a" },
                        );
                    }
                    Err(scrub_error) => {
                        metric!(
                            timer(RelayTimers::MinidumpScrubbing) = start.elapsed(),
                            status = "error"
                        );
                        relay_log::warn!(
                            error = &scrub_error as &dyn Error,
                            "failed to scrub minidump",
                        );
                        metric!(timer(RelayTimers::AttachmentScrubbing), {
                            processor.scrub_attachment(filename, &mut payload);
                        })
                    }
                }

                let content_type = item
                    .content_type()
                    .unwrap_or(&ContentType::Minidump)
                    .clone();

                item.set_payload(content_type, payload);
            }
        }
    }

    fn serialize_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let data = metric!(timer(RelayTimers::EventProcessingSerialization), {
            state
                .event
                .to_json()
                .map_err(ProcessingError::SerializeFailed)?
        });

        let event_type = state.event_type().unwrap_or_default();
        let mut event_item = Item::new(ItemType::from_event_type(event_type));
        event_item.set_payload(ContentType::Json, data);

        // If transaction metrics were extracted, set the corresponding item header
        event_item.set_metrics_extracted(state.event_metrics_extracted);

        // If there are sample rates, write them back to the envelope. In processing mode, sample
        // rates have been removed from the state and burnt into the event via `finalize_event`.
        if let Some(sample_rates) = state.sample_rates.take() {
            event_item.set_sample_rates(sample_rates);
        }

        state.envelope_mut().add_item(event_item);

        Ok(())
    }

    #[cfg(feature = "processing")]
    fn is_span_allowed(&self, span: &Span) -> bool {
        let Some(op) = span.op.value() else {
            return false;
        };
        let Some(description) = span.description.value() else {
            return false;
        };
        let system: &str = span
            .data
            .value()
            .and_then(|v| v.get("span.system"))
            .and_then(|system| system.as_str())
            .unwrap_or_default();
        op.contains("resource.script")
            || op.contains("resource.css")
            || op == "http.client"
            || op.starts_with("app.")
            || op.starts_with("ui.load")
            || op.starts_with("file")
            || op.starts_with("db")
                && !(op.contains("clickhouse")
                    || op.contains("mongodb")
                    || op.contains("redis")
                    || op.contains("compiler"))
                && !(op == "db.sql.query" && (description.contains("\"$") || system == "mongodb"))
    }

    #[cfg(feature = "processing")]
    fn extract_spans(&self, state: &mut ProcessEnvelopeState) {
        // For now, drop any spans submitted by the SDK.
        state.managed_envelope.retain_items(|item| match item.ty() {
            ItemType::Span => ItemAction::DropSilently,
            _ => ItemAction::Keep,
        });

        // Only extract spans from transactions (not errors).
        if state.event_type() != Some(EventType::Transaction) {
            return;
        };

        // Check feature flag.
        if !state
            .project_state
            .has_feature(Feature::SpanMetricsExtraction)
        {
            return;
        };

        let mut add_span = |span: Annotated<Span>| {
            let span = match self.validate_span(span) {
                Ok(span) => span,
                Err(e) => {
                    relay_log::error!("Invalid span: {e}");
                    return;
                }
            };
            let span = match span.to_json() {
                Ok(span) => span,
                Err(e) => {
                    relay_log::error!(error = &e as &dyn Error, "Failed to serialize span");
                    return;
                }
            };
            let mut item = Item::new(ItemType::Span);
            item.set_payload(ContentType::Json, span);
            state.managed_envelope.envelope_mut().add_item(item);
        };

        let Some(event) = state.event.value() else {
            return;
        };

        // Extract transaction as a span.
        let mut transaction_span: Span = event.into();

        let all_modules_enabled = state
            .project_state
            .has_feature(Feature::SpanMetricsExtractionAllModules);

        // Add child spans as envelope items.
        if let Some(child_spans) = event.spans.value() {
            for span in child_spans {
                let Some(inner_span) = span.value() else {
                    continue;
                };
                // HACK: filter spans based on module until we figure out grouping.
                if !all_modules_enabled && !self.is_span_allowed(inner_span) {
                    continue;
                }
                // HACK: clone the span to set the segment_id. This should happen
                // as part of normalization once standalone spans reach wider adoption.
                let mut new_span = inner_span.clone();
                new_span.is_segment = Annotated::new(false);
                new_span.received = transaction_span.received.clone();
                new_span.segment_id = transaction_span.segment_id.clone();

                // If a profile is associated with the transaction, also associate it with its
                // child spans.
                new_span.profile_id = transaction_span.profile_id.clone();

                add_span(Annotated::new(new_span));
            }
        }

        // Extract tags to add to this span as well
        let shared_tags = span::tag_extraction::extract_shared_tags(event);
        transaction_span.sentry_tags = Annotated::new(
            shared_tags
                .clone()
                .into_iter()
                .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
                .collect(),
        );
        add_span(transaction_span.into());
    }

    /// Helper for [`Self::extract_spans`].
    ///
    /// We do not extract spans with missing fields if those fields are required on the Kafka topic.
    #[cfg(feature = "processing")]
    fn validate_span(&self, mut span: Annotated<Span>) -> Result<Annotated<Span>, anyhow::Error> {
        let inner = span
            .value_mut()
            .as_mut()
            .ok_or(anyhow::anyhow!("empty span"))?;
        let Span {
            ref exclusive_time,
            ref mut tags,
            ref mut sentry_tags,
            ..
        } = inner;
        // The following required fields are already validated by the `TransactionsProcessor`:
        // - `timestamp`
        // - `start_timestamp`
        // - `trace_id`
        // - `span_id`
        //
        // `is_segment` is set by `extract_span`.
        exclusive_time
            .value()
            .ok_or(anyhow::anyhow!("missing exclusive_time"))?;

        if let Some(sentry_tags) = sentry_tags.value_mut() {
            sentry_tags.retain(|key, value| match value.value() {
                Some(s) => {
                    match key.as_str() {
                        "group" => {
                            // Only allow up to 16-char hex strings in group.
                            s.len() <= 16 && s.chars().all(|c| c.is_ascii_hexdigit())
                        }
                        "status_code" => s.parse::<u16>().is_ok(),
                        _ => true,
                    }
                }
                // Drop empty string values.
                None => false,
            });
        }
        if let Some(tags) = tags.value_mut() {
            tags.retain(|_, value| !value.value().is_empty())
        }

        Ok(span)
    }

    /// Computes the sampling decision on the incoming event
    fn run_dynamic_sampling(&self, state: &mut ProcessEnvelopeState) {
        // Running dynamic sampling involves either:
        // - Tagging whether an incoming error has a sampled trace connected to it.
        // - Computing the actual sampling decision on an incoming transaction.
        match state.event_type().unwrap_or_default() {
            EventType::Default | EventType::Error => {
                self.tag_error_with_sampling_decision(state);
            }
            EventType::Transaction => {
                match state.project_state.config.transaction_metrics {
                    Some(ErrorBoundary::Ok(ref c)) if c.is_enabled() => (),
                    _ => return,
                }

                let sampling_config = match state.project_state.config.sampling {
                    Some(ErrorBoundary::Ok(ref config)) if !config.unsupported() => Some(config),
                    _ => None,
                };

                let root_state = state.sampling_project_state.as_ref();
                let root_config = match root_state.and_then(|s| s.config.sampling.as_ref()) {
                    Some(ErrorBoundary::Ok(ref config)) if !config.unsupported() => Some(config),
                    _ => None,
                };

                state.sampling_result = Self::compute_sampling_decision(
                    self.inner.config.processing_enabled(),
                    &state.reservoir,
                    sampling_config,
                    state.event.value(),
                    root_config,
                    state.envelope().dsc(),
                );
            }
            _ => {}
        }
    }

    /// Computes the sampling decision on the incoming transaction.
    fn compute_sampling_decision(
        processing_enabled: bool,
        reservoir: &ReservoirEvaluator,
        sampling_config: Option<&SamplingConfig>,
        event: Option<&Event>,
        root_sampling_config: Option<&SamplingConfig>,
        dsc: Option<&DynamicSamplingContext>,
    ) -> SamplingResult {
        if (sampling_config.is_none() || event.is_none())
            && (root_sampling_config.is_none() || dsc.is_none())
        {
            return SamplingResult::NoMatch;
        }

        if sampling_config.map_or(false, |config| config.unsupported())
            || root_sampling_config.map_or(false, |config| config.unsupported())
        {
            if processing_enabled {
                relay_log::error!("found unsupported rules even as processing relay");
            } else {
                return SamplingResult::NoMatch;
            }
        }

        let adjustment_rate = match sampling_config
            .or(root_sampling_config)
            .map(|config| config.mode)
        {
            Some(SamplingMode::Received) => None,
            Some(SamplingMode::Total) => dsc.and_then(|dsc| dsc.sample_rate),
            Some(SamplingMode::Unsupported) => {
                if processing_enabled {
                    relay_log::error!("found unsupported sampling mode even as processing Relay");
                }
                return SamplingResult::NoMatch;
            }
            None => {
                relay_log::error!("cannot sample without at least one sampling config");
                return SamplingResult::NoMatch;
            }
        };

        let mut evaluator = SamplingEvaluator::new(Utc::now())
            .adjust_client_sample_rate(adjustment_rate)
            .set_reservoir(reservoir);

        if let (Some(event), Some(sampling_state)) = (event, sampling_config) {
            if let Some(seed) = event.id.value().map(|id| id.0) {
                let rules = sampling_state.filter_rules(RuleType::Transaction);
                evaluator = match evaluator.match_rules(seed, event, rules) {
                    ControlFlow::Continue(evaluator) => evaluator,
                    ControlFlow::Break(sampling_match) => {
                        return SamplingResult::Match(sampling_match);
                    }
                }
            };
        }

        if let (Some(dsc), Some(sampling_state)) = (dsc, root_sampling_config) {
            let rules = sampling_state.filter_rules(RuleType::Trace);
            return evaluator.match_rules(dsc.trace_id, dsc, rules).into();
        }

        SamplingResult::NoMatch
    }

    /// Runs dynamic sampling on an incoming error and tags it in case of successful sampling
    /// decision.
    ///
    /// This execution of dynamic sampling is technically a "simulation" since we will use the result
    /// only for tagging errors and not for actually sampling incoming events.
    fn tag_error_with_sampling_decision(&self, state: &mut ProcessEnvelopeState) {
        let (Some(dsc), Some(event)) = (
            state.managed_envelope.envelope().dsc(),
            state.event.value_mut(),
        ) else {
            return;
        };

        let root_state = state.sampling_project_state.as_ref();
        let config = match root_state.and_then(|s| s.config.sampling.as_ref()) {
            Some(ErrorBoundary::Ok(ref config)) => config,
            _ => return,
        };

        if config.unsupported() {
            if self.inner.config.processing_enabled() {
                relay_log::error!("found unsupported rules even as processing relay");
            }

            return;
        }

        let Some(sampled) = utils::is_trace_fully_sampled(config, dsc) else {
            return;
        };

        // We want to get the trace context, in which we will inject the `sampled` field.
        let context = event
            .contexts
            .get_or_insert_with(Contexts::new)
            .get_or_default::<TraceContext>();

        // We want to update `sampled` only if it was not set, since if we don't check this
        // we will end up overriding the value set by downstream Relays and this will lead
        // to more complex debugging in case of problems.
        if context.sampled.is_empty() {
            relay_log::trace!("tagged error with `sampled = {}` flag", sampled);
            context.sampled = Annotated::new(sampled);
        }
    }

    /// Apply the dynamic sampling decision from `compute_sampling_decision`.
    fn sample_envelope(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        if let SamplingResult::Match(sampling_match) = std::mem::take(&mut state.sampling_result) {
            // We assume that sampling is only supposed to work on transactions.
            if state.event_type() == Some(EventType::Transaction) && sampling_match.should_drop() {
                let matched_rules = sampling_match.into_matched_rules();

                state
                    .managed_envelope
                    .reject(Outcome::FilteredSampling(matched_rules.clone()));

                return Err(ProcessingError::Sampled(matched_rules));
            }
        }
        Ok(())
    }

    fn light_normalize_event(
        &self,
        state: &mut ProcessEnvelopeState,
    ) -> Result<(), ProcessingError> {
        let request_meta = state.managed_envelope.envelope().meta();
        let client_ipaddr = request_meta.client_addr().map(IpAddr::from);

        let light_normalize_spans = state
            .project_state
            .has_feature(Feature::SpanMetricsExtraction);

        let transaction_aggregator_config = self
            .inner
            .config
            .aggregator_config_for(MetricNamespace::Transactions);

        utils::log_transaction_name_metrics(&mut state.event, |event| {
            let config = NormalizeProcessorConfig {
                client_ip: client_ipaddr.as_ref(),
                user_agent: RawUserAgentInfo {
                    user_agent: request_meta.user_agent(),
                    client_hints: request_meta.client_hints().as_deref(),
                },
                received_at: Some(state.managed_envelope.received_at()),
                max_secs_in_past: Some(self.inner.config.max_secs_in_past()),
                max_secs_in_future: Some(self.inner.config.max_secs_in_future()),
                transaction_range: Some(
                    AggregatorConfig::from(transaction_aggregator_config).timestamp_range(),
                ),
                max_name_and_unit_len: Some(
                    transaction_aggregator_config
                        .max_name_length
                        .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),
                ),
                breakdowns_config: state.project_state.config.breakdowns_v2.as_ref(),
                performance_score: state.project_state.config.performance_score.as_ref(),
                normalize_user_agent: Some(true),
                transaction_name_config: TransactionNameConfig {
                    rules: &state.project_state.config.tx_name_rules,
                },
                device_class_synthesis_config: state
                    .project_state
                    .has_feature(Feature::DeviceClassSynthesis),
                enrich_spans: state
                    .project_state
                    .has_feature(Feature::SpanMetricsExtraction),
                max_tag_value_length: self
                    .inner
                    .config
                    .aggregator_config_for(MetricNamespace::Spans)
                    .max_tag_value_length,
                is_renormalize: false,
                light_normalize_spans,
                span_description_rules: state.project_state.config.span_description_rules.as_ref(),
                geoip_lookup: self.inner.geoip_lookup.as_ref(),
                enable_trimming: true,
                measurements: Some(DynamicMeasurementsConfig::new(
                    state.project_state.config().measurements.as_ref(),
                    state.global_config.measurements.as_ref(),
                )),
            };

            metric!(timer(RelayTimers::EventProcessingLightNormalization), {
                process_value(
                    event,
                    &mut relay_event_normalization::NormalizeProcessor::new(config),
                    ProcessingState::root(),
                )
                .map_err(|_| ProcessingError::InvalidTransaction)
            })
        })?;

        Ok(())
    }

    fn process_state(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        macro_rules! if_processing {
            ($if_true:block) => {
                #[cfg(feature = "processing")] {
                    if self.inner.config.processing_enabled() $if_true
                }
            };
        }

        session::process(state, self.inner.config.clone());
        report::process(
            state,
            self.inner.config.clone(),
            self.inner.outcome_aggregator.clone(),
        );
        replay::process(state, self.inner.config.clone())?;
        profile::filter(state);

        if state.creates_event() {
            // Some envelopes only create events in processing relays; for example, unreal events.
            // This makes it possible to get in this code block while not really having an event in
            // the envelope.

            if_processing!({
                self.expand_unreal(state)?;
            });

            event::extract(state, self.inner.config.clone())?;
            profile::transfer_id(state);

            if_processing!({
                self.process_unreal(state)?;
                self.create_placeholders(state);
            });

            event::finalize(state, self.inner.config.clone())?;
            self.light_normalize_event(state)?;
            self.normalize_dsc(state);
            self.filter_event(state)?;
            self.run_dynamic_sampling(state);

            // We avoid extracting metrics if we are not sampling the event while in non-processing
            // relays, in order to synchronize rate limits on indexed and processed transactions.
            if self.inner.config.processing_enabled() || state.sampling_result.should_drop() {
                self.extract_metrics(state)?;
            }

            self.sample_envelope(state)?;

            if_processing!({
                self.store_process_event(state)?;
            });
        }

        if_processing!({
            self.enforce_quotas(state)?;
            profile::process(state, self.inner.config.clone());
            self.process_check_ins(state);
        });

        if state.has_event() {
            self.scrub_event(state)?;
            self.serialize_event(state)?;
            if_processing!({
                self.extract_spans(state);
            });
        }

        self.scrub_attachments(state);

        Ok(())
    }

    fn process(
        &self,
        message: ProcessEnvelope,
    ) -> Result<ProcessEnvelopeResponse, ProcessingError> {
        let mut state = self.prepare_state(message)?;
        let project_id = state.project_id;
        let client = state.envelope().meta().client().map(str::to_owned);
        let user_agent = state.envelope().meta().user_agent().map(str::to_owned);

        relay_log::with_scope(
            |scope| {
                scope.set_tag("project", project_id);
                if let Some(client) = client {
                    scope.set_tag("sdk", client);
                }
                if let Some(user_agent) = user_agent {
                    scope.set_extra("user_agent", user_agent.into());
                }
            },
            || {
                match self.process_state(&mut state) {
                    Ok(()) => {
                        // The envelope could be modified or even emptied during processing, which
                        // requires recomputation of the context.
                        state.managed_envelope.update();

                        let has_metrics = !state.extracted_metrics.project_metrics.is_empty();

                        state.extracted_metrics.send_metrics(
                            state.managed_envelope.envelope(),
                            self.inner.project_cache.clone(),
                        );

                        let envelope_response = if state.managed_envelope.envelope().is_empty() {
                            if !has_metrics {
                                // Individual rate limits have already been issued
                                state.managed_envelope.reject(Outcome::RateLimited(None));
                            } else {
                                state.managed_envelope.accept();
                            }
                            None
                        } else {
                            Some(state.managed_envelope)
                        };

                        Ok(ProcessEnvelopeResponse {
                            envelope: envelope_response,
                        })
                    }
                    Err(err) => {
                        if let Some(outcome) = err.to_outcome() {
                            state.managed_envelope.reject(outcome);
                        }

                        if err.should_keep_metrics() {
                            state.extracted_metrics.send_metrics(
                                state.managed_envelope.envelope(),
                                self.inner.project_cache.clone(),
                            );
                        }

                        Err(err)
                    }
                }
            },
        )
    }

    fn handle_process_envelope(&self, message: ProcessEnvelope) {
        let project_key = message.envelope.envelope().meta().public_key();
        let wait_time = message.envelope.start_time().elapsed();
        metric!(timer(RelayTimers::EnvelopeWaitTime) = wait_time);

        let result = metric!(timer(RelayTimers::EnvelopeProcessingTime), {
            self.process(message)
        });

        match result {
            Ok(response) => {
                if let Some(managed_envelope) = response.envelope {
                    self.inner.envelope_manager.send(SubmitEnvelope {
                        envelope: managed_envelope,
                    })
                };
            }
            Err(error) => {
                // Errors are only logged for what we consider infrastructure or implementation
                // bugs. In other cases, we "expect" errors and log them as debug level.
                if error.is_unexpected() {
                    relay_log::error!(
                        tags.project_key = %project_key,
                        error = &error as &dyn Error,
                        "error processing envelope"
                    );
                }
            }
        }
    }

    fn handle_process_metrics(&self, message: ProcessMetrics) {
        let ProcessMetrics {
            items,
            project_key: public_key,
            start_time,
            sent_at,
        } = message;

        let received = relay_common::time::instant_to_date_time(start_time);
        let received_timestamp = UnixTimestamp::from_secs(received.timestamp() as u64);

        let clock_drift_processor =
            ClockDriftProcessor::new(sent_at, received).at_least(MINIMUM_CLOCK_DRIFT);

        for item in items {
            let payload = item.payload();
            if item.ty() == &ItemType::Statsd {
                let mut buckets = Vec::new();
                for bucket_result in Bucket::parse_all(&payload, received_timestamp) {
                    match bucket_result {
                        Ok(mut bucket) => {
                            clock_drift_processor.process_timestamp(&mut bucket.timestamp);
                            buckets.push(bucket);
                        }
                        Err(error) => relay_log::debug!(
                            error = &error as &dyn Error,
                            "failed to parse metric bucket from statsd format",
                        ),
                    }
                }

                relay_log::trace!("inserting metric buckets into project cache");
                self.inner
                    .project_cache
                    .send(MergeBuckets::new(public_key, buckets));
            } else if item.ty() == &ItemType::MetricBuckets {
                match serde_json::from_slice::<Vec<Bucket>>(&payload) {
                    Ok(mut buckets) => {
                        for bucket in &mut buckets {
                            clock_drift_processor.process_timestamp(&mut bucket.timestamp);
                        }

                        relay_log::trace!("merging metric buckets into project cache");
                        self.inner
                            .project_cache
                            .send(MergeBuckets::new(public_key, buckets));
                    }
                    Err(error) => {
                        relay_log::debug!(
                            error = &error as &dyn Error,
                            "failed to parse metric bucket",
                        );
                        metric!(counter(RelayCounters::MetricBucketsParsingFailed) += 1);
                    }
                }
            } else {
                relay_log::error!(
                    "invalid item of type {} passed to ProcessMetrics",
                    item.ty()
                );
            }
        }
    }

    fn handle_process_metric_meta(&self, message: ProcessMetricMeta) {
        let ProcessMetricMeta { items, project_key } = message;

        for item in items {
            if item.ty() != &ItemType::MetricMeta {
                relay_log::error!(
                    "invalid item of type {} passed to ProcessMetricMeta",
                    item.ty()
                );
                continue;
            }

            let mut payload = item.payload();
            match serde_json::from_slice::<MetricMeta>(&payload) {
                Ok(meta) => {
                    relay_log::trace!("adding metric metadata to project cache");
                    self.inner
                        .project_cache
                        .send(AddMetricMeta { project_key, meta });
                }
                Err(error) => {
                    metric!(counter(RelayCounters::MetricMetaParsingFailed) += 1);

                    relay_log::with_scope(
                        move |scope| {
                            // truncate the payload to basically 200KiB, just in case
                            payload.truncate(200_000);
                            scope.add_attachment(relay_log::protocol::Attachment {
                                buffer: payload.into(),
                                filename: "payload.json".to_owned(),
                                content_type: Some("application/json".to_owned()),
                                ty: None,
                            })
                        },
                        || {
                            relay_log::error!(
                                error = &error as &dyn Error,
                                "failed to parse metric meta"
                            )
                        },
                    );
                }
            }
        }
    }

    /// Check and apply rate limits to metrics buckets.
    #[cfg(feature = "processing")]
    fn handle_rate_limit_buckets(&self, message: RateLimitBuckets) {
        use relay_quotas::ItemScoping;

        let RateLimitBuckets { mut bucket_limiter } = message;

        let scoping = *bucket_limiter.scoping();

        if let Some(rate_limiter) = self.inner.rate_limiter.as_ref() {
            let item_scoping = ItemScoping {
                category: DataCategory::Transaction,
                scoping: &scoping,
            };

            // We set over_accept_once such that the limit is actually reached, which allows subsequent
            // calls with quantity=0 to be rate limited.
            let over_accept_once = true;
            let rate_limits = rate_limiter.is_rate_limited(
                bucket_limiter.quotas(),
                item_scoping,
                bucket_limiter.transaction_count(),
                over_accept_once,
            );

            let was_enforced = bucket_limiter.enforce_limits(
                rate_limits.as_ref().map_err(|_| ()),
                self.inner.outcome_aggregator.clone(),
            );

            if was_enforced {
                if let Ok(limits) = rate_limits {
                    // Update the rate limits in the project cache.
                    self.inner
                        .project_cache
                        .send(UpdateRateLimits::new(scoping.project_key, limits));
                }
            }
        }

        let project_key = bucket_limiter.scoping().project_key;
        let buckets = bucket_limiter.into_metrics();

        if !buckets.is_empty() {
            self.inner
                .aggregator
                .send(MergeBuckets::new(project_key, buckets));
        }
    }

    fn encode_envelope_body(
        body: Vec<u8>,
        http_encoding: HttpEncoding,
    ) -> Result<Vec<u8>, std::io::Error> {
        let envelope_body = match http_encoding {
            HttpEncoding::Identity => body,
            HttpEncoding::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body.as_ref())?;
                encoder.finish()?
            }
            HttpEncoding::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body.as_ref())?;
                encoder.finish()?
            }
            HttpEncoding::Br => {
                // Use default buffer size (via 0), medium quality (5), and the default lgwin (22).
                let mut encoder = BrotliEncoder::new(Vec::new(), 0, 5, 22);
                encoder.write_all(body.as_ref())?;
                encoder.into_inner()
            }
        };
        Ok(envelope_body)
    }

    fn handle_encode_envelope(&self, message: EncodeEnvelope) {
        let mut request = message.request;
        match Self::encode_envelope_body(request.envelope_body, request.http_encoding) {
            Err(e) => {
                request
                    .response_sender
                    .send(Err(SendEnvelopeError::BodyEncodingFailed(e)))
                    .ok();
            }
            Ok(envelope_body) => {
                request.envelope_body = envelope_body;
                self.inner.upstream_relay.send(SendRequest(request));
            }
        }
    }

    fn handle_encode_metrics(&self, message: EncodeMetrics) {
        let EncodeMetrics {
            buckets,
            scoping,
            extraction_mode,
        } = message;

        let partitions = self.inner.config.metrics_partitions();
        let max_batch_size_bytes = self.inner.config.metrics_max_batch_size_bytes();

        let upstream = self.inner.config.upstream_descriptor();
        let dsn = PartialDsn {
            scheme: upstream.scheme(),
            public_key: scoping.project_key,
            host: upstream.host().to_owned(),
            port: upstream.port(),
            path: "".to_owned(),
            project_id: Some(scoping.project_id),
        };
        for (partition_key, buckets) in partition_buckets(scoping.project_key, buckets, partitions)
        {
            let mut num_batches = 0;

            for batch in BucketsView::new(&buckets).by_size(max_batch_size_bytes) {
                let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn.clone()));
                envelope.add_item(create_metrics_item(&batch, extraction_mode));

                let mut envelope = ManagedEnvelope::standalone(
                    envelope,
                    self.inner.outcome_aggregator.clone(),
                    self.inner.test_store.clone(),
                );
                envelope.set_partition_key(partition_key).scope(scoping);

                relay_statsd::metric!(
                    histogram(RelayHistograms::BucketsPerBatch) = batch.len() as u64
                );

                self.inner
                    .envelope_manager
                    .send(SubmitEnvelope { envelope });

                num_batches += 1;
            }

            relay_statsd::metric!(histogram(RelayHistograms::BatchesPerPartition) = num_batches);
        }
    }

    fn handle_encode_metric_meta(&self, message: EncodeMetricMeta) {
        #[cfg(feature = "processing")]
        if self.inner.config.processing_enabled() {
            return self.store_metric_meta(message);
        }

        self.encode_metric_meta(message);
    }

    fn encode_metric_meta(&self, message: EncodeMetricMeta) {
        let EncodeMetricMeta { scoping, meta } = message;

        let upstream = self.inner.config.upstream_descriptor();
        let dsn = crate::extractors::PartialDsn {
            scheme: upstream.scheme(),
            public_key: scoping.project_key,
            host: upstream.host().to_owned(),
            port: upstream.port(),
            path: "".to_owned(),
            project_id: Some(scoping.project_id),
        };

        let mut item = Item::new(ItemType::MetricMeta);
        item.set_payload(ContentType::Json, serde_json::to_vec(&meta).unwrap());
        let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn));
        envelope.add_item(item);
        let envelope = ManagedEnvelope::standalone(envelope, Addr::dummy(), Addr::dummy());

        self.inner
            .envelope_manager
            .send(SubmitEnvelope { envelope });
    }

    #[cfg(feature = "processing")]
    fn store_metric_meta(&self, message: EncodeMetricMeta) {
        let EncodeMetricMeta { scoping, meta } = message;

        let Some(ref metric_meta_store) = self.inner.metric_meta_store else {
            return;
        };

        let r = metric_meta_store.store(scoping.organization_id, scoping.project_id, meta);
        if let Err(error) = r {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                "failed to store metric meta in redis"
            )
        }
    }

    fn handle_message(&self, message: EnvelopeProcessor) {
        match message {
            EnvelopeProcessor::ProcessEnvelope(message) => self.handle_process_envelope(*message),
            EnvelopeProcessor::ProcessMetrics(message) => self.handle_process_metrics(*message),
            EnvelopeProcessor::ProcessMetricMeta(message) => {
                self.handle_process_metric_meta(*message)
            }
            EnvelopeProcessor::EncodeEnvelope(message) => self.handle_encode_envelope(*message),
            EnvelopeProcessor::EncodeMetrics(message) => self.handle_encode_metrics(*message),
            EnvelopeProcessor::EncodeMetricMeta(message) => {
                self.handle_encode_metric_meta(*message)
            }
            #[cfg(feature = "processing")]
            EnvelopeProcessor::RateLimitBuckets(message) => {
                self.handle_rate_limit_buckets(message);
            }
        }
    }
}

impl Service for EnvelopeProcessorService {
    type Interface = EnvelopeProcessor;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let thread_count = self.inner.config.cpu_concurrency();
        relay_log::info!("starting {thread_count} envelope processing workers");

        tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::new(thread_count));

            loop {
                let next_msg = async {
                    let permit_result = semaphore.clone().acquire_owned().await;
                    // `permit_result` might get dropped when this future is cancelled while awaiting
                    // `rx.recv()`. This is OK though: No envelope is received so the permit is not
                    // required.
                    (rx.recv().await, permit_result)
                };

                tokio::select! {
                   biased;

                    (Some(message), Ok(permit)) = next_msg => {
                        let service = self.clone();
                        tokio::task::spawn_blocking(move || {
                            service.handle_message(message);
                            drop(permit);
                        });
                    },

                    else => break
                }
            }
        });
    }
}

fn create_metrics_item(buckets: &BucketsView<'_>, extraction_mode: ExtractionMode) -> Item {
    let source_quantities = buckets
        .iter()
        .filter_map(|bucket| extract_transaction_count(&bucket, extraction_mode))
        .fold(SourceQuantities::default(), |acc, c| {
            let profile_count = if c.has_profile { c.count } else { 0 };

            SourceQuantities {
                transactions: acc.transactions + c.count,
                profiles: acc.profiles + profile_count,
            }
        });

    let mut item = Item::new(ItemType::MetricBuckets);
    item.set_source_quantities(source_quantities);
    item.set_payload(ContentType::Json, serde_json::to_vec(&buckets).unwrap());

    item
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::env;

    use chrono::{DateTime, Utc};
    use relay_base_schema::metrics::{DurationUnit, MetricUnit};
    use relay_common::glob2::LazyGlob;
    use relay_dynamic_config::ProjectConfig;
    use relay_event_normalization::{
        MeasurementsConfig, NormalizeProcessor, RedactionRule, TransactionNameRule,
    };
    use relay_event_schema::protocol::{EventId, LenientString, TransactionSource};
    use relay_pii::DataScrubbingConfig;
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{
        DecayingFunction, RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule,
        SamplingValue, TimeRange,
    };
    use relay_sampling::evaluation::SamplingMatch;
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use crate::extractors::RequestMeta;
    use crate::metrics_extraction::transactions::types::{
        CommonTags, TransactionMeasurementTags, TransactionMetric,
    };
    use crate::metrics_extraction::IntoMetric;

    use crate::testutils::{
        self, create_test_processor, new_envelope, state_with_rule_and_condition,
    };
    use crate::utils::Semaphore as TestSemaphore;

    use super::*;

    fn dummy_reservoir() -> ReservoirEvaluator<'static> {
        ReservoirEvaluator::new(ReservoirCounters::default())
    }

    fn mocked_event(event_type: EventType, transaction: &str, release: &str) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
            ..Event::default()
        }
    }

    #[tokio::test]
    async fn test_dsc_respects_metrics_extracted() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let service: EnvelopeProcessorService = create_test_processor(config);

        // Gets a ProcessEnvelopeState, either with or without the metrics_exracted flag toggled.
        let get_state = |version: Option<u16>| {
            let event = Event {
                id: Annotated::new(EventId::new()),
                ty: Annotated::new(EventType::Transaction),
                transaction: Annotated::new("testing".to_owned()),
                ..Event::default()
            };

            let mut project_state = state_with_rule_and_condition(
                Some(0.0),
                RuleType::Transaction,
                RuleCondition::all(),
            );

            if let Some(version) = version {
                project_state.config.transaction_metrics =
                    ErrorBoundary::Ok(relay_dynamic_config::TransactionMetricsConfig {
                        version,
                        ..Default::default()
                    })
                    .into();
            }

            ProcessEnvelopeState {
                event: Annotated::from(event),
                metrics: Default::default(),
                sample_rates: None,
                sampling_result: SamplingResult::Pending,
                extracted_metrics: Default::default(),
                project_state: Arc::new(project_state),
                sampling_project_state: None,
                project_id: ProjectId::new(42),
                managed_envelope: ManagedEnvelope::new(
                    new_envelope(false, "foo"),
                    TestSemaphore::new(42).try_acquire().unwrap(),
                    outcome_aggregator.clone(),
                    test_store.clone(),
                ),
                profile_id: None,
                event_metrics_extracted: false,
                reservoir: dummy_reservoir(),
                global_config: Arc::default(),
            }
        };

        // None represents no TransactionMetricsConfig, DS will not be run
        let mut state = get_state(None);
        service.run_dynamic_sampling(&mut state);
        assert!(state.sampling_result.should_keep());

        // Current version is 1, so it won't run DS if it's outdated
        let mut state = get_state(Some(0));
        service.run_dynamic_sampling(&mut state);
        assert!(state.sampling_result.should_keep());

        // Dynamic sampling is run, as the transactionmetrics version is up to date.
        let mut state = get_state(Some(1));
        service.run_dynamic_sampling(&mut state);
        assert!(state.sampling_result.should_drop());
    }

    #[test]
    fn test_it_keeps_or_drops_transactions() {
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("testing".to_owned()),
            ..Event::default()
        };

        for (sample_rate, should_keep) in [(0.0, false), (1.0, true)] {
            let sampling_config = SamplingConfig {
                rules: vec![SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: sample_rate },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: DecayingFunction::Constant,
                }],
                ..SamplingConfig::new()
            };

            // TODO: This does not test if the sampling decision is actually applied. This should be
            // refactored to send a proper Envelope in and call process_state to cover the full
            // pipeline.
            let res = EnvelopeProcessorService::compute_sampling_decision(
                false,
                &dummy_reservoir(),
                Some(&sampling_config),
                Some(&event),
                None,
                None,
            );
            assert_eq!(res.should_keep(), should_keep);
        }
    }

    fn process_envelope_with_root_project_state(
        envelope: Box<Envelope>,
        sampling_project_state: Option<Arc<ProjectState>>,
    ) -> Envelope {
        let processor = create_test_processor(Default::default());
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state,
            reservoir_counters: ReservoirCounters::default(),
            global_config: Arc::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        ctx.envelope().clone()
    }

    fn extract_first_event_from_envelope(envelope: Envelope) -> Event {
        let item = envelope.items().next().unwrap();
        let annotated_event: Annotated<Event> =
            Annotated::from_json_bytes(&item.payload()).unwrap();
        annotated_event.into_value().unwrap()
    }

    fn mocked_error_item() -> Item {
        let mut item = Item::new(ItemType::Event);
        item.set_payload(
            ContentType::Json,
            r#"{
              "event_id": "52df9022835246eeb317dbd739ccd059",
              "exception": {
                "values": [
                    {
                      "type": "mytype",
                      "value": "myvalue",
                      "module": "mymodule",
                      "thread_id": 42,
                      "other": "value"
                    }
                ]
              }
            }"#,
        );
        item
    }

    fn project_state_with_single_rule(sample_rate: f64) -> ProjectState {
        let sampling_config = SamplingConfig {
            rules: vec![SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::SampleRate { value: sample_rate },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            ..SamplingConfig::new()
        };

        let mut sampling_project_state = ProjectState::allowed();
        sampling_project_state.config.sampling = Some(ErrorBoundary::Ok(sampling_config));
        sampling_project_state
    }

    #[tokio::test]
    async fn test_error_is_tagged_correctly_if_trace_sampling_result_is_some() {
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Default::default(),
            replay_id: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: Some(true),
            other: BTreeMap::new(),
        };
        envelope.set_dsc(dsc);
        envelope.add_item(mocked_error_item());

        // We test with sample rate equal to 100%.
        let sampling_project_state = project_state_with_single_rule(1.0);
        let new_envelope = process_envelope_with_root_project_state(
            envelope.clone(),
            Some(Arc::new(sampling_project_state)),
        );
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());

        // We test with sample rate equal to 0%.
        let sampling_project_state = project_state_with_single_rule(0.0);
        let new_envelope = process_envelope_with_root_project_state(
            envelope,
            Some(Arc::new(sampling_project_state)),
        );
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(!trace_context.sampled.value().unwrap());
    }

    #[tokio::test]
    async fn test_error_is_not_tagged_if_already_tagged() {
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);

        // We test tagging with an incoming event that has already been tagged by downstream Relay.
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);
        let mut item = Item::new(ItemType::Event);
        item.set_payload(
            ContentType::Json,
            r#"{
              "event_id": "52df9022835246eeb317dbd739ccd059",
              "exception": {
                "values": [
                    {
                      "type": "mytype",
                      "value": "myvalue",
                      "module": "mymodule",
                      "thread_id": 42,
                      "other": "value"
                    }
                ]
              },
              "contexts": {
                "trace": {
                    "sampled": true
                }
              }
            }"#,
        );
        envelope.add_item(item);
        let sampling_project_state = project_state_with_single_rule(0.0);
        let new_envelope = process_envelope_with_root_project_state(
            envelope,
            Some(Arc::new(sampling_project_state)),
        );
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());
    }

    #[tokio::test]
    async fn test_error_is_tagged_correctly_if_trace_sampling_result_is_none() {
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);

        // We test tagging when root project state and dsc are none.
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);
        envelope.add_item(mocked_error_item());
        let new_envelope = process_envelope_with_root_project_state(envelope, None);
        let event = extract_first_event_from_envelope(new_envelope);

        assert!(event.contexts.value().is_none());
    }

    #[tokio::test]
    async fn test_browser_version_extraction_with_pii_like_data() {
        let processor = create_test_processor(Default::default());
        let (outcome_aggregator, test_store) = testutils::processor_services();
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
                let mut item = Item::new(ItemType::Event);
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

        let mut project_state = ProjectState::allowed();
        project_state.config = config;
        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(project_state),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
            global_config: Arc::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let new_envelope = envelope_response.envelope.unwrap();
        let new_envelope = new_envelope.envelope();

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
        assert_eq!(Some("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/********* Safari/537.36"), headers.get_header("User-Agent"));
        // But we still get correct browser and version number
        let contexts = event.contexts.into_value().unwrap();
        let browser = contexts.0.get("browser").unwrap();
        assert_eq!(
            r#"{"name":"Chrome","version":"103.0.0","type":"browser"}"#,
            browser.to_json().unwrap()
        );
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_unprintable_fields() {
        let event = Annotated::new(Event {
            environment: Annotated::new(String::from(
                "�9�~YY���)�����9�~YY���)�����9�~YY���)�����9�~YY���)�����",
            )),
            ..Default::default()
        });
        assert!(has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            release: Annotated::new(
                String::from("���7��#1G����7��#1G����7��#1G����7��#1G����7��#").into(),
            ),
            ..Default::default()
        });
        assert!(has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            environment: Annotated::new(String::from("production")),
            ..Default::default()
        });
        assert!(!has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            release: Annotated::new(
                String::from("release with\t some\n normal\r\nwhitespace").into(),
            ),
            ..Default::default()
        });
        assert!(!has_unprintable_fields(&event));
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
                let config = NormalizeProcessorConfig {
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
                process_value(
                    event,
                    &mut NormalizeProcessor::new(config),
                    ProcessingState::root(),
                )
            })
            .unwrap();
        })
    }

    #[test]
    fn test_log_transaction_metrics_none() {
        let captures = capture_test_event("/nothing", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r#"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:none,source_out:sanitized,is_404:false",
        ]
        "#);
    }

    #[test]
    fn test_log_transaction_metrics_rule() {
        let captures = capture_test_event("/foo/john/denver", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r#"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:rule,source_out:sanitized,is_404:false",
        ]
        "#);
    }

    #[test]
    fn test_log_transaction_metrics_pattern() {
        let captures = capture_test_event("/something/12345", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r#"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:pattern,source_out:sanitized,is_404:false",
        ]
        "#);
    }

    #[test]
    fn test_log_transaction_metrics_both() {
        let captures = capture_test_event("/foo/john/12345", TransactionSource::Url);
        insta::assert_debug_snapshot!(captures, @r#"
        [
            "event.transaction_name_changes:1|c|#source_in:url,changes:both,source_out:sanitized,is_404:false",
        ]
        "#);
    }

    #[test]
    fn test_log_transaction_metrics_no_match() {
        let captures = capture_test_event("/foo/john/12345", TransactionSource::Route);
        insta::assert_debug_snapshot!(captures, @r#"
        [
            "event.transaction_name_changes:1|c|#source_in:route,changes:none,source_out:route,is_404:false",
        ]
        "#);
    }

    /// This is a stand-in test to assert panicking behavior for spawn_blocking.
    ///
    /// [`EnvelopeProcessorService`] relies on tokio to restart the worker threads for blocking
    /// tasks if there is a panic during processing. Tokio does not explicitly mention this behavior
    /// in documentation, though the `spawn_blocking` contract suggests that this is intentional.
    ///
    /// This test should be moved if the worker pool is extracted into a utility.
    #[test]
    fn test_processor_panics() {
        let future = async {
            let semaphore = Arc::new(Semaphore::new(1));

            // loop multiple times to prove that the runtime creates new threads
            for _ in 0..3 {
                // the previous permit should have been released during panic unwind
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                let handle = tokio::task::spawn_blocking(move || {
                    let _permit = permit; // drop(permit) after panic!() would warn as "unreachable"
                    panic!("ignored");
                });

                assert!(handle.await.is_err());
            }
        };

        tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(future);
    }

    /// Confirms that the hardcoded value we use for the fixed length of the measurement MRI is
    /// correct. Unit test is placed here because it has dependencies to relay-server and therefore
    /// cannot be called from relay-metrics.
    #[test]
    fn test_mri_overhead_constant() {
        let hardcoded_value = MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD;

        let derived_value = {
            let name = "foobar".to_string();
            let value = 5.0; // Arbitrary value.
            let unit = MetricUnit::Duration(DurationUnit::default());
            let tags = TransactionMeasurementTags {
                measurement_rating: None,
                universal_tags: CommonTags(BTreeMap::new()),
            };

            let measurement = TransactionMetric::Measurement {
                name: name.clone(),
                value,
                unit,
                tags,
            };

            let metric: Bucket = measurement.into_metric(UnixTimestamp::now());
            metric.name.len() - unit.to_string().len() - name.len()
        };
        assert_eq!(
            hardcoded_value, derived_value,
            "Update `MEASUREMENT_MRI_OVERHEAD` if the naming scheme changed."
        );
    }

    // Helper to extract the sampling match from SamplingResult if thats the variant.
    fn get_sampling_match(sampling_result: SamplingResult) -> SamplingMatch {
        if let SamplingResult::Match(sampling_match) = sampling_result {
            sampling_match
        } else {
            panic!()
        }
    }

    /// Happy path test for compute_sampling_decision.
    #[test]
    fn test_compute_sampling_decision_matching() {
        let event = mocked_event(EventType::Transaction, "foo", "bar");
        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Transaction,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let sampling_config = SamplingConfig {
            rules: vec![rule],
            ..SamplingConfig::new()
        };

        let res = EnvelopeProcessorService::compute_sampling_decision(
            false,
            &dummy_reservoir(),
            Some(&sampling_config),
            Some(&event),
            None,
            None,
        );
        assert!(res.is_match());
    }

    #[test]
    fn test_matching_with_unsupported_rule() {
        let event = mocked_event(EventType::Transaction, "foo", "bar");
        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Transaction,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let unsupported_rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Unsupported,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let sampling_config = SamplingConfig {
            rules: vec![rule, unsupported_rule],
            ..SamplingConfig::new()
        };

        // Unsupported rule should result in no match if processing is not enabled.
        let res = EnvelopeProcessorService::compute_sampling_decision(
            false,
            &dummy_reservoir(),
            Some(&sampling_config),
            Some(&event),
            None,
            None,
        );
        assert!(res.is_no_match());

        // Match if processing is enabled.
        let res = EnvelopeProcessorService::compute_sampling_decision(
            true,
            &dummy_reservoir(),
            Some(&sampling_config),
            Some(&event),
            None,
            None,
        );
        assert!(res.is_match());
    }

    #[test]
    fn test_client_sample_rate() {
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Default::default(),
            replay_id: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: Some(0.5),
            sampled: Some(true),
            other: BTreeMap::new(),
        };

        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 0.2 },
            ty: RuleType::Trace,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let mut sampling_config = SamplingConfig {
            rules: vec![rule],
            ..SamplingConfig::new()
        };

        let res = EnvelopeProcessorService::compute_sampling_decision(
            false,
            &dummy_reservoir(),
            None,
            None,
            Some(&sampling_config),
            Some(&dsc),
        );

        assert_eq!(get_sampling_match(res).sample_rate(), 0.2);

        sampling_config.mode = SamplingMode::Total;

        let res = EnvelopeProcessorService::compute_sampling_decision(
            false,
            &dummy_reservoir(),
            None,
            None,
            Some(&sampling_config),
            Some(&dsc),
        );

        assert_eq!(get_sampling_match(res).sample_rate(), 0.4);
    }
}
