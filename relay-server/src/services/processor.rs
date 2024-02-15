use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::future::Future;
use std::io::Write;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use brotli::CompressorWriter as BrotliEncoder;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use fnv::FnvHasher;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_common::time::UnixTimestamp;
use relay_config::{Config, HttpEncoding};
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::{
    normalize_event, validate_event_timestamps, validate_transaction, ClockDriftProcessor,
    DynamicMeasurementsConfig, EventValidationConfig, MeasurementsConfig, NormalizationConfig,
    TransactionNameConfig, TransactionValidationConfig,
};
use relay_event_normalization::{GeoIpLookup, RawUserAgentInfo};
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::{
    ClientReport, Event, EventId, EventType, IpAddr, Metrics, NetworkReportError,
};
use relay_filter::FilterStatKey;
use relay_metrics::aggregator::AggregatorConfig;
use relay_metrics::{Bucket, BucketView, BucketsView, MergeBuckets, MetricMeta, MetricNamespace};
use relay_pii::PiiConfigError;
use relay_profiling::ProfileId;
use relay_protocol::{Annotated, Value};
use relay_quotas::{DataCategory, Scoping};
use relay_sampling::config::RuleId;
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator};
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse, Service};
use reqwest::header;
use smallvec::{smallvec, SmallVec};
use tokio::sync::Semaphore;

#[cfg(feature = "processing")]
use {
    crate::services::store::{Store, StoreEnvelope},
    crate::utils::{EnvelopeLimiter, ItemAction, MetricsLimiter},
    itertools::Itertools,
    relay_cardinality::{
        CardinalityLimit, CardinalityLimiter, RedisSetLimiter, RedisSetLimiterOptions,
    },
    relay_dynamic_config::CardinalityLimiterMode,
    relay_metrics::{Aggregator, RedisMetricMetaStore},
    relay_quotas::{ItemScoping, RateLimitingError, RedisRateLimiter},
    relay_redis::RedisPool,
    symbolic_unreal::{Unreal4Error, Unreal4ErrorKind},
};

use crate::envelope::{
    self, ContentType, Envelope, EnvelopeError, Item, ItemType, SourceQuantities,
};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::http;
use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::transactions::{ExtractedMetrics, TransactionExtractor};
use crate::service::ServiceError;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::project::ProjectState;
use crate::services::project_cache::{AddMetricMeta, ProjectCache, UpdateRateLimits};
use crate::services::test_store::{Capture, TestStore};
use crate::services::upstream::{
    SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{self, ExtractionMode, ManagedEnvelope, MetricStats, SamplingResult};

mod attachment;
mod dynamic_sampling;
mod event;
mod profile;
mod replay;
mod report;
mod session;
mod span;
#[cfg(feature = "processing")]
mod unreal;

/// Creates the block only if used with `processing` feature.
///
/// Provided code block will be executed only if the provided config has `processing_enabled` set.
macro_rules! if_processing {
    ($config:expr, $if_true:block) => {
        #[cfg(feature = "processing")] {
            if $config.processing_enabled() $if_true
        }
    };
}

/// The minimum clock drift for correction to apply.
const MINIMUM_CLOCK_DRIFT: Duration = Duration::from_secs(55 * 60);

/// Transaction group type marker.
#[derive(Clone, Copy, Debug)]
pub struct TransactionGroup;

/// Error group type marker.
#[derive(Clone, Copy, Debug)]
pub struct ErrorGroup;

/// Session group type marker.
#[derive(Clone, Copy, Debug)]
pub struct SessionGroup;

/// Standalone group type marker.
#[derive(Clone, Copy, Debug)]
pub struct StandaloneGroup;

/// ClientReport group type marker.
#[derive(Clone, Copy, Debug)]
pub struct ClientReportGroup;

/// Replay group type marker.
#[derive(Clone, Copy, Debug)]
pub struct ReplayGroup;

/// CheckIn group type marker.
#[derive(Clone, Copy, Debug)]
pub struct CheckInGroup;

/// Span group type marker.
#[derive(Clone, Copy, Debug)]
pub struct SpanGroup;

/// Metrics group type marker.
#[derive(Clone, Copy, Debug)]
pub struct MetricsGroup;

/// Unknown group type marker.
#[derive(Clone, Copy, Debug)]
pub struct ForwardUnknownGroup;

/// Ungrouped group type marker.
#[derive(Clone, Copy, Debug)]
pub struct Ungrouped;

/// Describes the groups of the processable items.
#[derive(Clone, Copy, Debug)]
pub enum ProcessingGroup {
    /// All the transaction related items.
    ///
    /// Includes transactions, related attachments, profiles.
    Transaction(TransactionGroup),
    /// All the items which require (have or create) events.
    ///
    /// This includes: errors, NEL, security reports, user reports, some of the
    /// attachments.
    Error(ErrorGroup),
    /// Session events.
    Session(SessionGroup),
    /// Standalone items which can be sent alone without any event attached to it in the current
    /// envelope e.g. some attachments, user reports.
    Standalone(StandaloneGroup),
    /// Outcomes.
    ClientReport(ClientReportGroup),
    /// Replays and ReplayRecordings.
    Replay(ReplayGroup),
    /// Crons.
    CheckIn(CheckInGroup),
    /// Spans.
    Span(SpanGroup),
    /// Metrics.
    Metrics(MetricsGroup),
    /// Unknown item types will be forwarded upstream (to processing Relay), where we will
    /// decide what to do with them.
    ForwardUnknown(ForwardUnknownGroup),
    /// All the items in the envelope that could not be grouped.
    Ungrouped(Ungrouped),
}

impl ProcessingGroup {
    /// Splits provided envelope into list of tuples of groups with associated envelopes.
    pub fn split_envelope(mut envelope: Envelope) -> SmallVec<[(Self, Box<Envelope>); 3]> {
        let headers = envelope.headers().clone();
        let mut grouped_envelopes = smallvec![];

        // Each NEL item *must* have a dedicated envelope.
        let nel_envelopes = envelope
            .take_items_by(|item| matches!(item.ty(), &ItemType::Nel))
            .into_iter()
            .map(|item| {
                let headers = headers.clone();
                let items: SmallVec<[Item; 3]> = smallvec![item.clone()];
                let mut envelope = Envelope::from_parts(headers, items);
                envelope.set_event_id(EventId::new());
                (ProcessingGroup::Error(ErrorGroup), envelope)
            });
        grouped_envelopes.extend(nel_envelopes);

        // Extract replays.
        let replay_items = envelope.take_items_by(|item| {
            matches!(
                item.ty(),
                &ItemType::ReplayEvent | &ItemType::ReplayRecording
            )
        });
        if !replay_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Replay(ReplayGroup),
                Envelope::from_parts(headers.clone(), replay_items),
            ))
        }

        // Keep all the sessions together in one envelope.
        let session_items = envelope
            .take_items_by(|item| matches!(item.ty(), &ItemType::Session | &ItemType::Sessions));
        if !session_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Session(SessionGroup),
                Envelope::from_parts(headers.clone(), session_items),
            ))
        }

        // Extract spans.
        let span_items = envelope
            .take_items_by(|item| matches!(item.ty(), &ItemType::Span | &ItemType::OtelSpan));
        if !span_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Span(SpanGroup),
                Envelope::from_parts(headers.clone(), span_items),
            ))
        }

        // Extract all standalone items.
        //
        // Note: only if there are no items in the envelope which can create events, otherwise they
        // will be in the same envelope with all require event items.
        if !envelope.items().any(Item::creates_event) {
            let standalone_items = envelope.take_items_by(Item::requires_event);
            if !standalone_items.is_empty() {
                grouped_envelopes.push((
                    ProcessingGroup::Standalone(StandaloneGroup),
                    Envelope::from_parts(headers.clone(), standalone_items),
                ))
            }
        };

        // Extract all the items which require an event into separate envelope.
        let require_event_items = envelope.take_items_by(Item::requires_event);
        if !require_event_items.is_empty() {
            let group = if require_event_items
                .iter()
                .any(|item| matches!(item.ty(), &ItemType::Transaction | &ItemType::Profile))
            {
                ProcessingGroup::Transaction(TransactionGroup)
            } else {
                ProcessingGroup::Error(ErrorGroup)
            };
            grouped_envelopes.push((
                group,
                Envelope::from_parts(headers.clone(), require_event_items),
            ))
        }

        // Get the rest of the envelopes, one per item.
        let envelopes = envelope.items_mut().map(|item| {
            let headers = headers.clone();
            let items: SmallVec<[Item; 3]> = smallvec![item.clone()];
            let envelope = Envelope::from_parts(headers, items);
            let item_type = item.ty();
            let group = if matches!(item_type, &ItemType::CheckIn) {
                ProcessingGroup::CheckIn(CheckInGroup)
            } else if matches!(item.ty(), &ItemType::ClientReport) {
                ProcessingGroup::ClientReport(ClientReportGroup)
            } else if matches!(item_type, &ItemType::Unknown(_)) {
                ProcessingGroup::ForwardUnknown(ForwardUnknownGroup)
            } else {
                // Cannot group this item type.
                ProcessingGroup::Ungrouped(Ungrouped)
            };

            (group, envelope)
        });
        grouped_envelopes.extend(envelopes);

        grouped_envelopes
    }

    /// Returns the name of the group.
    pub fn variant(&self) -> &'static str {
        match self {
            ProcessingGroup::Transaction(_) => "transaction",
            ProcessingGroup::Error(_) => "error",
            ProcessingGroup::Session(_) => "session",
            ProcessingGroup::Standalone(_) => "standalone",
            ProcessingGroup::ClientReport(_) => "client_report",
            ProcessingGroup::Replay(_) => "replay",
            ProcessingGroup::CheckIn(_) => "check_in",
            ProcessingGroup::Span(_) => "span",
            ProcessingGroup::Metrics(_) => "metrics",
            ProcessingGroup::ForwardUnknown(_) => "forward_unknown",
            ProcessingGroup::Ungrouped(_) => "ungrouped",
        }
    }
}

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
        }
    }

    fn is_unexpected(&self) -> bool {
        self.to_outcome()
            .map_or(false, |outcome| outcome.is_unexpected())
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

impl ExtractedMetrics {
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
struct ProcessEnvelopeState<'a, Group> {
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

    _group: PhantomData<Group>,
}

impl<'a, Group> ProcessEnvelopeState<'a, Group> {
    /// Returns a reference to the contained [`Envelope`].
    fn envelope(&self) -> &Envelope {
        self.managed_envelope.envelope()
    }

    /// Returns a mutable reference to the contained [`Envelope`].
    fn envelope_mut(&mut self) -> &mut Envelope {
        self.managed_envelope.envelope_mut()
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
    fn remove_event(&mut self) {
        self.event = Annotated::empty();
    }

    fn reject_event(&mut self, outcome: Outcome) {
        self.remove_event();
        self.managed_envelope.reject_event(outcome);
    }
}

/// The view out of the [`ProcessEnvelopeState`] after processing.
#[derive(Debug)]
struct ProcessingStateResult {
    managed_envelope: ManagedEnvelope,
    extracted_metrics: ExtractedMetrics,
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

#[derive(Debug)]
pub struct ProcessBatchedMetrics {
    /// Metrics payload in JSON format.
    pub payload: Bytes,

    /// The instant at which the request was received.
    pub start_time: Instant,

    /// The instant at which the request was received.
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

/// Metric buckets with additional project.
#[derive(Debug, Clone)]
pub struct ProjectMetrics {
    /// The metric buckets to encode.
    pub buckets: Vec<Bucket>,
    /// Project state for extracting quotas.
    pub project_state: Arc<ProjectState>,
}

/// Encodes metrics into an envelope ready to be sent upstream.
#[derive(Debug)]
pub struct EncodeMetrics {
    pub scopes: BTreeMap<Scoping, ProjectMetrics>,
}

/// Encodes metric meta into an [`Envelope`] and sends it upstream.
///
/// At the moment, upstream means directly into Redis for processing relays
/// and otherwise submitting the Envelope via HTTP to the [`UpstreamRelay`].
#[derive(Debug)]
pub struct EncodeMetricMeta {
    /// Scoping of the meta.
    pub scoping: Scoping,
    /// The metric meta.
    pub meta: MetricMeta,
}

/// Sends an envelope to the upstream or Kafka.
#[derive(Debug)]
pub struct SubmitEnvelope {
    pub envelope: ManagedEnvelope,
}

/// Sends a client report to the upstream.
#[derive(Debug)]
pub struct SubmitClientReports {
    /// The client report to be sent.
    pub client_reports: Vec<ClientReport>,
    /// Scoping information for the client report.
    pub scoping: Scoping,
}

/// Applies rate limits to metrics buckets and forwards them to the [`Aggregator`].
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
    ProcessBatchedMetrics(Box<ProcessBatchedMetrics>),
    ProcessMetricMeta(Box<ProcessMetricMeta>),
    EncodeMetrics(Box<EncodeMetrics>),
    EncodeMetricMeta(Box<EncodeMetricMeta>),
    SubmitEnvelope(Box<SubmitEnvelope>),
    SubmitClientReports(Box<SubmitClientReports>),
    #[cfg(feature = "processing")]
    RateLimitBuckets(RateLimitBuckets),
}

impl EnvelopeProcessor {
    /// Returns the name of the message variant.
    pub fn variant(&self) -> &'static str {
        match self {
            EnvelopeProcessor::ProcessEnvelope(_) => "ProcessEnvelope",
            EnvelopeProcessor::ProcessMetrics(_) => "ProcessMetrics",
            EnvelopeProcessor::ProcessBatchedMetrics(_) => "ProcessBatchedMetrics",
            EnvelopeProcessor::ProcessMetricMeta(_) => "ProcessMetricMeta",
            EnvelopeProcessor::EncodeMetrics(_) => "EncodeMetrics",
            EnvelopeProcessor::EncodeMetricMeta(_) => "EncodeMetricMeta",
            EnvelopeProcessor::SubmitEnvelope(_) => "SubmitEnvelope",
            EnvelopeProcessor::SubmitClientReports(_) => "SubmitClientReports",
            #[cfg(feature = "processing")]
            EnvelopeProcessor::RateLimitBuckets(_) => "RateLimitBuckets",
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
        Self::ProcessMetrics(Box::new(message))
    }
}

impl FromMessage<ProcessBatchedMetrics> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: ProcessBatchedMetrics, _: ()) -> Self {
        Self::ProcessBatchedMetrics(Box::new(message))
    }
}

impl FromMessage<ProcessMetricMeta> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: ProcessMetricMeta, _: ()) -> Self {
        Self::ProcessMetricMeta(Box::new(message))
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

impl FromMessage<SubmitEnvelope> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: SubmitEnvelope, _: ()) -> Self {
        Self::SubmitEnvelope(Box::new(message))
    }
}

impl FromMessage<SubmitClientReports> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: SubmitClientReports, _: ()) -> Self {
        Self::SubmitClientReports(Box::new(message))
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
    global_config: GlobalConfigHandle,
    #[cfg(feature = "processing")]
    redis_pool: Option<RedisPool>,
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
    #[cfg(feature = "processing")]
    cardinality_limiter: Option<CardinalityLimiter>,
    #[cfg(feature = "processing")]
    store_forwarder: Option<Addr<Store>>,
}

impl EnvelopeProcessorService {
    /// Creates a multi-threaded envelope processor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        #[cfg(feature = "processing")] redis: Option<RedisPool>,
        outcome_aggregator: Addr<TrackOutcome>,
        project_cache: Addr<ProjectCache>,
        upstream_relay: Addr<UpstreamRelay>,
        test_store: Addr<TestStore>,
        #[cfg(feature = "processing")] aggregator: Addr<Aggregator>,
        #[cfg(feature = "processing")] store_forwarder: Option<Addr<Store>>,
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
            global_config,
            #[cfg(feature = "processing")]
            redis_pool: redis.clone(),
            #[cfg(feature = "processing")]
            rate_limiter: redis
                .clone()
                .map(|pool| RedisRateLimiter::new(pool).max_limit(config.max_rate_limit())),
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
            #[cfg(feature = "processing")]
            cardinality_limiter: redis
                .clone()
                .map(|pool| {
                    RedisSetLimiter::new(
                        RedisSetLimiterOptions {
                            cache_vacuum_interval: config
                                .cardinality_limiter_cache_vacuum_interval(),
                        },
                        pool,
                    )
                })
                .map(CardinalityLimiter::new),
            #[cfg(feature = "processing")]
            store_forwarder,
            config,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Normalize monitor check-ins and remove invalid ones.
    #[cfg(feature = "processing")]
    fn process_check_ins(&self, state: &mut ProcessEnvelopeState<CheckInGroup>) {
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
    fn prepare_state<G>(
        &self,
        mut managed_envelope: ManagedEnvelope,
        project_id: ProjectId,
        project_state: Arc<ProjectState>,
        sampling_project_state: Option<Arc<ProjectState>>,
        reservoir_counters: Arc<Mutex<BTreeMap<RuleId, i64>>>,
    ) -> ProcessEnvelopeState<G> {
        let envelope = managed_envelope.envelope_mut();

        // Set the event retention. Effectively, this value will only be available in processing
        // mode when the full project config is queried from the upstream.
        if let Some(retention) = project_state.config.event_retention {
            envelope.set_retention(retention);
        }

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

        ProcessEnvelopeState {
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
            _group: PhantomData::<G> {},
        }
    }

    #[cfg(feature = "processing")]
    fn enforce_quotas<G>(
        &self,
        state: &mut ProcessEnvelopeState<G>,
    ) -> Result<(), ProcessingError> {
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
    fn extract_metrics(
        &self,
        state: &mut ProcessEnvelopeState<TransactionGroup>,
    ) -> Result<(), ProcessingError> {
        // NOTE: This function requires a `metric_extraction` in the project config. Legacy configs
        // will upsert this configuration from transaction and conditional tagging fields, even if
        // it is not present in the actual project config payload. Once transaction metric
        // extraction is moved to generic metrics, this can be converted into an early return.
        let config = match state.project_state.config.metric_extraction {
            ErrorBoundary::Ok(ref config) if config.is_enabled() => Some(config),
            _ => None,
        };

        if let Some(event) = state.event.value() {
            if state.event_metrics_extracted {
                return Ok(());
            }

            if let Some(config) = config {
                let global_config = self.inner.global_config.current();
                let metrics = crate::metrics_extraction::event::extract_metrics(
                    event,
                    config,
                    Some(&global_config.options),
                );
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

    fn light_normalize_event<G>(
        &self,
        state: &mut ProcessEnvelopeState<G>,
    ) -> Result<(), ProcessingError> {
        let request_meta = state.managed_envelope.envelope().meta();
        let client_ipaddr = request_meta.client_addr().map(IpAddr::from);

        let transaction_aggregator_config = self
            .inner
            .config
            .aggregator_config_for(MetricNamespace::Transactions);

        let global_config = self.inner.global_config.current();

        utils::log_transaction_name_metrics(&mut state.event, |event| {
            let tx_validation_config = TransactionValidationConfig {
                timestamp_range: Some(
                    AggregatorConfig::from(transaction_aggregator_config).timestamp_range(),
                ),
            };
            let event_validation_config = EventValidationConfig {
                received_at: Some(state.managed_envelope.received_at()),
                max_secs_in_past: Some(self.inner.config.max_secs_in_past()),
                max_secs_in_future: Some(self.inner.config.max_secs_in_future()),
            };
            let normalization_config = NormalizationConfig {
                client_ip: client_ipaddr.as_ref(),
                user_agent: RawUserAgentInfo {
                    user_agent: request_meta.user_agent(),
                    client_hints: request_meta.client_hints().as_deref(),
                },
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
                span_description_rules: state.project_state.config.span_description_rules.as_ref(),
                geoip_lookup: self.inner.geoip_lookup.as_ref(),
                enable_trimming: true,
                measurements: Some(DynamicMeasurementsConfig::new(
                    state.project_state.config().measurements.as_ref(),
                    global_config.measurements.as_ref(),
                )),
            };

            metric!(timer(RelayTimers::EventProcessingLightNormalization), {
                validate_transaction(event, &tx_validation_config)
                    .map_err(|_| ProcessingError::InvalidTransaction)?;
                validate_event_timestamps(event, &event_validation_config)
                    .map_err(|_| ProcessingError::InvalidTransaction)?;
                normalize_event(event, &normalization_config);
                Result::<(), ProcessingError>::Ok(())
            })
        })?;

        Ok(())
    }

    /// Processes the general errors, and the items which require or create the events.
    fn process_errors(
        &self,
        state: &mut ProcessEnvelopeState<ErrorGroup>,
    ) -> Result<(), ProcessingError> {
        // Events can also contain user reports.
        report::process_user_reports(state);

        if_processing!(self.inner.config, {
            unreal::expand(state, &self.inner.config)?;
        });

        event::extract(state, &self.inner.config)?;

        if_processing!(self.inner.config, {
            unreal::process(state)?;
            attachment::create_placeholders(state);
        });

        event::finalize(state, &self.inner.config)?;
        self.light_normalize_event(state)?;
        event::filter(state)?;
        dynamic_sampling::tag_error_with_sampling_decision(state, &self.inner.config);

        if_processing!(self.inner.config, {
            event::store(state, &self.inner.config)?;
            self.enforce_quotas(state)?;
        });

        if state.has_event() {
            event::scrub(state)?;
            event::serialize(state)?;
        }

        attachment::scrub(state);

        Ok(())
    }

    /// Processes only transactions and transaction-related items.
    fn process_transactions(
        &self,
        state: &mut ProcessEnvelopeState<TransactionGroup>,
    ) -> Result<(), ProcessingError> {
        profile::filter(state);
        event::extract(state, &self.inner.config)?;
        profile::transfer_id(state);

        if_processing!(self.inner.config, {
            attachment::create_placeholders(state);
        });

        event::finalize(state, &self.inner.config)?;
        self.light_normalize_event(state)?;
        dynamic_sampling::normalize(state);
        event::filter(state)?;
        dynamic_sampling::run(state, &self.inner.config);

        // We avoid extracting metrics if we are not sampling the event while in non-processing
        // relays, in order to synchronize rate limits on indexed and processed transactions.
        if self.inner.config.processing_enabled() || state.sampling_result.should_drop() {
            self.extract_metrics(state)?;
        }

        dynamic_sampling::sample_envelope_items(
            state,
            &self.inner.config,
            &self.inner.global_config.current(),
        );

        if_processing!(self.inner.config, {
            event::store(state, &self.inner.config)?;
            self.enforce_quotas(state)?;
            profile::process(state, &self.inner.config);
        });

        if state.has_event() {
            event::scrub(state)?;
            event::serialize(state)?;
            if_processing!(self.inner.config, {
                span::extract_from_event(state);
            });
        }

        attachment::scrub(state);
        Ok(())
    }

    /// Processes standalone items that require an event ID, but do not have an event on the same envelope.
    fn process_standalone(
        &self,
        state: &mut ProcessEnvelopeState<StandaloneGroup>,
    ) -> Result<(), ProcessingError> {
        profile::filter(state);

        if_processing!(self.inner.config, {
            self.enforce_quotas(state)?;
        });

        report::process_user_reports(state);
        attachment::scrub(state);
        Ok(())
    }

    /// Processes user sessions.
    fn process_sessions(
        &self,
        state: &mut ProcessEnvelopeState<SessionGroup>,
    ) -> Result<(), ProcessingError> {
        session::process(state, &self.inner.config);
        if_processing!(self.inner.config, {
            self.enforce_quotas(state)?;
        });
        Ok(())
    }

    /// Processes user and client reports.
    fn process_client_reports(
        &self,
        state: &mut ProcessEnvelopeState<ClientReportGroup>,
    ) -> Result<(), ProcessingError> {
        if_processing!(self.inner.config, {
            self.enforce_quotas(state)?;
        });

        report::process_client_reports(
            state,
            &self.inner.config,
            self.inner.outcome_aggregator.clone(),
        );

        Ok(())
    }

    /// Processes replays.
    fn process_replays(
        &self,
        state: &mut ProcessEnvelopeState<ReplayGroup>,
    ) -> Result<(), ProcessingError> {
        replay::process(state, &self.inner.config)?;
        if_processing!(self.inner.config, {
            self.enforce_quotas(state)?;
        });
        Ok(())
    }

    /// Processes cron check-ins.
    fn process_checkins(
        &self,
        _state: &mut ProcessEnvelopeState<CheckInGroup>,
    ) -> Result<(), ProcessingError> {
        if_processing!(self.inner.config, {
            self.enforce_quotas(_state)?;
            self.process_check_ins(_state);
        });
        Ok(())
    }

    /// Processes spans.
    fn process_spans(
        &self,
        state: &mut ProcessEnvelopeState<SpanGroup>,
    ) -> Result<(), ProcessingError> {
        span::filter(state);
        if_processing!(self.inner.config, {
            self.enforce_quotas(state)?;
            span::process(
                state,
                self.inner.config.clone(),
                &self.inner.global_config.current(),
            );
        });
        Ok(())
    }

    fn process_envelope(
        &self,
        managed_envelope: ManagedEnvelope,
        project_id: ProjectId,
        project_state: Arc<ProjectState>,
        sampling_project_state: Option<Arc<ProjectState>>,
        reservoir_counters: Arc<Mutex<BTreeMap<RuleId, i64>>>,
    ) -> Result<ProcessingStateResult, (ProcessingError, ManagedEnvelope)> {
        // Get the group from the managed envelope context, and if it's not set, try to guess it
        // from the contents of the envelope.
        let group = managed_envelope.group();

        macro_rules! run {
            ($fn:ident) => {{
                let mut state = self.prepare_state(
                    managed_envelope,
                    project_id,
                    project_state,
                    sampling_project_state,
                    reservoir_counters,
                );
                match self.$fn(&mut state) {
                    Ok(()) => Ok(ProcessingStateResult {
                        managed_envelope: state.managed_envelope,
                        extracted_metrics: state.extracted_metrics,
                    }),
                    Err(e) => Err((e, state.managed_envelope)),
                }
            }};
        }

        relay_log::trace!("Processing {group:?} group");

        match group {
            ProcessingGroup::Error(ErrorGroup) => run!(process_errors),
            ProcessingGroup::Transaction(TransactionGroup) => run!(process_transactions),
            ProcessingGroup::Session(SessionGroup) => run!(process_sessions),
            ProcessingGroup::Standalone(StandaloneGroup) => run!(process_standalone),
            ProcessingGroup::ClientReport(ClientReportGroup) => run!(process_client_reports),
            ProcessingGroup::Replay(ReplayGroup) => run!(process_replays),
            ProcessingGroup::CheckIn(CheckInGroup) => run!(process_checkins),
            ProcessingGroup::Span(SpanGroup) => run!(process_spans),
            // Currently is not used.
            ProcessingGroup::Metrics(MetricsGroup) => {
                relay_log::error!(
                    tags.project = %project_id,
                    items = ?managed_envelope.envelope().items().next().map(Item::ty),
                    "received metrics in the process_state"
                );
                Ok(ProcessingStateResult {
                    managed_envelope,
                    extracted_metrics: Default::default(),
                })
            }
            // Fallback to the legacy process_state implementation for Ungrouped events.
            ProcessingGroup::Ungrouped(Ungrouped) => {
                relay_log::error!(
                    tags.project = %project_id,
                    items = ?managed_envelope.envelope().items().next().map(Item::ty),
                    "could not identify the processing group based on the envelope's items"
                );
                Ok(ProcessingStateResult {
                    managed_envelope,
                    extracted_metrics: Default::default(),
                })
            }
            // Leave this group unchanged.
            //
            // This will later be forwarded to upstream.
            ProcessingGroup::ForwardUnknown(ForwardUnknownGroup) => Ok(ProcessingStateResult {
                managed_envelope,
                extracted_metrics: Default::default(),
            }),
        }
    }

    fn process(
        &self,
        message: ProcessEnvelope,
    ) -> Result<ProcessEnvelopeResponse, ProcessingError> {
        let ProcessEnvelope {
            envelope: mut managed_envelope,
            project_state,
            sampling_project_state,
            reservoir_counters,
        } = message;

        // Prefer the project's project ID, and fall back to the stated project id from the
        // envelope. The project ID is available in all modes, other than in proxy mode, where
        // envelopes for unknown projects are forwarded blindly.
        //
        // Neither ID can be available in proxy mode on the /store/ endpoint. This is not supported,
        // since we cannot process an envelope without project ID, so drop it.
        let project_id = match project_state
            .project_id
            .or_else(|| managed_envelope.envelope().meta().project_id())
        {
            Some(project_id) => project_id,
            None => {
                managed_envelope.reject(Outcome::Invalid(DiscardReason::Internal));
                return Err(ProcessingError::MissingProjectId);
            }
        };

        let client = managed_envelope
            .envelope()
            .meta()
            .client()
            .map(str::to_owned);

        let user_agent = managed_envelope
            .envelope()
            .meta()
            .user_agent()
            .map(str::to_owned);

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
                match self.process_envelope(
                    managed_envelope,
                    project_id,
                    project_state,
                    sampling_project_state,
                    reservoir_counters,
                ) {
                    Ok(mut state) => {
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
                    Err((err, mut managed_envelope)) => {
                        if let Some(outcome) = err.to_outcome() {
                            managed_envelope.reject(outcome);
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

        let group = message.envelope.group().variant();
        let result = metric!(timer(RelayTimers::EnvelopeProcessingTime), group = group, {
            self.process(message)
        });
        match result {
            Ok(response) => {
                if let Some(envelope) = response.envelope {
                    self.handle_submit_envelope(SubmitEnvelope { envelope });
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

    fn handle_process_batched_metrics(&self, message: ProcessBatchedMetrics) {
        let ProcessBatchedMetrics {
            payload,
            start_time,
            sent_at,
        } = message;

        let received = relay_common::time::instant_to_date_time(start_time);
        let clock_drift_processor =
            ClockDriftProcessor::new(sent_at, received).at_least(MINIMUM_CLOCK_DRIFT);

        #[derive(serde::Deserialize)]
        struct Wrapper {
            buckets: HashMap<ProjectKey, Vec<Bucket>>,
        }

        match serde_json::from_slice(&payload) {
            Ok(Wrapper { buckets }) => {
                for (public_key, mut buckets) in buckets {
                    for bucket in &mut buckets {
                        clock_drift_processor.process_timestamp(&mut bucket.timestamp);
                    }

                    MetricStats::new(&buckets).emit(
                        RelayCounters::ProcessorBatchedMetricsCalls,
                        RelayCounters::ProcessorBatchedMetricsCount,
                        RelayCounters::ProcessorBatchedMetricsCost,
                    );

                    relay_log::trace!("merging metric buckets into project cache");
                    self.inner
                        .project_cache
                        .send(MergeBuckets::new(public_key, buckets));
                }
            }
            Err(error) => {
                relay_log::debug!(
                    error = &error as &dyn Error,
                    "failed to parse batched metrics",
                );
                metric!(counter(RelayCounters::MetricBucketsParsingFailed) += 1);
            }
        };
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

    fn handle_submit_envelope(&self, message: SubmitEnvelope) {
        let SubmitEnvelope { mut envelope } = message;

        #[cfg(feature = "processing")]
        if self.inner.config.processing_enabled() {
            if let Some(store_forwarder) = self.inner.store_forwarder.clone() {
                relay_log::trace!("sending envelope to kafka");
                store_forwarder.send(StoreEnvelope { envelope });
                return;
            }
        }

        // If we are in capture mode, we stash away the event instead of forwarding it.
        if Capture::should_capture(&self.inner.config) {
            relay_log::trace!("capturing envelope in memory");
            self.inner.test_store.send(Capture::accepted(envelope));
            return;
        }

        // Override the `sent_at` timestamp. Since the envelope went through basic
        // normalization, all timestamps have been corrected. We propagate the new
        // `sent_at` to allow the next Relay to double-check this timestamp and
        // potentially apply correction again. This is done as close to sending as
        // possible so that we avoid internal delays.
        envelope.envelope_mut().set_sent_at(Utc::now());

        relay_log::trace!("sending envelope to sentry endpoint");
        let http_encoding = self.inner.config.http_encoding();
        let result = envelope.envelope().to_vec().and_then(|v| {
            encode_payload(&v.into(), http_encoding).map_err(EnvelopeError::PayloadIoFailed)
        });

        match result {
            Ok(body) => {
                self.inner.upstream_relay.send(SendRequest(SendEnvelope {
                    envelope,
                    body,
                    http_encoding,
                    project_cache: self.inner.project_cache.clone(),
                }));
            }
            Err(error) => {
                // Errors are only logged for what we consider an internal discard reason. These
                // indicate errors in the infrastructure or implementation bugs.
                relay_log::error!(
                    error = &error as &dyn Error,
                    tags.project_key = %envelope.scoping().project_key,
                    "failed to serialize envelope payload"
                );

                envelope.reject(Outcome::Invalid(DiscardReason::Internal));
            }
        }
    }

    fn handle_submit_client_reports(&self, message: SubmitClientReports) {
        let SubmitClientReports {
            client_reports,
            scoping,
        } = message;

        let upstream = self.inner.config.upstream_descriptor();
        let dsn = PartialDsn::outbound(&scoping, upstream);

        let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn));
        for client_report in client_reports {
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(ContentType::Json, client_report.serialize().unwrap()); // TODO: unwrap OK?
            envelope.add_item(item);
        }

        let envelope = ManagedEnvelope::standalone(
            envelope,
            self.inner.outcome_aggregator.clone(),
            self.inner.test_store.clone(),
            ProcessingGroup::ClientReport(ClientReportGroup),
        );
        self.handle_submit_envelope(SubmitEnvelope { envelope });
    }

    /// Check and apply rate limits to metrics buckets.
    #[cfg(feature = "processing")]
    fn handle_rate_limit_buckets(&self, message: RateLimitBuckets) {
        let RateLimitBuckets { mut bucket_limiter } = message;

        let scoping = *bucket_limiter.scoping();

        if let Some(rate_limiter) = self.inner.rate_limiter.as_ref() {
            let item_scoping = relay_quotas::ItemScoping {
                category: DataCategory::Transaction,
                scoping: &scoping,
                namespace: None,
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
            MetricStats::new(&buckets).emit(
                RelayCounters::ProcessorRateLimitBucketsCalls,
                RelayCounters::ProcessorRateLimitBucketsCount,
                RelayCounters::ProcessorRateLimitBucketsCost,
            );

            self.inner
                .aggregator
                .send(MergeBuckets::new(project_key, buckets));
        }
    }

    #[cfg(feature = "processing")]
    fn rate_limit_buckets_by_namespace(
        &self,
        scoping: Scoping,
        buckets: Vec<Bucket>,
        quotas: &[relay_quotas::Quota],
        mode: ExtractionMode,
    ) -> Vec<Bucket> {
        let Some(rate_limiter) = self.inner.rate_limiter.as_ref() else {
            return buckets;
        };

        let buckets_by_ns: HashMap<MetricNamespace, Vec<Bucket>> = buckets
            .into_iter()
            .filter_map(|bucket| Some((bucket.parse_namespace().ok()?, bucket)))
            .into_group_map();

        buckets_by_ns
            .into_iter()
            .filter_map(|(namespace, buckets)| {
                let item_scoping = ItemScoping {
                    category: DataCategory::MetricBucket,
                    scoping: &scoping,
                    namespace: Some(namespace),
                };

                (!self.rate_limit_buckets(item_scoping, &buckets, quotas, mode, rate_limiter))
                    .then_some(buckets)
            })
            .flatten()
            .collect()
    }

    /// Returns `true` if the batches should be rate limited.
    #[cfg(feature = "processing")]
    fn rate_limit_buckets(
        &self,
        item_scoping: relay_quotas::ItemScoping,
        buckets: &[Bucket],
        quotas: &[relay_quotas::Quota],
        mode: ExtractionMode,
        rate_limiter: &RedisRateLimiter,
    ) -> bool {
        let batch_size = self.inner.config.metrics_max_batch_size_bytes();
        let batched_bucket_iter = BucketsView::new(buckets).by_size(batch_size).flatten();
        let quantities = utils::extract_metric_quantities(batched_bucket_iter, mode);

        // Check with redis if the throughput limit has been exceeded, while also updating
        // the count so that other relays will be updated too.
        match rate_limiter.is_rate_limited(quotas, item_scoping, quantities.buckets, false) {
            Ok(limits) if limits.is_limited() => {
                relay_log::debug!(
                    "dropping {} buckets due to throughput rate limit",
                    quantities.buckets
                );

                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                utils::reject_metrics(
                    &self.inner.outcome_aggregator,
                    quantities,
                    *item_scoping.scoping,
                    Outcome::RateLimited(reason_code),
                );

                self.inner.project_cache.send(UpdateRateLimits::new(
                    item_scoping.scoping.project_key,
                    limits,
                ));

                true
            }
            Ok(_) => false,
            Err(e) => {
                relay_log::error!(
                    error = &e as &dyn std::error::Error,
                    "failed to check redis rate limits"
                );
                false
            }
        }
    }

    /// Cardinality limits the passed buckets and returns a filtered vector of only accepted buckets.
    #[cfg(feature = "processing")]
    fn cardinality_limit_buckets(
        &self,
        scoping: Scoping,
        limits: &[CardinalityLimit],
        buckets: Vec<Bucket>,
        mode: ExtractionMode,
    ) -> Vec<Bucket> {
        let global_config = self.inner.global_config.current();
        let cardinality_limiter_mode = global_config.options.cardinality_limiter_mode;

        if matches!(cardinality_limiter_mode, CardinalityLimiterMode::Disabled) {
            return buckets;
        }

        let Some(ref limiter) = self.inner.cardinality_limiter else {
            return buckets;
        };

        let cardinality_scope = relay_cardinality::Scoping {
            organization_id: scoping.organization_id,
            project_id: scoping.project_id,
        };

        let limits = match limiter.check_cardinality_limits(cardinality_scope, limits, buckets) {
            Ok(limits) => limits,
            Err((buckets, error)) => {
                relay_log::error!(
                    error = &error as &dyn std::error::Error,
                    "cardinality limiter failed"
                );

                return buckets;
            }
        };

        let error_sample_rate = global_config.options.cardinality_limiter_error_sample_rate;
        if limits.has_rejections() && sample(error_sample_rate) {
            for limit_id in limits.enforced_limits() {
                relay_log::error!(
                    tags.organization_id = scoping.organization_id,
                    tags.limit_id = limit_id,
                    "Cardinality Limit"
                );
            }
        }

        if matches!(cardinality_limiter_mode, CardinalityLimiterMode::Passive) {
            return limits.into_source();
        }

        // Log outcomes for rejected buckets.
        utils::reject_metrics(
            &self.inner.outcome_aggregator,
            utils::extract_metric_quantities(limits.rejected(), mode),
            scoping,
            Outcome::CardinalityLimited,
        );

        limits.into_accepted()
    }

    /// Processes metric buckets and sends them to kafka.
    ///
    /// This function runs the following steps:
    ///  - cardinality limiting
    ///  - rate limiting
    ///  - submit to `StoreForwarder`
    #[cfg(feature = "processing")]
    fn encode_metrics_processing(&self, message: EncodeMetrics, store_forwarder: &Addr<Store>) {
        use crate::constants::DEFAULT_EVENT_RETENTION;
        use crate::services::store::StoreMetrics;

        for (scoping, message) in message.scopes {
            let ProjectMetrics {
                buckets,
                project_state,
            } = message;

            let mode = project_state.get_extraction_mode();
            let limits = project_state.get_cardinality_limits();

            let buckets = self.cardinality_limit_buckets(scoping, limits, buckets, mode);

            let buckets = self.rate_limit_buckets_by_namespace(
                scoping,
                buckets,
                &project_state.config.quotas,
                mode,
            );

            if buckets.is_empty() {
                continue;
            }

            let retention = project_state
                .config
                .event_retention
                .unwrap_or(DEFAULT_EVENT_RETENTION);

            // The store forwarder takes care of bucket splitting internally, so we can submit the
            // entire list of buckets. There is no batching needed here.
            store_forwarder.send(StoreMetrics {
                buckets,
                scoping,
                retention,
                mode,
            });
        }
    }

    /// Serializes metric buckets to JSON and sends them to the upstream.
    ///
    /// This function runs the following steps:
    ///  - partitioning
    ///  - batching by configured size limit
    ///  - serialize to JSON and pack in an envelope
    ///  - submit the envelope to upstream or kafka depending on configuration
    ///
    /// Cardinality limiting and rate limiting run only in processing Relays as they both require
    /// access to the central Redis instance. Cached rate limits are applied in the project cache
    /// already.
    fn encode_metrics_envelope(&self, message: EncodeMetrics) {
        let batch_size = self.inner.config.metrics_max_batch_size_bytes();
        let upstream = self.inner.config.upstream_descriptor();

        for (scoping, message) in message.scopes {
            let ProjectMetrics {
                buckets,
                project_state,
            } = message;

            let project_key = scoping.project_key;
            let dsn = PartialDsn::outbound(&scoping, upstream);
            let mode = project_state.get_extraction_mode();

            let partitions = if let Some(count) = self.inner.config.metrics_partitions() {
                let mut partitions: BTreeMap<Option<u64>, Vec<Bucket>> = BTreeMap::new();
                for bucket in buckets {
                    let partition_key = partition_key(project_key, &bucket, Some(count));
                    partitions.entry(partition_key).or_default().push(bucket);
                }
                partitions
            } else {
                BTreeMap::from([(None, buckets)])
            };

            for (partition_key, buckets) in partitions {
                if let Some(key) = partition_key {
                    relay_statsd::metric!(histogram(RelayHistograms::PartitionKeys) = key);
                }

                let mut num_batches = 0;
                for batch in BucketsView::new(&buckets).by_size(batch_size) {
                    let mut envelope =
                        Envelope::from_request(None, RequestMeta::outbound(dsn.clone()));

                    let mut item = Item::new(ItemType::MetricBuckets);
                    item.set_source_quantities(utils::extract_metric_quantities(&batch, mode));
                    item.set_payload(ContentType::Json, serde_json::to_vec(&buckets).unwrap());
                    envelope.add_item(item);

                    let mut envelope = ManagedEnvelope::standalone(
                        envelope,
                        self.inner.outcome_aggregator.clone(),
                        self.inner.test_store.clone(),
                        ProcessingGroup::Metrics(MetricsGroup),
                    );
                    envelope.set_partition_key(partition_key).scope(scoping);

                    relay_statsd::metric!(
                        histogram(RelayHistograms::BucketsPerBatch) = batch.len() as u64
                    );

                    self.handle_submit_envelope(SubmitEnvelope { envelope });
                    num_batches += 1;
                }

                relay_statsd::metric!(
                    histogram(RelayHistograms::BatchesPerPartition) = num_batches
                );
            }
        }
    }

    /// Creates a [`SendMetricsRequest`] and sends it to the upstream relay.
    fn send_global_partition(&self, key: Option<u64>, partition: &mut Partition<'_>) {
        if partition.is_empty() {
            return;
        }

        let (unencoded, quantities) = partition.take_parts();
        let http_encoding = self.inner.config.http_encoding();
        let encoded = match encode_payload(&unencoded, http_encoding) {
            Ok(payload) => payload,
            Err(error) => {
                let error = &error as &dyn std::error::Error;
                relay_log::error!(error, "failed to encode metrics payload");
                return;
            }
        };

        let request = SendMetricsRequest {
            partition_key: key.map(|k| k.to_string()),
            unencoded,
            encoded,
            http_encoding,
            quantities,
            outcome_aggregator: self.inner.outcome_aggregator.clone(),
        };

        self.inner.upstream_relay.send(SendRequest(request));
    }

    /// Serializes metric buckets to JSON and sends them to the upstream via the global endpoint.
    ///
    /// This function is similar to [`Self::encode_metrics_envelope`], but sends a global batched
    /// payload directly instead of per-project Envelopes.
    ///
    /// This function runs the following steps:
    ///  - partitioning
    ///  - batching by configured size limit
    ///  - serialize to JSON
    ///  - submit the directly to the upstream
    ///
    /// Cardinality limiting and rate limiting run only in processing Relays as they both require
    /// access to the central Redis instance. Cached rate limits are applied in the project cache
    /// already.
    fn encode_metrics_global(&self, message: EncodeMetrics) {
        let partition_count = self.inner.config.metrics_partitions();
        let batch_size = self.inner.config.metrics_max_batch_size_bytes();

        let mut partitions = BTreeMap::new();

        for (scoping, message) in &message.scopes {
            let ProjectMetrics {
                buckets,
                project_state,
            } = message;

            let mode = project_state.get_extraction_mode();

            for bucket in buckets {
                let partition_key = partition_key(scoping.project_key, bucket, partition_count);

                let mut remaining = Some(BucketView::new(bucket));
                while let Some(bucket) = remaining.take() {
                    let partition = partitions
                        .entry(partition_key)
                        .or_insert_with(|| Partition::new(batch_size, mode));

                    if let Some(next) = partition.insert(bucket, *scoping) {
                        // A part of the bucket could not be inserted. Take the partition and submit
                        // it immediately. Repeat until the final part was inserted. This should
                        // always result in a request, otherwise we would enter an endless loop.
                        self.send_global_partition(partition_key, partition);
                        remaining = Some(next);
                    }
                }
            }
        }

        for (partition_key, mut partition) in partitions {
            self.send_global_partition(partition_key, &mut partition);
        }
    }

    fn handle_encode_metrics(&self, message: EncodeMetrics) {
        let mut stats = MetricStats::default();
        for p in message.scopes.values() {
            stats.update(&p.buckets);
        }
        stats.emit(
            RelayCounters::ProcessorEncodeMetricsCalls,
            RelayCounters::ProcessorEncodeMetricsCount,
            RelayCounters::ProcessorEncodeMetricsCost,
        );

        #[cfg(feature = "processing")]
        if self.inner.config.processing_enabled() {
            if let Some(ref store_forwarder) = self.inner.store_forwarder {
                return self.encode_metrics_processing(message, store_forwarder);
            }
        }

        if self.inner.config.http_global_metrics() {
            self.encode_metrics_global(message)
        } else {
            self.encode_metrics_envelope(message)
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
        let dsn = PartialDsn::outbound(&scoping, upstream);

        let mut item = Item::new(ItemType::MetricMeta);
        item.set_payload(ContentType::Json, serde_json::to_vec(&meta).unwrap());
        let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn));
        envelope.add_item(item);

        let envelope = ManagedEnvelope::standalone(
            envelope,
            self.inner.outcome_aggregator.clone(),
            self.inner.test_store.clone(),
            ProcessingGroup::Metrics(MetricsGroup),
        );
        self.handle_submit_envelope(SubmitEnvelope { envelope });
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

    #[cfg(all(test, feature = "processing"))]
    fn redis_rate_limiter_enabled(&self) -> bool {
        self.inner.rate_limiter.is_some()
    }

    fn handle_message(&self, message: EnvelopeProcessor) {
        let ty = message.variant();
        metric!(timer(RelayTimers::ProcessMessageDuration), message = ty, {
            match message {
                EnvelopeProcessor::ProcessEnvelope(m) => self.handle_process_envelope(*m),
                EnvelopeProcessor::ProcessMetrics(m) => self.handle_process_metrics(*m),
                EnvelopeProcessor::ProcessBatchedMetrics(m) => {
                    self.handle_process_batched_metrics(*m)
                }
                EnvelopeProcessor::ProcessMetricMeta(m) => self.handle_process_metric_meta(*m),
                EnvelopeProcessor::EncodeMetrics(m) => self.handle_encode_metrics(*m),
                EnvelopeProcessor::EncodeMetricMeta(m) => self.handle_encode_metric_meta(*m),
                EnvelopeProcessor::SubmitEnvelope(m) => self.handle_submit_envelope(*m),
                EnvelopeProcessor::SubmitClientReports(m) => self.handle_submit_client_reports(*m),
                #[cfg(feature = "processing")]
                EnvelopeProcessor::RateLimitBuckets(m) => self.handle_rate_limit_buckets(m),
            }
        });
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

fn encode_payload(body: &Bytes, http_encoding: HttpEncoding) -> Result<Bytes, std::io::Error> {
    let envelope_body: Vec<u8> = match http_encoding {
        HttpEncoding::Identity => return Ok(body.clone()),
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

    Ok(envelope_body.into())
}

/// An upstream request that submits an envelope via HTTP.
#[derive(Debug)]
pub struct SendEnvelope {
    envelope: ManagedEnvelope,
    body: Bytes,
    http_encoding: HttpEncoding,
    project_cache: Addr<ProjectCache>,
}

impl UpstreamRequest for SendEnvelope {
    fn method(&self) -> reqwest::Method {
        reqwest::Method::POST
    }

    fn path(&self) -> Cow<'_, str> {
        format!("/api/{}/envelope/", self.envelope.scoping().project_id).into()
    }

    fn route(&self) -> &'static str {
        "envelope"
    }

    fn build(&mut self, builder: &mut http::RequestBuilder) -> Result<(), http::HttpError> {
        let envelope_body = self.body.clone();
        metric!(histogram(RelayHistograms::UpstreamEnvelopeBodySize) = envelope_body.len() as u64);

        let meta = &self.envelope.meta();
        let shard = self.envelope.partition_key().map(|p| p.to_string());
        builder
            .content_encoding(self.http_encoding)
            .header_opt("Origin", meta.origin().map(|url| url.as_str()))
            .header_opt("User-Agent", meta.user_agent())
            .header("X-Sentry-Auth", meta.auth_header())
            .header("X-Forwarded-For", meta.forwarded_for())
            .header("Content-Type", envelope::CONTENT_TYPE)
            .header_opt("X-Sentry-Relay-Shard", shard)
            .body(envelope_body);

        Ok(())
    }

    fn respond(
        self: Box<Self>,
        result: Result<http::Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let result = match result {
                Ok(mut response) => response.consume().await.map_err(UpstreamRequestError::Http),
                Err(error) => Err(error),
            };

            match result {
                Ok(()) => self.envelope.accept(),
                Err(error) if error.is_received() => {
                    let scoping = self.envelope.scoping();
                    self.envelope.accept();

                    if let UpstreamRequestError::RateLimited(limits) = error {
                        self.project_cache.send(UpdateRateLimits::new(
                            scoping.project_key,
                            limits.scope(&scoping),
                        ));
                    }
                }
                Err(error) => {
                    // Errors are only logged for what we consider an internal discard reason. These
                    // indicate errors in the infrastructure or implementation bugs.
                    let mut envelope = self.envelope;
                    envelope.reject(Outcome::Invalid(DiscardReason::Internal));
                    relay_log::error!(
                        error = &error as &dyn Error,
                        tags.project_key = %envelope.scoping().project_key,
                        "error sending envelope"
                    );
                }
            }
        })
    }
}

/// Returns `true` if the current item should be sampled.
///
/// The passed `rate` is expected to be `0 <= rate <= 1`.
#[cfg(feature = "processing")]
fn sample(rate: f32) -> bool {
    (rate >= 1.0) || (rate > 0.0 && rand::random::<f32>() < rate)
}

/// Computes a stable partitioning key for sharded metric requests.
fn partition_key(project_key: ProjectKey, bucket: &Bucket, partitions: Option<u64>) -> Option<u64> {
    use std::hash::{Hash, Hasher};

    let partitions = partitions?.max(1);
    let key = (project_key, &bucket.name, &bucket.tags);

    let mut hasher = FnvHasher::default();
    key.hash(&mut hasher);
    Some(hasher.finish() % partitions)
}

/// A container for metric buckets from multiple projects.
///
/// This container is used to send metrics to the upstream in global batches as part of the
/// [`EncodeMetrics`] message if the `http.global_metrics` option is enabled. The container monitors
/// the size of all metrics and allows to split them into multiple batches. See
/// [`insert`](Self::insert) for more information.
#[derive(Debug)]
struct Partition<'a> {
    max_size: usize,
    remaining: usize,
    views: HashMap<ProjectKey, Vec<BucketView<'a>>>,
    quantities: Vec<(Scoping, SourceQuantities)>,
    mode: ExtractionMode,
}

impl<'a> Partition<'a> {
    /// Creates a new partition with the given maximum size in bytes.
    pub fn new(size: usize, mode: ExtractionMode) -> Self {
        Self {
            max_size: size,
            remaining: size,
            views: HashMap::new(),
            quantities: Vec::new(),
            mode,
        }
    }

    /// Inserts a bucket into the partition, splitting it if necessary.
    ///
    /// This function attempts to add the bucket to this partition. If the bucket does not fit
    /// entirely into the partition given its maximum size, the remaining part of the bucket is
    /// returned from this function call.
    ///
    /// If this function returns `Some(_)`, the partition is full and should be submitted to the
    /// upstream immediately. Use [`take_parts`](Self::take_parts) to retrieve the contents of the
    /// partition. Afterwards, the caller is responsible to call this function again with the
    /// remaining bucket until it is fully inserted.
    pub fn insert(&mut self, bucket: BucketView<'a>, scoping: Scoping) -> Option<BucketView<'a>> {
        let (current, next) = bucket.split(self.remaining, Some(self.max_size));

        if let Some(current) = current {
            self.remaining = self.remaining.saturating_sub(current.estimated_size());
            let quantities = utils::extract_metric_quantities([current.clone()], self.mode);
            self.quantities.push((scoping, quantities));
            self.views
                .entry(scoping.project_key)
                .or_default()
                .push(current);
        }

        next
    }

    /// Returns `true` if the partition does not hold any data.
    fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// Returns the serialized buckets and the source quantities for this partition.
    ///
    /// This empties the partition, so that it can be reused.
    fn take_parts(&mut self) -> (Bytes, Vec<(Scoping, SourceQuantities)>) {
        #[derive(serde::Serialize)]
        struct Wrapper<'a> {
            buckets: &'a HashMap<ProjectKey, Vec<BucketView<'a>>>,
        }

        let buckets = &self.views;
        let payload = serde_json::to_vec(&Wrapper { buckets }).unwrap().into();
        let quantities = self.quantities.clone();

        self.views.clear();
        self.quantities.clear();
        self.remaining = self.max_size;

        (payload, quantities)
    }
}

/// An upstream request that submits metric buckets via HTTP.
///
/// This request is not awaited. It automatically tracks outcomes if the request is not received.
#[derive(Debug)]
struct SendMetricsRequest {
    /// If the partition key is set, the request is marked with `X-Sentry-Relay-Shard`.
    partition_key: Option<String>,
    /// Serialized metric buckets without encoding applied, used for signing.
    unencoded: Bytes,
    /// Serialized metric buckets with the stated HTTP encoding applied.
    encoded: Bytes,
    /// Encoding (compression) of the payload.
    http_encoding: HttpEncoding,
    /// Information about the metric quantities in the payload for outcomes.
    quantities: Vec<(Scoping, SourceQuantities)>,
    /// Address of the outcome aggregator to send outcomes to on error.
    outcome_aggregator: Addr<TrackOutcome>,
}

impl UpstreamRequest for SendMetricsRequest {
    fn set_relay_id(&self) -> bool {
        true
    }

    fn sign(&mut self) -> Option<Bytes> {
        Some(self.unencoded.clone())
    }

    fn method(&self) -> reqwest::Method {
        reqwest::Method::POST
    }

    fn path(&self) -> Cow<'_, str> {
        "/api/0/relays/metrics/".into()
    }

    fn route(&self) -> &'static str {
        "global_metrics"
    }

    fn build(&mut self, builder: &mut http::RequestBuilder) -> Result<(), http::HttpError> {
        metric!(histogram(RelayHistograms::UpstreamMetricsBodySize) = self.encoded.len() as u64);

        builder
            .content_encoding(self.http_encoding)
            .header_opt("X-Sentry-Relay-Shard", self.partition_key.as_ref())
            .header(header::CONTENT_TYPE, b"application/json")
            .body(self.encoded.clone());

        Ok(())
    }

    fn respond(
        self: Box<Self>,
        result: Result<http::Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async {
            match result {
                Ok(mut response) => {
                    response.consume().await.ok();
                }
                // Request did not arrive, we are responsible for outcomes.
                Err(error) if !error.is_received() => {
                    for (scoping, quantities) in self.quantities {
                        utils::reject_metrics(
                            &self.outcome_aggregator,
                            quantities,
                            scoping,
                            Outcome::Invalid(DiscardReason::Internal),
                        );
                    }
                }
                // Upstream is responsible to log outcomes.
                Err(_received) => (),
            }
        })
    }
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
        normalize_event, MeasurementsConfig, RedactionRule, TransactionNameRule,
    };
    use relay_event_schema::protocol::{EventId, TransactionSource};
    use relay_pii::DataScrubbingConfig;
    use similar_asserts::assert_eq;

    use crate::extractors::RequestMeta;
    use crate::metrics_extraction::transactions::types::{
        CommonTags, TransactionMeasurementTags, TransactionMetric,
    };
    use crate::metrics_extraction::IntoMetric;
    use crate::testutils::{self, create_test_processor};

    #[cfg(feature = "processing")]
    use {
        relay_metrics::BucketValue,
        relay_quotas::{Quota, ReasonCode},
        relay_test::mock_service,
    };

    use super::*;

    /// Ensures that if we ratelimit one batch of buckets in [`EncodeMetrics`] message, it won't
    /// also ratelimit the next batches in the same message automatically.
    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_ratelimit_per_batch() {
        let rate_limited_org = 1;
        let not_ratelimited_org = 2;

        let message = {
            let project_state = {
                let quota = Quota {
                    id: Some("testing".into()),
                    categories: vec![DataCategory::MetricBucket].into(),
                    scope: relay_quotas::QuotaScope::Organization,
                    scope_id: Some(rate_limited_org.to_string()),
                    limit: Some(0),
                    window: None,
                    reason_code: Some(ReasonCode::new("test")),
                    namespace: None,
                };

                let mut config = ProjectConfig::default();
                config.quotas.push(quota);

                let mut project_state = ProjectState::allowed();
                project_state.config = config;
                Arc::new(project_state)
            };

            let project_metrics = ProjectMetrics {
                buckets: vec![Bucket {
                    name: "d:transactions/bar".to_string(),
                    value: BucketValue::Counter(relay_metrics::FiniteF64::new(1.0).unwrap()),
                    timestamp: UnixTimestamp::now(),
                    tags: Default::default(),
                    width: 10,
                }],
                project_state,
            };

            let scoping_by_org_id = |org_id: u64| Scoping {
                organization_id: org_id,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            };

            let mut scopes = BTreeMap::<Scoping, ProjectMetrics>::new();
            scopes.insert(scoping_by_org_id(rate_limited_org), project_metrics.clone());
            scopes.insert(scoping_by_org_id(not_ratelimited_org), project_metrics);

            EncodeMetrics { scopes }
        };

        // ensure the order of the map while iterating is as expected.
        let mut iter = message.scopes.keys();
        assert_eq!(iter.next().unwrap().organization_id, rate_limited_org);
        assert_eq!(iter.next().unwrap().organization_id, not_ratelimited_org);
        assert!(iter.next().is_none());

        let config = {
            let config_json = serde_json::json!({
                "processing": {
                    "enabled": true,
                    "kafka_config": [],
                    "redis": {
                        "server": std::env::var("RELAY_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned()),
                    }
                }
            });
            Config::from_json_value(config_json).unwrap()
        };

        let (store, handle) = {
            let f = |org_ids: &mut Vec<u64>, msg: Store| {
                let org_id = match msg {
                    Store::Metrics(x) => x.scoping.organization_id,
                    Store::Envelope(_) => panic!("received envelope when expecting only metrics"),
                };
                org_ids.push(org_id);
            };

            mock_service("store_forwarder", vec![], f)
        };

        let processor = create_test_processor(config);
        assert!(processor.redis_rate_limiter_enabled());

        processor.encode_metrics_processing(message, &store);

        drop(store);
        let orgs_not_ratelimited = handle.await.unwrap();

        assert_eq!(orgs_not_ratelimited, vec![not_ratelimited_org]);
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

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store, group);

        let message = ProcessEnvelope {
            envelope,
            project_state: Arc::new(project_state),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
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
                normalize_event(event, &config)
            });
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
            let value = 5.into(); // Arbitrary value.
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
}
