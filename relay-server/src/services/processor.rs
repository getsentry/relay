use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use anyhow::Context;
use brotli::CompressorWriter as BrotliEncoder;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_cogs::{AppFeature, Cogs, FeatureWeights, ResourceId, Token};
use relay_common::time::UnixTimestamp;
use relay_config::{Config, HttpEncoding, NormalizationLevel, RelayMode};
use relay_dynamic_config::{CombinedMetricExtractionConfig, ErrorBoundary, Feature, GlobalConfig};
use relay_event_normalization::{
    normalize_event, validate_event, ClockDriftProcessor, CombinedMeasurementsConfig,
    EventValidationConfig, GeoIpLookup, MeasurementsConfig, NormalizationConfig, RawUserAgentInfo,
    TransactionNameConfig,
};
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::{
    ClientReport, Event, EventId, EventType, IpAddr, Metrics, NetworkReportError,
};
use relay_filter::FilterStatKey;
use relay_metrics::{
    Bucket, BucketMetadata, BucketValue, BucketView, BucketsView, FiniteF64, MetricMeta,
    MetricNamespace,
};
use relay_pii::PiiConfigError;
use relay_profiling::ProfileId;
use relay_protocol::{Annotated, Value};
use relay_quotas::{DataCategory, RateLimits, Scoping};
use relay_sampling::config::RuleId;
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator, SamplingDecision};
use relay_sampling::DynamicSamplingContext;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse, Service};
use reqwest::header;
use smallvec::{smallvec, SmallVec};

#[cfg(feature = "processing")]
use {
    crate::services::store::{Store, StoreEnvelope},
    crate::utils::{sample, Enforcement, EnvelopeLimiter, ItemAction},
    itertools::Itertools,
    relay_cardinality::{
        CardinalityLimit, CardinalityLimiter, CardinalityLimitsSplit, RedisSetLimiter,
        RedisSetLimiterOptions,
    },
    relay_dynamic_config::{CardinalityLimiterMode, MetricExtractionGroups},
    relay_metrics::RedisMetricMetaStore,
    relay_quotas::{Quota, RateLimitingError, RedisRateLimiter},
    relay_redis::{RedisPool, RedisPools},
    std::iter::Chain,
    std::slice::Iter,
    symbolic_unreal::{Unreal4Error, Unreal4ErrorKind},
};

use crate::envelope::{self, ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::http;
use crate::metrics::{MetricOutcomes, MetricsLimiter, MinimalTrackableBucket};
use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::transactions::{ExtractedMetrics, TransactionExtractor};
use crate::service::ServiceError;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::metrics::{Aggregator, MergeBuckets};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::event::FiltersStatus;
use crate::services::project::{ProjectInfo, ProjectState};
use crate::services::project_cache::{
    AddMetricMeta, BucketSource, ProcessMetrics, ProjectCache, UpdateRateLimits,
};
use crate::services::test_store::{Capture, TestStore};
use crate::services::upstream::{
    SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{
    self, InvalidProcessingGroupType, ManagedEnvelope, SamplingResult, ThreadPool, TypedEnvelope,
    WorkerGroup,
};

mod attachment;
mod dynamic_sampling;
mod event;
mod metrics;
mod profile;
mod profile_chunk;
mod replay;
mod report;
mod session;
mod span;
pub use span::extract_transaction_span;

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
    ($config:expr, $if_true:block else $if_false:block) => {
        {
            #[cfg(feature = "processing")] {
                if $config.processing_enabled() $if_true else $if_false
            }
            #[cfg(not(feature = "processing"))] {
                $if_false
            }
        }
    };
}

/// The minimum clock drift for correction to apply.
const MINIMUM_CLOCK_DRIFT: Duration = Duration::from_secs(55 * 60);

#[derive(Debug)]
pub struct GroupTypeError;

impl Display for GroupTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to convert processing group into corresponding type")
    }
}

impl std::error::Error for GroupTypeError {}

macro_rules! processing_group {
    ($ty:ident, $variant:ident) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $ty;

        impl From<$ty> for ProcessingGroup {
            fn from(_: $ty) -> Self {
                ProcessingGroup::$variant
            }
        }

        impl TryFrom<ProcessingGroup> for $ty {
            type Error = GroupTypeError;

            fn try_from(value: ProcessingGroup) -> Result<Self, Self::Error> {
                if matches!(value, ProcessingGroup::$variant) {
                    return Ok($ty);
                }
                return Err(GroupTypeError);
            }
        }
    };
}

/// A marker trait.
///
/// Should be used only with groups which are responsible for processing envelopes with events.
pub trait EventProcessing {}

/// A trait for processing groups that can be dynamically sampled.
pub trait Sampling {
    /// Whether dynamic sampling should run under the given project's conditions.
    fn supports_sampling(project_state: &ProjectInfo) -> bool;

    /// Whether reservoir sampling applies to this processing group (a.k.a. data type).
    fn supports_reservoir_sampling() -> bool;
}

processing_group!(TransactionGroup, Transaction);
impl EventProcessing for TransactionGroup {}

impl Sampling for TransactionGroup {
    fn supports_sampling(project_state: &ProjectInfo) -> bool {
        // For transactions, we require transaction metrics to be enabled before sampling.
        matches!(&project_state.config.transaction_metrics, Some(ErrorBoundary::Ok(c)) if c.is_enabled())
    }

    fn supports_reservoir_sampling() -> bool {
        true
    }
}

processing_group!(ErrorGroup, Error);
impl EventProcessing for ErrorGroup {}

processing_group!(SessionGroup, Session);
processing_group!(StandaloneGroup, Standalone);
processing_group!(ClientReportGroup, ClientReport);
processing_group!(ReplayGroup, Replay);
processing_group!(CheckInGroup, CheckIn);
processing_group!(SpanGroup, Span);

impl Sampling for SpanGroup {
    fn supports_sampling(project_state: &ProjectInfo) -> bool {
        // If no metrics could be extracted, do not sample anything.
        matches!(&project_state.config().metric_extraction, ErrorBoundary::Ok(c) if c.is_supported())
    }

    fn supports_reservoir_sampling() -> bool {
        false
    }
}

processing_group!(ProfileChunkGroup, ProfileChunk);
processing_group!(MetricsGroup, Metrics);
processing_group!(ForwardUnknownGroup, ForwardUnknown);
processing_group!(Ungrouped, Ungrouped);

/// Processed group type marker.
///
/// Marks the envelopes which passed through the processing pipeline.
#[derive(Clone, Copy, Debug)]
pub struct Processed;

/// Describes the groups of the processable items.
#[derive(Clone, Copy, Debug)]
pub enum ProcessingGroup {
    /// All the transaction related items.
    ///
    /// Includes transactions, related attachments, profiles.
    Transaction,
    /// All the items which require (have or create) events.
    ///
    /// This includes: errors, NEL, security reports, user reports, some of the
    /// attachments.
    Error,
    /// Session events.
    Session,
    /// Standalone items which can be sent alone without any event attached to it in the current
    /// envelope e.g. some attachments, user reports.
    Standalone,
    /// Outcomes.
    ClientReport,
    /// Replays and ReplayRecordings.
    Replay,
    /// Crons.
    CheckIn,
    /// Spans.
    Span,
    /// Metrics.
    Metrics,
    /// ProfileChunk.
    ProfileChunk,
    /// Unknown item types will be forwarded upstream (to processing Relay), where we will
    /// decide what to do with them.
    ForwardUnknown,
    /// All the items in the envelope that could not be grouped.
    Ungrouped,
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
                (ProcessingGroup::Error, envelope)
            });
        grouped_envelopes.extend(nel_envelopes);

        // Extract replays.
        let replay_items = envelope.take_items_by(|item| {
            matches!(
                item.ty(),
                &ItemType::ReplayEvent | &ItemType::ReplayRecording | &ItemType::ReplayVideo
            )
        });
        if !replay_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Replay,
                Envelope::from_parts(headers.clone(), replay_items),
            ))
        }

        // Keep all the sessions together in one envelope.
        let session_items = envelope
            .take_items_by(|item| matches!(item.ty(), &ItemType::Session | &ItemType::Sessions));
        if !session_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Session,
                Envelope::from_parts(headers.clone(), session_items),
            ))
        }

        // Extract spans.
        let span_items = envelope
            .take_items_by(|item| matches!(item.ty(), &ItemType::Span | &ItemType::OtelSpan));
        if !span_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Span,
                Envelope::from_parts(headers.clone(), span_items),
            ))
        }

        // Exract all metric items.
        //
        // Note: Should only be relevant in proxy mode. In other modes we send metrics through
        // a separate pipeline.
        let metric_items = envelope.take_items_by(|i| i.ty().is_metrics());
        if !metric_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Metrics,
                Envelope::from_parts(headers.clone(), metric_items),
            ))
        }

        // Extract profile chunks.
        let profile_chunk_items =
            envelope.take_items_by(|item| matches!(item.ty(), &ItemType::ProfileChunk));
        if !profile_chunk_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::ProfileChunk,
                Envelope::from_parts(headers.clone(), profile_chunk_items),
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
                    ProcessingGroup::Standalone,
                    Envelope::from_parts(headers.clone(), standalone_items),
                ))
            }
        };

        // Make sure we create separate envelopes for each `RawSecurity` report.
        let security_reports_items = envelope
            .take_items_by(|i| matches!(i.ty(), &ItemType::RawSecurity))
            .into_iter()
            .map(|item| {
                let headers = headers.clone();
                let items: SmallVec<[Item; 3]> = smallvec![item.clone()];
                let mut envelope = Envelope::from_parts(headers, items);
                envelope.set_event_id(EventId::new());
                (ProcessingGroup::Error, envelope)
            });
        grouped_envelopes.extend(security_reports_items);

        // Extract all the items which require an event into separate envelope.
        let require_event_items = envelope.take_items_by(Item::requires_event);
        if !require_event_items.is_empty() {
            let group = if require_event_items
                .iter()
                .any(|item| matches!(item.ty(), &ItemType::Transaction | &ItemType::Profile))
            {
                ProcessingGroup::Transaction
            } else {
                ProcessingGroup::Error
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
                ProcessingGroup::CheckIn
            } else if matches!(item.ty(), &ItemType::ClientReport) {
                ProcessingGroup::ClientReport
            } else if matches!(item_type, &ItemType::Unknown(_)) {
                ProcessingGroup::ForwardUnknown
            } else {
                // Cannot group this item type.
                ProcessingGroup::Ungrouped
            };

            (group, envelope)
        });
        grouped_envelopes.extend(envelopes);

        grouped_envelopes
    }

    /// Returns the name of the group.
    pub fn variant(&self) -> &'static str {
        match self {
            ProcessingGroup::Transaction => "transaction",
            ProcessingGroup::Error => "error",
            ProcessingGroup::Session => "session",
            ProcessingGroup::Standalone => "standalone",
            ProcessingGroup::ClientReport => "client_report",
            ProcessingGroup::Replay => "replay",
            ProcessingGroup::CheckIn => "check_in",
            ProcessingGroup::Span => "span",
            ProcessingGroup::Metrics => "metrics",
            ProcessingGroup::ProfileChunk => "profile_chunk",
            ProcessingGroup::ForwardUnknown => "forward_unknown",
            ProcessingGroup::Ungrouped => "ungrouped",
        }
    }
}

impl From<ProcessingGroup> for AppFeature {
    fn from(value: ProcessingGroup) -> Self {
        match value {
            ProcessingGroup::Transaction => AppFeature::Transactions,
            ProcessingGroup::Error => AppFeature::Errors,
            ProcessingGroup::Session => AppFeature::Sessions,
            ProcessingGroup::Standalone => AppFeature::UnattributedEnvelope,
            ProcessingGroup::ClientReport => AppFeature::ClientReports,
            ProcessingGroup::Replay => AppFeature::Replays,
            ProcessingGroup::CheckIn => AppFeature::CheckIns,
            ProcessingGroup::Span => AppFeature::Spans,
            ProcessingGroup::Metrics => AppFeature::UnattributedMetrics,
            ProcessingGroup::ProfileChunk => AppFeature::Profiles,
            ProcessingGroup::ForwardUnknown => AppFeature::UnattributedEnvelope,
            ProcessingGroup::Ungrouped => AppFeature::UnattributedEnvelope,
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

    #[error("unsupported security report type")]
    UnsupportedSecurityType,

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

    #[error("invalid processing group type")]
    InvalidProcessingGroup(#[from] InvalidProcessingGroupType),

    #[error("invalid replay")]
    InvalidReplay(DiscardReason),

    #[error("replay filtered with reason: {0:?}")]
    ReplayFiltered(FilterStatKey),
}

impl ProcessingError {
    fn to_outcome(&self) -> Option<Outcome> {
        match self {
            // General outcomes for invalid events
            Self::PayloadTooLarge => Some(Outcome::Invalid(DiscardReason::TooLarge)),
            Self::InvalidJson(_) => Some(Outcome::Invalid(DiscardReason::InvalidJson)),
            Self::InvalidMsgpack(_) => Some(Outcome::Invalid(DiscardReason::InvalidMsgpack)),
            Self::InvalidSecurityType(_) => {
                Some(Outcome::Invalid(DiscardReason::SecurityReportType))
            }
            Self::InvalidSecurityReport(_) => Some(Outcome::Invalid(DiscardReason::SecurityReport)),
            Self::UnsupportedSecurityType => Some(Outcome::Filtered(FilterStatKey::InvalidCsp)),
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
            Self::InvalidProcessingGroup(_) => None,

            Self::InvalidReplay(reason) => Some(Outcome::Invalid(*reason)),
            Self::ReplayFiltered(key) => Some(Outcome::Filtered(key.clone())),
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

/// A container for extracted metrics during processing.
///
/// The container enforces that the extracted metrics are correctly tagged
/// with the dynamic sampling decision.
#[derive(Debug)]
pub struct ProcessingExtractedMetrics {
    metrics: ExtractedMetrics,
    project: Arc<ProjectInfo>,
    global: Arc<GlobalConfig>,
    extrapolation_factor: Option<FiniteF64>,
}

impl ProcessingExtractedMetrics {
    pub fn new(
        project: Arc<ProjectInfo>,
        global: Arc<GlobalConfig>,
        dsc: Option<&DynamicSamplingContext>,
    ) -> Self {
        let factor = match dsc.and_then(|dsc| dsc.sample_rate) {
            // no need for extrapolation if sample rate is 1.0, and a sample rate of `0` or lower is
            // invalid, in which case we also skip.
            Some(rate) if rate > 0.0 && rate < 1.0 => FiniteF64::new(1.0 / rate),
            _ => None,
        };

        Self {
            metrics: ExtractedMetrics::default(),
            project,
            global,
            extrapolation_factor: factor,
        }
    }

    /// Extends the contained metrics with [`ExtractedMetrics`].
    pub fn extend(
        &mut self,
        extracted: ExtractedMetrics,
        sampling_decision: Option<SamplingDecision>,
    ) {
        self.extend_project_metrics(extracted.project_metrics, sampling_decision);
        self.extend_sampling_metrics(extracted.sampling_metrics, sampling_decision);
    }

    /// Extends the contained project metrics.
    pub fn extend_project_metrics(
        &mut self,
        mut buckets: Vec<Bucket>,
        sampling_decision: Option<SamplingDecision>,
    ) {
        for bucket in &mut buckets {
            bucket.metadata.extracted_from_indexed =
                sampling_decision == Some(SamplingDecision::Keep);
        }
        self.extrapolate(&mut buckets);
        self.metrics.project_metrics.extend(buckets);
    }

    /// Extends the contained sampling metrics.
    pub fn extend_sampling_metrics(
        &mut self,
        mut buckets: Vec<Bucket>,
        sampling_decision: Option<SamplingDecision>,
    ) {
        for bucket in &mut buckets {
            bucket.metadata.extracted_from_indexed =
                sampling_decision == Some(SamplingDecision::Keep);
        }
        self.extrapolate(&mut buckets);
        self.metrics.sampling_metrics.extend(buckets);
    }

    /// Applies rate limits to the contained metrics.
    ///
    /// This is used to apply rate limits which have been enforced on sampled items of an envelope
    /// to also consistently apply to the metrics extracted from these items.
    #[cfg(feature = "processing")]
    fn apply_enforcement(&mut self, enforcement: &Enforcement) {
        for (namespace, limit) in [
            (MetricNamespace::Transactions, &enforcement.event),
            (MetricNamespace::Spans, &enforcement.spans),
        ] {
            if limit.is_active() {
                relay_log::trace!(
                    "dropping {namespace} metrics, due to enforced limit on envelope"
                );
                self.retain(|bucket| bucket.name.try_namespace() != Some(namespace));
            }
        }
    }

    #[cfg(feature = "processing")]
    fn retain(&mut self, mut f: impl FnMut(&Bucket) -> bool) {
        self.metrics.project_metrics.retain(&mut f);
        self.metrics.sampling_metrics.retain(&mut f);
    }

    fn extrapolate(&self, buckets: &mut [Bucket]) {
        let Some(factor) = self.extrapolation_factor else {
            return;
        };

        let extrapolate = match self.project.config().metric_extraction {
            ErrorBoundary::Ok(ref config) if !config.extrapolate.is_empty() => &config.extrapolate,
            _ => return,
        };

        let duplication = (factor.to_f64().round() as usize)
            .min(self.global.options.extrapolation_duplication_limit)
            .max(1);

        for bucket in buckets {
            if !extrapolate.matches(&bucket.name) {
                continue;
            }

            match bucket.value {
                BucketValue::Counter(ref mut counter) => {
                    *counter = counter.saturating_mul(factor);
                }
                BucketValue::Distribution(ref mut dist) => {
                    // Duplicate values in the distribution to ensure that higher sample rates are
                    // represented correctly in the final distribution sketch. This is inefficient
                    // and there are two ways to optimize this:
                    //  1. Store the sample rate or weight directly in the distribution structure.
                    //     Then duplicate on the fly in the kafka producer.
                    //  2. Change the schema to include sample rates or weights and then extrapolate
                    //     in storage.
                    *dist = std::mem::take(dist)
                        .into_iter()
                        .flat_map(|f| std::iter::repeat(f).take(duplication))
                        .collect();
                }
                BucketValue::Set(_) => {
                    // do nothing for sets
                }
                BucketValue::Gauge(ref mut gauge) => {
                    gauge.sum = gauge.sum.saturating_mul(factor);
                    gauge.count = FiniteF64::new(gauge.count as f64)
                        .map(|f| f.saturating_mul(factor).to_f64() as u64)
                        .unwrap_or(u64::MAX);
                }
            }
        }
    }
}

fn send_metrics(metrics: ExtractedMetrics, envelope: &Envelope, aggregator: &Addr<Aggregator>) {
    let project_key = envelope.meta().public_key();

    let ExtractedMetrics {
        project_metrics,
        sampling_metrics,
    } = metrics;

    if !project_metrics.is_empty() {
        aggregator.send(MergeBuckets {
            project_key,
            buckets: project_metrics,
        });
    }

    if !sampling_metrics.is_empty() {
        // If no sampling project state is available, we associate the sampling
        // metrics with the current project.
        //
        // project_without_tracing         -> metrics goes to self
        // dependent_project_with_tracing  -> metrics goes to root
        // root_project_with_tracing       -> metrics goes to root == self
        let sampling_project_key = envelope.sampling_key().unwrap_or(project_key);
        aggregator.send(MergeBuckets {
            project_key: sampling_project_key,
            buckets: sampling_metrics,
        });
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

    /// Track whether spans and span metrics were already extracted.
    ///
    /// Only applies to envelopes with a transaction item.
    spans_extracted: bool,

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

    /// Metrics extracted from items in the envelope.
    ///
    /// Relay can extract metrics for sessions and transactions, which is controlled by
    /// configuration objects in the project config.
    extracted_metrics: ProcessingExtractedMetrics,

    /// The state of the project that this envelope belongs to.
    project_state: Arc<ProjectInfo>,

    /// The config of this Relay instance.
    config: Arc<Config>,

    /// The state of the project that initiated the current trace.
    /// This is the config used for trace-based dynamic sampling.
    sampling_project_state: Option<Arc<ProjectInfo>>,

    /// The id of the project that this envelope is ingested into.
    ///
    /// This identifier can differ from the one stated in the Envelope's DSN if the key was moved to
    /// a new project or on the legacy endpoint. In that case, normalization will update the project
    /// ID.
    project_id: ProjectId,

    /// The managed envelope before processing.
    managed_envelope: TypedEnvelope<Group>,

    /// Reservoir evaluator that we use for dynamic sampling.
    reservoir: ReservoirEvaluator<'a>,

    /// Track whether the event has already been fully normalized.
    ///
    /// If the processing pipeline applies changes to the event, it should
    /// disable this flag to ensure the event is always normalized.
    event_fully_normalized: bool,
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

    /// Returns `true` for managed relays if a feature is disabled.
    ///
    /// Some envelope items are dropped based on a feature flag,
    /// but we want to forward them in proxy mode.
    fn feature_disabled_by_upstream(&self, feature: Feature) -> bool {
        match self.config.relay_mode() {
            RelayMode::Proxy | RelayMode::Static | RelayMode::Capture => false,
            RelayMode::Managed => !self.project_state.has_feature(feature),
        }
    }
}

/// The view out of the [`ProcessEnvelopeState`] after processing.
#[derive(Debug)]
struct ProcessingStateResult {
    managed_envelope: TypedEnvelope<Processed>,
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
    pub envelope: Option<TypedEnvelope<Processed>>,
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
    pub project_info: Arc<ProjectInfo>,
    pub sampling_project_info: Option<Arc<ProjectInfo>>,
    pub reservoir_counters: ReservoirCounters,
}

/// Parses a list of metrics or metric buckets and pushes them to the project's aggregator.
///
/// This parses and validates the metrics:
///  - For [`Metrics`](ItemType::Statsd), each metric is parsed separately, and invalid metrics are
///    ignored independently.
///  - For [`MetricBuckets`](ItemType::MetricBuckets), the entire list of buckets is parsed and
///    dropped together on parsing failure.
///  - Other envelope items will be ignored with an error message.
///
/// Additionally, processing applies clock drift correction using the system clock of this Relay, if
/// the Envelope specifies the [`sent_at`](Envelope::sent_at) header.
#[derive(Debug)]
pub struct ProcessProjectMetrics {
    /// The project state the metrics belong to.
    ///
    /// The project state can be pending, in which case cached rate limits
    /// and other project specific operations are skipped and executed once
    /// the project state becomes available.
    pub project_state: ProjectState,
    /// Currently active cached rate limits for this project.
    pub rate_limits: RateLimits,

    /// A list of metric items.
    pub data: MetricData,
    /// The target project.
    pub project_key: ProjectKey,
    /// Whether to keep or reset the metric metadata.
    pub source: BucketSource,
    /// The instant at which the request was received.
    pub start_time: Instant,
    /// The value of the Envelope's [`sent_at`](Envelope::sent_at) header for clock drift
    /// correction.
    pub sent_at: Option<DateTime<Utc>>,
}

/// Raw unparsed metric data.
#[derive(Debug)]
pub enum MetricData {
    /// Raw data, unparsed envelope items.
    Raw(Vec<Item>),
    /// Already parsed buckets but unprocessed.
    Parsed(Vec<Bucket>),
}

impl MetricData {
    /// Consumes the metric data and parses the contained buckets.
    ///
    /// If the contained data is already parsed the buckets are returned unchanged.
    /// Raw buckets are parsed and created with the passed `timestamp`.
    fn into_buckets(self, timestamp: UnixTimestamp) -> Vec<Bucket> {
        let items = match self {
            Self::Parsed(buckets) => return buckets,
            Self::Raw(items) => items,
        };

        let mut buckets = Vec::new();
        for item in items {
            let payload = item.payload();
            if item.ty() == &ItemType::Statsd {
                for bucket_result in Bucket::parse_all(&payload, timestamp) {
                    match bucket_result {
                        Ok(bucket) => buckets.push(bucket),
                        Err(error) => relay_log::debug!(
                            error = &error as &dyn Error,
                            "failed to parse metric bucket from statsd format",
                        ),
                    }
                }
            } else if item.ty() == &ItemType::MetricBuckets {
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
            } else {
                relay_log::error!(
                    "invalid item of type {} passed to ProcessMetrics",
                    item.ty()
                );
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
    /// Project info for extracting quotas.
    pub project_info: Arc<ProjectInfo>,
    /// Currently cached rate limits.
    pub rate_limits: RateLimits,
}

/// Encodes metrics into an envelope ready to be sent upstream.
#[derive(Debug)]
pub struct EncodeMetrics {
    pub partition_key: Option<u64>,
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
    pub envelope: TypedEnvelope<Processed>,
}

/// Sends a client report to the upstream.
#[derive(Debug)]
pub struct SubmitClientReports {
    /// The client report to be sent.
    pub client_reports: Vec<ClientReport>,
    /// Scoping information for the client report.
    pub scoping: Scoping,
}

/// CPU-intensive processing tasks for envelopes.
#[derive(Debug)]
pub enum EnvelopeProcessor {
    ProcessEnvelope(Box<ProcessEnvelope>),
    ProcessProjectMetrics(Box<ProcessProjectMetrics>),
    ProcessBatchedMetrics(Box<ProcessBatchedMetrics>),
    ProcessMetricMeta(Box<ProcessMetricMeta>),
    EncodeMetrics(Box<EncodeMetrics>),
    EncodeMetricMeta(Box<EncodeMetricMeta>),
    SubmitEnvelope(Box<SubmitEnvelope>),
    SubmitClientReports(Box<SubmitClientReports>),
}

impl EnvelopeProcessor {
    /// Returns the name of the message variant.
    pub fn variant(&self) -> &'static str {
        match self {
            EnvelopeProcessor::ProcessEnvelope(_) => "ProcessEnvelope",
            EnvelopeProcessor::ProcessProjectMetrics(_) => "ProcessProjectMetrics",
            EnvelopeProcessor::ProcessBatchedMetrics(_) => "ProcessBatchedMetrics",
            EnvelopeProcessor::ProcessMetricMeta(_) => "ProcessMetricMeta",
            EnvelopeProcessor::EncodeMetrics(_) => "EncodeMetrics",
            EnvelopeProcessor::EncodeMetricMeta(_) => "EncodeMetricMeta",
            EnvelopeProcessor::SubmitEnvelope(_) => "SubmitEnvelope",
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

impl FromMessage<ProcessProjectMetrics> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: ProcessProjectMetrics, _: ()) -> Self {
        Self::ProcessProjectMetrics(Box::new(message))
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

/// Service implementing the [`EnvelopeProcessor`] interface.
///
/// This service handles messages in a worker pool with configurable concurrency.
#[derive(Clone)]
pub struct EnvelopeProcessorService {
    inner: Arc<InnerProcessor>,
}

/// Contains the addresses of services that the processor publishes to.
pub struct Addrs {
    pub project_cache: Addr<ProjectCache>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub upstream_relay: Addr<UpstreamRelay>,
    pub test_store: Addr<TestStore>,
    #[cfg(feature = "processing")]
    pub store_forwarder: Option<Addr<Store>>,
    pub aggregator: Addr<Aggregator>,
}

impl Default for Addrs {
    fn default() -> Self {
        Addrs {
            project_cache: Addr::dummy(),
            outcome_aggregator: Addr::dummy(),
            upstream_relay: Addr::dummy(),
            test_store: Addr::dummy(),
            #[cfg(feature = "processing")]
            store_forwarder: None,
            aggregator: Addr::dummy(),
        }
    }
}

struct InnerProcessor {
    workers: WorkerGroup,
    config: Arc<Config>,
    global_config: GlobalConfigHandle,
    cogs: Cogs,
    #[cfg(feature = "processing")]
    quotas_pool: Option<RedisPool>,
    addrs: Addrs,
    #[cfg(feature = "processing")]
    rate_limiter: Option<RedisRateLimiter>,
    geoip_lookup: Option<GeoIpLookup>,
    #[cfg(feature = "processing")]
    metric_meta_store: Option<RedisMetricMetaStore>,
    #[cfg(feature = "processing")]
    cardinality_limiter: Option<CardinalityLimiter>,
    metric_outcomes: MetricOutcomes,
}

impl EnvelopeProcessorService {
    /// Creates a multi-threaded envelope processor.
    pub fn new(
        pool: ThreadPool,
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        cogs: Cogs,
        #[cfg(feature = "processing")] redis: Option<RedisPools>,
        addrs: Addrs,
        metric_outcomes: MetricOutcomes,
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

        #[cfg(feature = "processing")]
        let (cardinality, quotas, misc) = match redis {
            Some(RedisPools {
                cardinality,
                quotas,
                misc,
                ..
            }) => (Some(cardinality), Some(quotas), Some(misc)),
            None => (None, None, None),
        };

        let inner = InnerProcessor {
            workers: WorkerGroup::new(pool),
            global_config,
            cogs,
            #[cfg(feature = "processing")]
            quotas_pool: quotas.clone(),
            #[cfg(feature = "processing")]
            rate_limiter: quotas
                .map(|quotas| RedisRateLimiter::new(quotas).max_limit(config.max_rate_limit())),
            addrs,
            geoip_lookup,
            #[cfg(feature = "processing")]
            metric_meta_store: misc.map(|misc| {
                RedisMetricMetaStore::new(misc, config.metrics_meta_locations_expiry())
            }),
            #[cfg(feature = "processing")]
            cardinality_limiter: cardinality
                .map(|cardinality| {
                    RedisSetLimiter::new(
                        RedisSetLimiterOptions {
                            cache_vacuum_interval: config
                                .cardinality_limiter_cache_vacuum_interval(),
                        },
                        cardinality,
                    )
                })
                .map(CardinalityLimiter::new),
            metric_outcomes,
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
    #[allow(clippy::too_many_arguments)]
    fn prepare_state<G>(
        &self,
        config: Arc<Config>,
        global_config: Arc<GlobalConfig>,
        mut managed_envelope: TypedEnvelope<G>,
        project_id: ProjectId,
        project_state: Arc<ProjectInfo>,
        sampling_project_state: Option<Arc<ProjectInfo>>,
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

        // Only trust item headers in envelopes coming from internal relays
        let event_fully_normalized = envelope.meta().is_from_internal_relay()
            && envelope
                .items()
                .any(|item| item.creates_event() && item.fully_normalized());

        #[allow(unused_mut)]
        let mut reservoir = ReservoirEvaluator::new(reservoir_counters);
        #[cfg(feature = "processing")]
        if let Some(quotas_pool) = self.inner.quotas_pool.as_ref() {
            let org_id = managed_envelope.scoping().organization_id;
            reservoir.set_redis(org_id, quotas_pool);
        }

        let extracted_metrics = ProcessingExtractedMetrics::new(
            project_state.clone(),
            global_config,
            // The processor sometimes computes a DSC just-in-time for dynamic sampling if it is not
            // provided by the SDK so that trace rules can still match. Metric extraction won't see
            // this generated DSC. However, metric extraction just needs the client sample rate,
            // which is never affected by this. Hence, this operation is safe.
            managed_envelope.envelope().dsc(),
        );

        ProcessEnvelopeState {
            event: Annotated::empty(),
            event_metrics_extracted: false,
            spans_extracted: false,
            metrics: Metrics::default(),
            sample_rates: None,
            extracted_metrics,
            project_state,
            config,
            sampling_project_state,
            project_id,
            managed_envelope,
            reservoir,
            event_fully_normalized,
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
        let global_config = self.inner.global_config.current();
        let quotas = CombinedQuotas::new(&global_config, project_state.get_quotas());

        if quotas.is_empty() {
            return Ok(());
        }

        let event_category = state.event_category();

        // When invoking the rate limiter, capture if the event item has been rate limited to also
        // remove it from the processing state eventually.
        let mut envelope_limiter = EnvelopeLimiter::new(|item_scope, quantity| {
            rate_limiter.is_rate_limited(quotas, item_scope, quantity, false)
        });

        // Tell the envelope limiter about the event, since it has been removed from the Envelope at
        // this stage in processing.
        if let Some(category) = event_category {
            envelope_limiter.assume_event(category);
        }

        let scoping = state.managed_envelope.scoping();
        let (enforcement, limits) = metric!(timer(RelayTimers::EventProcessingRateLimiting), {
            envelope_limiter.compute(state.managed_envelope.envelope_mut(), &scoping)?
        });
        let event_active = enforcement.is_event_active();

        // Use the same rate limits as used for the envelope on the metrics.
        // Those rate limits should not be checked for expiry or similar to ensure a consistent
        // limiting of envelope items and metrics.
        state.extracted_metrics.apply_enforcement(&enforcement);
        enforcement.apply_with_outcomes(&mut state.managed_envelope);

        if event_active {
            state.remove_event();
            debug_assert!(state.envelope().is_empty());
        }

        if !limits.is_empty() {
            self.inner
                .addrs
                .project_cache
                .send(UpdateRateLimits::new(scoping.project_key, limits));
        }

        Ok(())
    }

    /// Extract transaction metrics.
    fn extract_transaction_metrics(
        &self,
        state: &mut ProcessEnvelopeState<TransactionGroup>,
        sampling_decision: SamplingDecision,
        profile_id: Option<ProfileId>,
    ) -> Result<(), ProcessingError> {
        if state.event_metrics_extracted {
            return Ok(());
        }
        let Some(event) = state.event.value_mut() else {
            return Ok(());
        };

        // NOTE: This function requires a `metric_extraction` in the project config. Legacy configs
        // will upsert this configuration from transaction and conditional tagging fields, even if
        // it is not present in the actual project config payload.
        let global = self.inner.global_config.current();
        let combined_config = {
            let config = match &state.project_state.config.metric_extraction {
                ErrorBoundary::Ok(ref config) if config.is_supported() => config,
                _ => return Ok(()),
            };
            let global_config = match &global.metric_extraction {
                ErrorBoundary::Ok(global_config) => global_config,
                #[allow(unused_variables)]
                ErrorBoundary::Err(e) => {
                    if_processing!(self.inner.config, {
                        // Config is invalid, but we will try to extract what we can with just the
                        // project config.
                        relay_log::error!("Failed to parse global extraction config {e}");
                        MetricExtractionGroups::EMPTY
                    } else {
                        // If there's an error with global metrics extraction, it is safe to assume that this
                        // Relay instance is not up-to-date, and we should skip extraction.
                        relay_log::debug!("Failed to parse global extraction config: {e}");
                        return Ok(());
                    })
                }
            };
            CombinedMetricExtractionConfig::new(global_config, config)
        };

        // Require a valid transaction metrics config.
        let tx_config = match &state.project_state.config.transaction_metrics {
            Some(ErrorBoundary::Ok(tx_config)) => tx_config,
            Some(ErrorBoundary::Err(e)) => {
                relay_log::debug!("Failed to parse legacy transaction metrics config: {e}");
                return Ok(());
            }
            None => {
                relay_log::debug!("Legacy transaction metrics config is missing");
                return Ok(());
            }
        };

        if !tx_config.is_enabled() {
            static TX_CONFIG_ERROR: Once = Once::new();
            TX_CONFIG_ERROR.call_once(|| {
                if self.inner.config.processing_enabled() {
                    relay_log::error!(
                        "Processing Relay outdated, received tx config in version {}, which is not supported",
                        tx_config.version
                    );
                }
            });

            return Ok(());
        }

        let metrics = crate::metrics_extraction::event::extract_metrics(
            event,
            state.spans_extracted,
            combined_config,
            self.inner
                .config
                .aggregator_config_for(MetricNamespace::Spans)
                .aggregator
                .max_tag_value_length,
            global.options.span_extraction_sample_rate,
        );

        state
            .extracted_metrics
            .extend_project_metrics(metrics, Some(sampling_decision));

        if !state.project_state.has_feature(Feature::DiscardTransaction) {
            let transaction_from_dsc = state
                .managed_envelope
                .envelope()
                .dsc()
                .and_then(|dsc| dsc.transaction.as_deref());

            let extractor = TransactionExtractor {
                config: tx_config,
                generic_config: Some(combined_config),
                transaction_from_dsc,
                sampling_decision,
                has_profile: profile_id.is_some(),
            };

            state
                .extracted_metrics
                .extend(extractor.extract(event)?, Some(sampling_decision));
        }

        state.event_metrics_extracted = true;

        Ok(())
    }

    fn normalize_event<G: EventProcessing>(
        &self,
        state: &mut ProcessEnvelopeState<G>,
    ) -> Result<(), ProcessingError> {
        if !state.has_event() {
            // NOTE(iker): only processing relays create events from
            // attachments, so these events won't be normalized in
            // non-processing relays even if the config is set to run full
            // normalization.
            return Ok(());
        }

        let full_normalization = match self.inner.config.normalization_level() {
            NormalizationLevel::Full => true,
            NormalizationLevel::Default => {
                if self.inner.config.processing_enabled() && state.event_fully_normalized {
                    return Ok(());
                }

                self.inner.config.processing_enabled()
            }
        };

        let request_meta = state.managed_envelope.envelope().meta();
        let client_ipaddr = request_meta.client_addr().map(IpAddr::from);

        let transaction_aggregator_config = self
            .inner
            .config
            .aggregator_config_for(MetricNamespace::Transactions);

        let global_config = self.inner.global_config.current();
        let ai_model_costs = global_config.ai_model_costs.clone().ok();
        let http_span_allowed_hosts = global_config.options.http_span_allowed_hosts.as_slice();

        utils::log_transaction_name_metrics(&mut state.event, |event| {
            let event_validation_config = EventValidationConfig {
                received_at: Some(state.managed_envelope.received_at()),
                max_secs_in_past: Some(self.inner.config.max_secs_in_past()),
                max_secs_in_future: Some(self.inner.config.max_secs_in_future()),
                transaction_timestamp_range: Some(
                    transaction_aggregator_config.aggregator.timestamp_range(),
                ),
                is_validated: false,
            };

            let key_id = state
                .project_state
                .get_public_key_config()
                .and_then(|key| Some(key.numeric_id?.to_string()));
            if full_normalization && key_id.is_none() {
                relay_log::error!(
                    "project state for key {} is missing key id",
                    state.managed_envelope.envelope().meta().public_key()
                );
            }

            let normalization_config = NormalizationConfig {
                project_id: Some(state.project_id.value()),
                client: request_meta.client().map(str::to_owned),
                key_id,
                protocol_version: Some(request_meta.version().to_string()),
                grouping_config: state.project_state.config.grouping_config.clone(),
                client_ip: client_ipaddr.as_ref(),
                client_sample_rate: state
                    .managed_envelope
                    .envelope()
                    .dsc()
                    .and_then(|ctx| ctx.sample_rate),
                user_agent: RawUserAgentInfo {
                    user_agent: request_meta.user_agent(),
                    client_hints: request_meta.client_hints().as_deref(),
                },
                max_name_and_unit_len: Some(
                    transaction_aggregator_config
                        .aggregator
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
                    .has_feature(Feature::ExtractSpansFromEvent)
                    || state
                        .project_state
                        .has_feature(Feature::ExtractCommonSpanMetricsFromEvent),
                max_tag_value_length: self
                    .inner
                    .config
                    .aggregator_config_for(MetricNamespace::Spans)
                    .aggregator
                    .max_tag_value_length,
                is_renormalize: false,
                remove_other: full_normalization,
                emit_event_errors: full_normalization,
                span_description_rules: state.project_state.config.span_description_rules.as_ref(),
                geoip_lookup: self.inner.geoip_lookup.as_ref(),
                ai_model_costs: ai_model_costs.as_ref(),
                enable_trimming: true,
                measurements: Some(CombinedMeasurementsConfig::new(
                    state.project_state.config().measurements.as_ref(),
                    global_config.measurements.as_ref(),
                )),
                normalize_spans: true,
                replay_id: state
                    .managed_envelope
                    .envelope()
                    .dsc()
                    .and_then(|ctx| ctx.replay_id),
                span_allowed_hosts: http_span_allowed_hosts,
            };

            metric!(timer(RelayTimers::EventProcessingNormalization), {
                validate_event(event, &event_validation_config)
                    .map_err(|_| ProcessingError::InvalidTransaction)?;
                normalize_event(event, &normalization_config);
                if full_normalization && event::has_unprintable_fields(event) {
                    metric!(counter(RelayCounters::EventCorrupted) += 1);
                }
                Result::<(), ProcessingError>::Ok(())
            })
        })?;

        state.event_fully_normalized |= full_normalization;

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

    /// Processes only transactions and transaction-related items.
    fn process_transactions(
        &self,
        state: &mut ProcessEnvelopeState<TransactionGroup>,
    ) -> Result<(), ProcessingError> {
        let global_config = self.inner.global_config.current();

        event::extract(state, &self.inner.config)?;

        let profile_id = profile::filter(state);
        profile::transfer_id(state, profile_id);

        if_processing!(self.inner.config, {
            attachment::create_placeholders(state);
        });

        event::finalize(state, &self.inner.config)?;
        self.normalize_event(state)?;

        dynamic_sampling::ensure_dsc(state);

        let filter_run = event::filter(state, &self.inner.global_config.current())?;

        // Always run dynamic sampling on processing Relays,
        // but delay decision until inbound filters have been fully processed.
        let run_dynamic_sampling =
            matches!(filter_run, FiltersStatus::Ok) || self.inner.config.processing_enabled();

        let sampling_result = match run_dynamic_sampling {
            true => dynamic_sampling::run(state, &self.inner.config),
            false => SamplingResult::Pending,
        };

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            let keep_profiles = dynamic_sampling::forward_unsampled_profiles(state, &global_config);
            // Process profiles before dropping the transaction, if necessary.
            // Before metric extraction to make sure the profile count is reflected correctly.
            let profile_id = match keep_profiles {
                true => profile::process(state, &self.inner.config),
                false => profile_id,
            };

            // Extract metrics here, we're about to drop the event/transaction.
            self.extract_transaction_metrics(state, SamplingDecision::Drop, profile_id)?;

            dynamic_sampling::drop_unsampled_items(state, outcome, keep_profiles);

            // At this point we have:
            //  - An empty envelope.
            //  - An envelope containing only processed profiles.
            // We need to make sure there are enough quotas for these profiles.
            if_processing!(self.inner.config, {
                self.enforce_quotas(state)?;
            });

            return Ok(());
        }

        // Need to scrub the transaction before extracting spans.
        //
        // Unconditionally scrub to make sure PII is removed as early as possible.
        event::scrub(state)?;
        attachment::scrub(state);

        if_processing!(self.inner.config, {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            let profile_id = profile::process(state, &self.inner.config);
            profile::transfer_id(state, profile_id);

            // Always extract metrics in processing Relays for sampled items.
            self.extract_transaction_metrics(state, SamplingDecision::Keep, profile_id)?;

            if state
                .project_state
                .has_feature(Feature::ExtractSpansFromEvent)
            {
                span::extract_from_event(state, &self.inner.config, &global_config);
            }

            self.enforce_quotas(state)?;

            span::maybe_discard_transaction(state);
        });

        // Event may have been dropped because of a quota and the envelope can be empty.
        if state.has_event() {
            event::serialize(state)?;
        }

        if self.inner.config.processing_enabled() && !state.event_fully_normalized {
            relay_log::error!(
                tags.project = %state.project_id,
                tags.ty = state.event_type().map(|e| e.to_string()).unwrap_or("none".to_owned()),
                "ingested event without normalizing"
            );
        };

        Ok(())
    }

    fn process_profile_chunks(
        &self,
        state: &mut ProcessEnvelopeState<ProfileChunkGroup>,
    ) -> Result<(), ProcessingError> {
        profile_chunk::filter(state);
        if_processing!(self.inner.config, {
            profile_chunk::process(state, &self.inner.config);
        });
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
            self.inner.addrs.outcome_aggregator.clone(),
        );

        Ok(())
    }

    /// Processes replays.
    fn process_replays(
        &self,
        state: &mut ProcessEnvelopeState<ReplayGroup>,
    ) -> Result<(), ProcessingError> {
        replay::process(
            state,
            &self.inner.config,
            &self.inner.global_config.current(),
        )?;
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

    /// Processes standalone spans.
    ///
    /// This function does *not* run for spans extracted from transactions.
    fn process_standalone_spans(
        &self,
        state: &mut ProcessEnvelopeState<SpanGroup>,
    ) -> Result<(), ProcessingError> {
        span::filter(state);

        if_processing!(self.inner.config, {
            let global_config = self.inner.global_config.current();

            span::process(state, self.inner.config.clone(), &global_config);

            self.enforce_quotas(state)?;
        });
        Ok(())
    }

    fn process_envelope(
        &self,
        mut managed_envelope: ManagedEnvelope,
        project_id: ProjectId,
        project_state: Arc<ProjectInfo>,
        sampling_project_state: Option<Arc<ProjectInfo>>,
        reservoir_counters: Arc<Mutex<BTreeMap<RuleId, i64>>>,
    ) -> Result<ProcessingStateResult, ProcessingError> {
        // Get the group from the managed envelope context, and if it's not set, try to guess it
        // from the contents of the envelope.
        let group = managed_envelope.group();

        // Pre-process the envelope headers.
        if let Some(sampling_state) = sampling_project_state.as_ref() {
            // Both transactions and standalone span envelopes need a normalized DSC header
            // to make sampling rules based on the segment/transaction name work correctly.
            managed_envelope
                .envelope_mut()
                .parametrize_dsc_transaction(&sampling_state.config.tx_name_rules);
        }

        macro_rules! run {
            ($fn:ident) => {{
                let managed_envelope = managed_envelope.try_into()?;
                let mut state = self.prepare_state(
                    self.inner.config.clone(),
                    self.inner.global_config.current(),
                    managed_envelope,
                    project_id,
                    project_state,
                    sampling_project_state,
                    reservoir_counters,
                );
                match self.$fn(&mut state) {
                    Ok(()) => Ok(ProcessingStateResult {
                        managed_envelope: state.managed_envelope.into_processed(),
                        extracted_metrics: state.extracted_metrics.metrics,
                    }),
                    Err(e) => {
                        if let Some(outcome) = e.to_outcome() {
                            state.managed_envelope.reject(outcome);
                        }
                        return Err(e);
                    }
                }
            }};
        }

        relay_log::trace!("Processing {group} group", group = group.variant());

        match group {
            ProcessingGroup::Error => run!(process_errors),
            ProcessingGroup::Transaction => run!(process_transactions),
            ProcessingGroup::Session => run!(process_sessions),
            ProcessingGroup::Standalone => run!(process_standalone),
            ProcessingGroup::ClientReport => run!(process_client_reports),
            ProcessingGroup::Replay => run!(process_replays),
            ProcessingGroup::CheckIn => run!(process_checkins),
            ProcessingGroup::Span => run!(process_standalone_spans),
            ProcessingGroup::ProfileChunk => run!(process_profile_chunks),
            // Currently is not used.
            ProcessingGroup::Metrics => {
                // In proxy mode we simply forward the metrics.
                // This group shouldn't be used outside of proxy mode.
                if self.inner.config.relay_mode() != RelayMode::Proxy {
                    relay_log::error!(
                        tags.project = %project_id,
                        items = ?managed_envelope.envelope().items().next().map(Item::ty),
                        "received metrics in the process_state"
                    );
                }

                Ok(ProcessingStateResult {
                    managed_envelope: managed_envelope.into_processed(),
                    extracted_metrics: Default::default(),
                })
            }
            // Fallback to the legacy process_state implementation for Ungrouped events.
            ProcessingGroup::Ungrouped => {
                relay_log::error!(
                    tags.project = %project_id,
                    items = ?managed_envelope.envelope().items().next().map(Item::ty),
                    "could not identify the processing group based on the envelope's items"
                );
                Ok(ProcessingStateResult {
                    managed_envelope: managed_envelope.into_processed(),
                    extracted_metrics: Default::default(),
                })
            }
            // Leave this group unchanged.
            //
            // This will later be forwarded to upstream.
            ProcessingGroup::ForwardUnknown => Ok(ProcessingStateResult {
                managed_envelope: managed_envelope.into_processed(),
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
            project_info,
            sampling_project_info,
            reservoir_counters,
        } = message;

        // Prefer the project's project ID, and fall back to the stated project id from the
        // envelope. The project ID is available in all modes, other than in proxy mode, where
        // envelopes for unknown projects are forwarded blindly.
        //
        // Neither ID can be available in proxy mode on the /store/ endpoint. This is not supported,
        // since we cannot process an envelope without project ID, so drop it.
        let project_id = match project_info
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
                    project_info,
                    sampling_project_info,
                    reservoir_counters,
                ) {
                    Ok(mut state) => {
                        // The envelope could be modified or even emptied during processing, which
                        // requires recomputation of the context.
                        state.managed_envelope.update();

                        let has_metrics = !state.extracted_metrics.project_metrics.is_empty();
                        send_metrics(
                            state.extracted_metrics,
                            state.managed_envelope.envelope(),
                            &self.inner.addrs.aggregator,
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
                    Err(err) => Err(err),
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

    fn handle_process_project_metrics(&self, cogs: &mut Token, message: ProcessProjectMetrics) {
        let ProcessProjectMetrics {
            project_state,
            rate_limits,
            data,
            project_key,
            start_time,
            sent_at,
            source,
        } = message;

        let received_timestamp = UnixTimestamp::from_instant(start_time);

        let mut buckets = data.into_buckets(received_timestamp);
        if buckets.is_empty() {
            return;
        };
        cogs.update(relay_metrics::cogs::BySize(&buckets));

        let received = relay_common::time::instant_to_date_time(start_time);
        let clock_drift_processor =
            ClockDriftProcessor::new(sent_at, received).at_least(MINIMUM_CLOCK_DRIFT);

        for bucket in &mut buckets {
            clock_drift_processor.process_timestamp(&mut bucket.timestamp);
            if !matches!(source, BucketSource::Internal) {
                bucket.metadata = BucketMetadata::new(received_timestamp);
            }
        }

        let buckets = self::metrics::filter_namespaces(buckets, source);

        // Best effort check to filter and rate limit buckets, if there is no project state
        // available at the current time, we will check again after flushing.
        let buckets = match project_state.enabled() {
            Some(project_info) => {
                self.check_buckets(project_key, &project_info, &rate_limits, buckets)
            }
            None => buckets,
        };

        relay_log::trace!("merging metric buckets into the aggregator");
        self.inner
            .addrs
            .aggregator
            .send(MergeBuckets::new(project_key, buckets));
    }

    fn handle_process_batched_metrics(&self, cogs: &mut Token, message: ProcessBatchedMetrics) {
        let ProcessBatchedMetrics {
            payload,
            source,
            start_time,
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

        let mut feature_weights = FeatureWeights::none();
        for (project_key, buckets) in buckets {
            feature_weights = feature_weights.merge(relay_metrics::cogs::BySize(&buckets).into());

            self.inner.addrs.project_cache.send(ProcessMetrics {
                data: MetricData::Parsed(buckets),
                project_key,
                source,
                start_time: start_time.into(),
                sent_at,
            });
        }

        if !feature_weights.is_empty() {
            cogs.update(feature_weights);
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

            let payload = item.payload();
            match serde_json::from_slice::<MetricMeta>(&payload) {
                Ok(meta) => {
                    relay_log::trace!("adding metric metadata to project cache");
                    self.inner
                        .addrs
                        .project_cache
                        .send(AddMetricMeta { project_key, meta });
                }
                Err(error) => {
                    metric!(counter(RelayCounters::MetricMetaParsingFailed) += 1);
                    relay_log::debug!(error = &error as &dyn Error, "failed to parse metric meta");
                }
            }
        }
    }

    fn handle_submit_envelope(&self, message: SubmitEnvelope) {
        let SubmitEnvelope { mut envelope } = message;

        #[cfg(feature = "processing")]
        if self.inner.config.processing_enabled() {
            if let Some(store_forwarder) = self.inner.addrs.store_forwarder.clone() {
                relay_log::trace!("sending envelope to kafka");
                store_forwarder.send(StoreEnvelope { envelope });
                return;
            }
        }

        // If we are in capture mode, we stash away the event instead of forwarding it.
        if Capture::should_capture(&self.inner.config) {
            relay_log::trace!("capturing envelope in memory");
            self.inner
                .addrs
                .test_store
                .send(Capture::accepted(envelope));
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
                self.inner
                    .addrs
                    .upstream_relay
                    .send(SendRequest(SendEnvelope {
                        envelope,
                        body,
                        http_encoding,
                        project_cache: self.inner.addrs.project_cache.clone(),
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

        let envelope = ManagedEnvelope::new(
            envelope,
            self.inner.addrs.outcome_aggregator.clone(),
            self.inner.addrs.test_store.clone(),
            ProcessingGroup::ClientReport,
        );
        self.handle_submit_envelope(SubmitEnvelope {
            envelope: envelope.into_processed(),
        });
    }

    fn check_buckets(
        &self,
        project_key: ProjectKey,
        project_info: &ProjectInfo,
        rate_limits: &RateLimits,
        buckets: Vec<Bucket>,
    ) -> Vec<Bucket> {
        let Some(scoping) = project_info.scoping(project_key) else {
            relay_log::error!(
                tags.project_key = project_key.as_str(),
                "there is no scoping: dropping {} buckets",
                buckets.len(),
            );
            return Vec::new();
        };

        let mut buckets = self::metrics::apply_project_info(
            buckets,
            &self.inner.metric_outcomes,
            project_info,
            scoping,
        );

        let namespaces: BTreeSet<MetricNamespace> = buckets
            .iter()
            .filter_map(|bucket| bucket.name.try_namespace())
            .collect();

        for namespace in namespaces {
            let limits = rate_limits.check_with_quotas(
                project_info.get_quotas(),
                scoping.item(DataCategory::MetricBucket),
            );

            if limits.is_limited() {
                let rejected;
                (buckets, rejected) = utils::split_off(buckets, |bucket| {
                    bucket.name.try_namespace() == Some(namespace)
                });

                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                self.inner.metric_outcomes.track(
                    scoping,
                    &rejected,
                    Outcome::RateLimited(reason_code),
                );
            }
        }

        let quotas = project_info.config.quotas.clone();
        match MetricsLimiter::create(buckets, quotas, scoping) {
            Ok(mut bucket_limiter) => {
                bucket_limiter.enforce_limits(
                    rate_limits,
                    &self.inner.metric_outcomes,
                    &self.inner.addrs.outcome_aggregator,
                );
                bucket_limiter.into_buckets()
            }
            Err(buckets) => buckets,
        }
    }

    #[cfg(feature = "processing")]
    fn rate_limit_buckets(
        &self,
        scoping: Scoping,
        project_state: &ProjectInfo,
        mut buckets: Vec<Bucket>,
    ) -> Vec<Bucket> {
        let Some(rate_limiter) = self.inner.rate_limiter.as_ref() else {
            return buckets;
        };

        let global_config = self.inner.global_config.current();
        let namespaces = buckets
            .iter()
            .filter_map(|bucket| bucket.name.try_namespace())
            .counts();

        let quotas = CombinedQuotas::new(&global_config, project_state.get_quotas());

        for (namespace, quantity) in namespaces {
            let item_scoping = scoping.metric_bucket(namespace);

            let limits = match rate_limiter.is_rate_limited(quotas, item_scoping, quantity, false) {
                Ok(limits) => limits,
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn std::error::Error,
                        "failed to check redis rate limits"
                    );
                    break;
                }
            };

            if limits.is_limited() {
                let rejected;
                (buckets, rejected) = utils::split_off(buckets, |bucket| {
                    bucket.name.try_namespace() == Some(namespace)
                });

                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                self.inner.metric_outcomes.track(
                    scoping,
                    &rejected,
                    Outcome::RateLimited(reason_code),
                );

                self.inner.addrs.project_cache.send(UpdateRateLimits::new(
                    item_scoping.scoping.project_key,
                    limits,
                ));
            }
        }

        match MetricsLimiter::create(buckets, project_state.config.quotas.clone(), scoping) {
            Err(buckets) => buckets,
            Ok(bucket_limiter) => self.apply_other_rate_limits(bucket_limiter),
        }
    }

    /// Check and apply rate limits to metrics buckets for transactions and spans.
    #[cfg(feature = "processing")]
    fn apply_other_rate_limits(&self, mut bucket_limiter: MetricsLimiter) -> Vec<Bucket> {
        relay_log::trace!("handle_rate_limit_buckets");

        let scoping = *bucket_limiter.scoping();

        if let Some(rate_limiter) = self.inner.rate_limiter.as_ref() {
            let global_config = self.inner.global_config.current();
            let quotas = CombinedQuotas::new(&global_config, bucket_limiter.quotas());

            // We set over_accept_once such that the limit is actually reached, which allows subsequent
            // calls with quantity=0 to be rate limited.
            let over_accept_once = true;
            let mut rate_limits = RateLimits::new();

            for category in [DataCategory::Transaction, DataCategory::Span] {
                let count = bucket_limiter.count(category);

                let timer = Instant::now();
                let mut is_limited = false;

                if let Some(count) = count {
                    match rate_limiter.is_rate_limited(
                        quotas,
                        scoping.item(category),
                        count,
                        over_accept_once,
                    ) {
                        Ok(limits) => {
                            is_limited = limits.is_limited();
                            rate_limits.merge(limits)
                        }
                        Err(e) => relay_log::error!(error = &e as &dyn Error),
                    }
                }

                relay_statsd::metric!(
                    timer(RelayTimers::RateLimitBucketsDuration) = timer.elapsed(),
                    category = category.name(),
                    limited = if is_limited { "true" } else { "false" },
                    count = match count {
                        None => "none",
                        Some(0) => "0",
                        Some(1) => "1",
                        Some(1..=10) => "10",
                        Some(1..=25) => "25",
                        Some(1..=50) => "50",
                        Some(51..=100) => "100",
                        Some(101..=500) => "500",
                        _ => "> 500",
                    },
                );
            }

            if rate_limits.is_limited() {
                let was_enforced = bucket_limiter.enforce_limits(
                    &rate_limits,
                    &self.inner.metric_outcomes,
                    &self.inner.addrs.outcome_aggregator,
                );

                if was_enforced {
                    // Update the rate limits in the project cache.
                    self.inner
                        .addrs
                        .project_cache
                        .send(UpdateRateLimits::new(scoping.project_key, rate_limits));
                }
            }
        }

        bucket_limiter.into_buckets()
    }

    /// Cardinality limits the passed buckets and returns a filtered vector of only accepted buckets.
    #[cfg(feature = "processing")]
    fn cardinality_limit_buckets(
        &self,
        scoping: Scoping,
        limits: &[CardinalityLimit],
        buckets: Vec<Bucket>,
    ) -> Vec<Bucket> {
        let global_config = self.inner.global_config.current();
        let cardinality_limiter_mode = global_config.options.cardinality_limiter_mode;

        if matches!(cardinality_limiter_mode, CardinalityLimiterMode::Disabled) {
            return buckets;
        }

        let Some(ref limiter) = self.inner.cardinality_limiter else {
            return buckets;
        };

        let scope = relay_cardinality::Scoping {
            organization_id: scoping.organization_id,
            project_id: scoping.project_id,
        };

        let limits = match limiter.check_cardinality_limits(scope, limits, buckets) {
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
        if !limits.exceeded_limits().is_empty() && sample(error_sample_rate) {
            for limit in limits.exceeded_limits() {
                relay_log::error!(
                    tags.organization_id = scoping.organization_id,
                    tags.limit_id = limit.id,
                    tags.passive = limit.passive,
                    "Cardinality Limit"
                );
            }
        }

        for (limit, reports) in limits.cardinality_reports() {
            for report in reports {
                self.inner
                    .metric_outcomes
                    .cardinality(scoping, limit, report);
            }
        }

        if matches!(cardinality_limiter_mode, CardinalityLimiterMode::Passive) {
            return limits.into_source();
        }

        let CardinalityLimitsSplit { accepted, rejected } = limits.into_split();

        for (bucket, exceeded) in rejected {
            self.inner.metric_outcomes.track(
                scoping,
                &[bucket],
                Outcome::CardinalityLimited(exceeded.id.clone()),
            );
        }
        accepted
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
                project_info,
                rate_limits: _,
            } = message;

            let buckets = self.rate_limit_buckets(scoping, &project_info, buckets);

            let limits = project_info.get_cardinality_limits();
            let buckets = self.cardinality_limit_buckets(scoping, limits, buckets);

            if buckets.is_empty() {
                continue;
            }

            let retention = project_info
                .config
                .event_retention
                .unwrap_or(DEFAULT_EVENT_RETENTION);

            // The store forwarder takes care of bucket splitting internally, so we can submit the
            // entire list of buckets. There is no batching needed here.
            store_forwarder.send(StoreMetrics {
                buckets,
                scoping,
                retention,
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
        let EncodeMetrics {
            partition_key,
            scopes,
        } = message;

        let batch_size = self.inner.config.metrics_max_batch_size_bytes();
        let upstream = self.inner.config.upstream_descriptor();

        for (scoping, message) in scopes {
            let ProjectMetrics { buckets, .. } = message;

            let dsn = PartialDsn::outbound(&scoping, upstream);

            if let Some(key) = partition_key {
                relay_statsd::metric!(histogram(RelayHistograms::PartitionKeys) = key);
            }

            let mut num_batches = 0;
            for batch in BucketsView::from(&buckets).by_size(batch_size) {
                let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn.clone()));

                let mut item = Item::new(ItemType::MetricBuckets);
                item.set_source_quantities(crate::metrics::extract_quantities(batch));
                item.set_payload(ContentType::Json, serde_json::to_vec(&buckets).unwrap());
                envelope.add_item(item);

                let mut envelope = ManagedEnvelope::new(
                    envelope,
                    self.inner.addrs.outcome_aggregator.clone(),
                    self.inner.addrs.test_store.clone(),
                    ProcessingGroup::Metrics,
                );
                envelope.set_partition_key(partition_key).scope(scoping);

                relay_statsd::metric!(
                    histogram(RelayHistograms::BucketsPerBatch) = batch.len() as u64
                );

                self.handle_submit_envelope(SubmitEnvelope {
                    envelope: envelope.into_processed(),
                });
                num_batches += 1;
            }

            relay_statsd::metric!(histogram(RelayHistograms::BatchesPerPartition) = num_batches);
        }
    }

    /// Creates a [`SendMetricsRequest`] and sends it to the upstream relay.
    fn send_global_partition(&self, partition_key: Option<u64>, partition: &mut Partition<'_>) {
        if partition.is_empty() {
            return;
        }

        let (unencoded, project_info) = partition.take();
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
            partition_key: partition_key.map(|k| k.to_string()),
            unencoded,
            encoded,
            project_info,
            http_encoding,
            metric_outcomes: self.inner.metric_outcomes.clone(),
        };

        self.inner.addrs.upstream_relay.send(SendRequest(request));
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
    ///  - submit directly to the upstream
    ///
    /// Cardinality limiting and rate limiting run only in processing Relays as they both require
    /// access to the central Redis instance. Cached rate limits are applied in the project cache
    /// already.
    fn encode_metrics_global(&self, message: EncodeMetrics) {
        let EncodeMetrics {
            partition_key,
            scopes,
        } = message;

        let batch_size = self.inner.config.metrics_max_batch_size_bytes();
        let mut partition = Partition::new(batch_size);
        let mut partition_splits = 0;

        for (scoping, message) in &scopes {
            let ProjectMetrics { buckets, .. } = message;

            for bucket in buckets {
                let mut remaining = Some(BucketView::new(bucket));

                while let Some(bucket) = remaining.take() {
                    if let Some(next) = partition.insert(bucket, *scoping) {
                        // A part of the bucket could not be inserted. Take the partition and submit
                        // it immediately. Repeat until the final part was inserted. This should
                        // always result in a request, otherwise we would enter an endless loop.
                        self.send_global_partition(partition_key, &mut partition);
                        remaining = Some(next);
                        partition_splits += 1;
                    }
                }
            }
        }

        if partition_splits > 0 {
            metric!(histogram(RelayHistograms::PartitionSplits) = partition_splits);
        }

        self.send_global_partition(partition_key, &mut partition);
    }

    fn handle_encode_metrics(&self, mut message: EncodeMetrics) {
        for (scoping, pm) in message.scopes.iter_mut() {
            let buckets = std::mem::take(&mut pm.buckets);
            pm.buckets = self.check_buckets(
                scoping.project_key,
                &pm.project_info,
                &pm.rate_limits,
                buckets,
            );
        }

        #[cfg(feature = "processing")]
        if self.inner.config.processing_enabled() {
            if let Some(ref store_forwarder) = self.inner.addrs.store_forwarder {
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

        let envelope = ManagedEnvelope::new(
            envelope,
            self.inner.addrs.outcome_aggregator.clone(),
            self.inner.addrs.test_store.clone(),
            ProcessingGroup::Metrics,
        );
        self.handle_submit_envelope(SubmitEnvelope {
            envelope: envelope.into_processed(),
        });
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
        let feature_weights = self.feature_weights(&message);

        metric!(timer(RelayTimers::ProcessMessageDuration), message = ty, {
            let mut cogs = self.inner.cogs.timed(ResourceId::Relay, feature_weights);

            match message {
                EnvelopeProcessor::ProcessEnvelope(m) => self.handle_process_envelope(*m),
                EnvelopeProcessor::ProcessProjectMetrics(m) => {
                    self.handle_process_project_metrics(&mut cogs, *m)
                }
                EnvelopeProcessor::ProcessBatchedMetrics(m) => {
                    self.handle_process_batched_metrics(&mut cogs, *m)
                }
                EnvelopeProcessor::ProcessMetricMeta(m) => self.handle_process_metric_meta(*m),
                EnvelopeProcessor::EncodeMetrics(m) => self.handle_encode_metrics(*m),
                EnvelopeProcessor::EncodeMetricMeta(m) => self.handle_encode_metric_meta(*m),
                EnvelopeProcessor::SubmitEnvelope(m) => self.handle_submit_envelope(*m),
                EnvelopeProcessor::SubmitClientReports(m) => self.handle_submit_client_reports(*m),
            }
        });
    }

    fn feature_weights(&self, message: &EnvelopeProcessor) -> FeatureWeights {
        match message {
            EnvelopeProcessor::ProcessEnvelope(v) => AppFeature::from(v.envelope.group()).into(),
            EnvelopeProcessor::ProcessProjectMetrics(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::ProcessBatchedMetrics(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::ProcessMetricMeta(_) => AppFeature::MetricMeta.into(),
            EnvelopeProcessor::EncodeMetrics(v) => v
                .scopes
                .values()
                .map(|s| {
                    if self.inner.config.processing_enabled() {
                        // Processing does not encode the metrics but instead rate and cardinality
                        // limits the metrics, which scales by count and not size.
                        relay_metrics::cogs::ByCount(&s.buckets).into()
                    } else {
                        relay_metrics::cogs::BySize(&s.buckets).into()
                    }
                })
                .fold(FeatureWeights::none(), FeatureWeights::merge),
            EnvelopeProcessor::EncodeMetricMeta(_) => AppFeature::MetricMeta.into(),
            EnvelopeProcessor::SubmitEnvelope(v) => AppFeature::from(v.envelope.group()).into(),
            EnvelopeProcessor::SubmitClientReports(_) => AppFeature::ClientReports.into(),
        }
    }
}

impl Service for EnvelopeProcessorService {
    type Interface = EnvelopeProcessor;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let service = self.clone();
                self.inner
                    .workers
                    .spawn(move || service.handle_message(message))
                    .await;
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
    envelope: TypedEnvelope<Processed>,
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
    project_info: HashMap<ProjectKey, Scoping>,
}

impl<'a> Partition<'a> {
    /// Creates a new partition with the given maximum size in bytes.
    pub fn new(size: usize) -> Self {
        Self {
            max_size: size,
            remaining: size,
            views: HashMap::new(),
            project_info: HashMap::new(),
        }
    }

    /// Inserts a bucket into the partition, splitting it if necessary.
    ///
    /// This function attempts to add the bucket to this partition. If the bucket does not fit
    /// entirely into the partition given its maximum size, the remaining part of the bucket is
    /// returned from this function call.
    ///
    /// If this function returns `Some(_)`, the partition is full and should be submitted to the
    /// upstream immediately. Use [`Self::take`] to retrieve the contents of the
    /// partition. Afterwards, the caller is responsible to call this function again with the
    /// remaining bucket until it is fully inserted.
    pub fn insert(&mut self, bucket: BucketView<'a>, scoping: Scoping) -> Option<BucketView<'a>> {
        let (current, next) = bucket.split(self.remaining, Some(self.max_size));

        if let Some(current) = current {
            self.remaining = self.remaining.saturating_sub(current.estimated_size());
            self.views
                .entry(scoping.project_key)
                .or_default()
                .push(current);

            self.project_info
                .entry(scoping.project_key)
                .or_insert(scoping);
        }

        next
    }

    /// Returns `true` if the partition does not hold any data.
    fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// Returns the serialized buckets for this partition.
    ///
    /// This empties the partition, so that it can be reused.
    fn take(&mut self) -> (Bytes, HashMap<ProjectKey, Scoping>) {
        #[derive(serde::Serialize)]
        struct Wrapper<'a> {
            buckets: &'a HashMap<ProjectKey, Vec<BucketView<'a>>>,
        }

        let buckets = &self.views;
        let payload = serde_json::to_vec(&Wrapper { buckets }).unwrap().into();

        let scopings = self.project_info.clone();
        self.project_info.clear();

        self.views.clear();
        self.remaining = self.max_size;

        (payload, scopings)
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
    /// Mapping of all contained project keys to their scoping and extraction mode.
    ///
    /// Used to track outcomes for transmission failures.
    project_info: HashMap<ProjectKey, Scoping>,
    /// Encoding (compression) of the payload.
    http_encoding: HttpEncoding,
    /// Metric outcomes instance to send outcomes on error.
    metric_outcomes: MetricOutcomes,
}

impl SendMetricsRequest {
    fn create_error_outcomes(self) {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            buckets: HashMap<ProjectKey, Vec<MinimalTrackableBucket>>,
        }

        let buckets = match serde_json::from_slice(&self.unencoded) {
            Ok(Wrapper { buckets }) => buckets,
            Err(err) => {
                relay_log::error!(
                    error = &err as &dyn std::error::Error,
                    "failed to parse buckets from failed transmission"
                );
                return;
            }
        };

        for (key, buckets) in buckets {
            let Some(&scoping) = self.project_info.get(&key) else {
                relay_log::error!("missing scoping for project key");
                continue;
            };

            self.metric_outcomes.track(
                scoping,
                &buckets,
                Outcome::Invalid(DiscardReason::Internal),
            );
        }
    }
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
                Err(error) => {
                    relay_log::error!(error = &error as &dyn Error, "Failed to send metrics batch");

                    // If the request did not arrive at the upstream, we are responsible for outcomes.
                    // Otherwise, the upstream is responsible to log outcomes.
                    if error.is_received() {
                        return;
                    }

                    self.create_error_outcomes()
                }
            }
        })
    }
}

/// Container for global and project level [`Quota`].
#[cfg(feature = "processing")]
#[derive(Copy, Clone)]
struct CombinedQuotas<'a> {
    global_quotas: &'a [Quota],
    project_quotas: &'a [Quota],
}

#[cfg(feature = "processing")]
impl<'a> CombinedQuotas<'a> {
    /// Returns a new [`CombinedQuotas`].
    pub fn new(global_config: &'a GlobalConfig, project_quotas: &'a [Quota]) -> Self {
        Self {
            global_quotas: &global_config.quotas,
            project_quotas,
        }
    }

    /// Returns `true` if both global quotas and project quotas are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of both global and project quotas.
    pub fn len(&self) -> usize {
        self.global_quotas.len() + self.project_quotas.len()
    }
}

#[cfg(feature = "processing")]
impl<'a> IntoIterator for CombinedQuotas<'a> {
    type Item = &'a Quota;
    type IntoIter = Chain<Iter<'a, Quota>, Iter<'a, Quota>>;

    fn into_iter(self) -> Self::IntoIter {
        self.global_quotas.iter().chain(self.project_quotas.iter())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use insta::assert_debug_snapshot;
    use relay_base_schema::metrics::{DurationUnit, MetricUnit};
    use relay_common::glob2::LazyGlob;
    use relay_dynamic_config::{ExtrapolationConfig, MetricExtractionConfig, ProjectConfig};
    use relay_event_normalization::{RedactionRule, TransactionNameRule};
    use relay_event_schema::protocol::TransactionSource;
    use relay_pii::DataScrubbingConfig;
    use serde::Deserialize;
    use similar_asserts::assert_eq;

    use crate::metrics_extraction::transactions::types::{
        CommonTags, TransactionMeasurementTags, TransactionMetric,
    };
    use crate::metrics_extraction::IntoMetric;
    use crate::testutils::{self, create_test_processor, create_test_processor_with_addrs};

    #[cfg(feature = "processing")]
    use {
        relay_metrics::BucketValue,
        relay_quotas::{QuotaScope, ReasonCode},
        relay_test::mock_service,
    };

    use super::*;

    #[cfg(feature = "processing")]
    fn mock_quota(id: &str) -> Quota {
        Quota {
            id: Some(id.into()),
            categories: smallvec::smallvec![DataCategory::MetricBucket],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
        }
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_dynamic_quotas() {
        let global_config = GlobalConfig {
            quotas: vec![mock_quota("foo"), mock_quota("bar")],
            ..Default::default()
        };

        let project_quotas = vec![mock_quota("baz"), mock_quota("qux")];

        let dynamic_quotas = CombinedQuotas::new(&global_config, &project_quotas);

        assert_eq!(dynamic_quotas.len(), 4);
        assert!(!dynamic_quotas.is_empty());

        let quota_ids = dynamic_quotas.into_iter().filter_map(|q| q.id.as_deref());
        assert!(quota_ids.eq(["foo", "bar", "baz", "qux"]));
    }

    /// Ensures that if we ratelimit one batch of buckets in [`EncodeMetrics`] message, it won't
    /// also ratelimit the next batches in the same message automatically.
    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_ratelimit_per_batch() {
        let rate_limited_org = 1;
        let not_ratelimited_org = 2;

        let message = {
            let project_info = {
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

                Arc::new(ProjectInfo {
                    config,
                    ..Default::default()
                })
            };

            let project_metrics = ProjectMetrics {
                buckets: vec![Bucket {
                    name: "d:transactions/bar".into(),
                    value: BucketValue::Counter(relay_metrics::FiniteF64::new(1.0).unwrap()),
                    timestamp: UnixTimestamp::now(),
                    tags: Default::default(),
                    width: 10,
                    metadata: BucketMetadata::default(),
                }],
                rate_limits: Default::default(),
                project_info,
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

            EncodeMetrics {
                partition_key: None,
                scopes,
            }
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
                    _ => panic!("received envelope when expecting only metrics"),
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

        let project_info = ProjectInfo {
            config,
            ..Default::default()
        };

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator, test_store, group);

        let message = ProcessEnvelope {
            envelope,
            project_info: Arc::new(project_info),
            sampling_project_info: None,
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

    #[tokio::test]
    #[cfg(feature = "processing")]
    async fn test_materialize_dsc() {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        let dsc = r#"{
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "e12d836b15bb49d7bbf99e64295d995b",
            "sample_rate": "0.2"
        }"#;
        envelope.set_dsc(serde_json::from_str(dsc).unwrap());

        let mut item = Item::new(ItemType::Event);
        item.set_payload(ContentType::Json, r#"{}"#);
        envelope.add_item(item);

        let (outcome_aggregator, test_store) = testutils::processor_services();
        let managed_envelope = ManagedEnvelope::new(
            envelope,
            outcome_aggregator,
            test_store,
            ProcessingGroup::Error,
        );

        let process_message = ProcessEnvelope {
            envelope: managed_envelope,
            project_info: Arc::new(ProjectInfo::default()),
            sampling_project_info: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let processor = create_test_processor(config);
        let response = processor.process(process_message).unwrap();
        let envelope = response.envelope.as_ref().unwrap().envelope();
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
                    "00000000-0000-0000-0000-000000000000",
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
                score_profile_version: None,
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

    #[tokio::test]
    async fn test_process_metrics_bucket_metadata() {
        let mut token = Cogs::noop().timed(ResourceId::Relay, AppFeature::Unattributed);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let start_time = Instant::now();
        let config = Config::default();

        let (aggregator, mut aggregator_rx) = Addr::custom();
        let processor = create_test_processor_with_addrs(
            config,
            Addrs {
                aggregator,
                ..Default::default()
            },
        );

        let mut item = Item::new(ItemType::Statsd);
        item.set_payload(
            ContentType::Text,
            "transactions/foo:3182887624:4267882815|s",
        );
        for (source, expected_received_at) in [
            (
                BucketSource::External,
                Some(UnixTimestamp::from_instant(start_time)),
            ),
            (BucketSource::Internal, None),
        ] {
            let message = ProcessProjectMetrics {
                data: MetricData::Raw(vec![item.clone()]),
                project_state: ProjectState::Pending,
                rate_limits: Default::default(),
                project_key,
                source,
                start_time,
                sent_at: Some(Utc::now()),
            };
            processor.handle_process_project_metrics(&mut token, message);

            let value = aggregator_rx.recv().await.unwrap();
            let Aggregator::MergeBuckets(merge_buckets) = value else {
                panic!()
            };
            let buckets = merge_buckets.buckets;
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].metadata.received_at, expected_received_at);
        }
    }

    #[tokio::test]
    async fn test_process_batched_metrics() {
        let mut token = Cogs::noop().timed(ResourceId::Relay, AppFeature::Unattributed);
        let start_time = Instant::now();
        let config = Config::default();

        let (project_cache, mut project_cache_rx) = Addr::custom();
        let processor = create_test_processor_with_addrs(
            config,
            Addrs {
                project_cache,
                ..Default::default()
            },
        );

        let payload = r#"{
    "buckets": {
        "11111111111111111111111111111111": [
            {
                "timestamp": 1615889440,
                "width": 0,
                "name": "d:custom/endpoint.response_time@millisecond",
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
                "name": "d:custom/endpoint.cache_rate@none",
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
            start_time,
            sent_at: Some(Utc::now()),
        };
        processor.handle_process_batched_metrics(&mut token, message);

        let value = project_cache_rx.recv().await.unwrap();
        let ProjectCache::ProcessMetrics(pm1) = value else {
            panic!()
        };
        let value = project_cache_rx.recv().await.unwrap();
        let ProjectCache::ProcessMetrics(pm2) = value else {
            panic!()
        };

        let mut messages = vec![pm1, pm2];
        messages.sort_by_key(|pm| pm.project_key);

        let actual = messages
            .into_iter()
            .map(|pm| (pm.project_key, pm.data, pm.source))
            .collect::<Vec<_>>();

        assert_debug_snapshot!(actual, @r###"
        [
            (
                ProjectKey("11111111111111111111111111111111"),
                Parsed(
                    [
                        Bucket {
                            timestamp: UnixTimestamp(1615889440),
                            width: 0,
                            name: MetricName(
                                "d:custom/endpoint.response_time@millisecond",
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
                Internal,
            ),
            (
                ProjectKey("22222222222222222222222222222222"),
                Parsed(
                    [
                        Bucket {
                            timestamp: UnixTimestamp(1615889440),
                            width: 0,
                            name: MetricName(
                                "d:custom/endpoint.cache_rate@none",
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
                Internal,
            ),
        ]
        "###);
    }

    #[test]
    fn test_extrapolate() {
        let mut project_info = ProjectInfo::default();
        project_info.config.metric_extraction = ErrorBoundary::Ok(MetricExtractionConfig {
            extrapolate: ExtrapolationConfig {
                include: vec![LazyGlob::new("*")],
                exclude: vec![],
            },
            ..Default::default()
        });

        let dsc = DynamicSamplingContext::deserialize(serde_json::json!({
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "sample_rate": "0.2",
        }))
        .unwrap();

        let mut global_config = GlobalConfig::default();
        global_config.options.extrapolation_duplication_limit = 3;

        let mut extracted_metrics = ProcessingExtractedMetrics::new(
            Arc::new(project_info),
            Arc::new(global_config),
            Some(&dsc),
        );

        let buckets = serde_json::from_value(serde_json::json!([
          {
            "timestamp": 1615889440,
            "width": 10,
            "name": "endpoint.response_time",
            "type": "d",
            "value": [
              36.0,
              49.0,
              57.0,
              68.0
            ],
            "tags": {
              "route": "user_index"
            }
          },
          {
            "timestamp": 1615889440,
            "width": 10,
            "name": "endpoint.hits",
            "type": "c",
            "value": 4.0,
            "tags": {
              "route": "user_index"
            }
          },
          {
            "timestamp": 1615889440,
            "width": 10,
            "name": "endpoint.parallel_requests",
            "type": "g",
            "value": {
              "last": 25.0,
              "min": 17.0,
              "max": 42.0,
              "sum": 2210.0,
              "count": 85
            }
          },
          {
            "timestamp": 1615889440,
            "width": 10,
            "name": "endpoint.users",
            "type": "s",
            "value": [
              3182887624u32,
              4267882815u32
            ],
            "tags": {
              "route": "user_index"
            }
          }
        ]))
        .unwrap();

        extracted_metrics.extend_project_metrics(buckets, None);

        insta::assert_debug_snapshot!(extracted_metrics.metrics.project_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                width: 10,
                name: MetricName(
                    "endpoint.response_time",
                ),
                value: Distribution(
                    [
                        36.0,
                        36.0,
                        36.0,
                        49.0,
                        49.0,
                        49.0,
                        57.0,
                        57.0,
                        57.0,
                        68.0,
                        68.0,
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
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                width: 10,
                name: MetricName(
                    "endpoint.hits",
                ),
                value: Counter(
                    20.0,
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
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                width: 10,
                name: MetricName(
                    "endpoint.parallel_requests",
                ),
                value: Gauge(
                    GaugeValue {
                        last: 25.0,
                        min: 17.0,
                        max: 42.0,
                        sum: 11050.0,
                        count: 425,
                    },
                ),
                tags: {},
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: None,
                    extracted_from_indexed: false,
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                width: 10,
                name: MetricName(
                    "endpoint.users",
                ),
                value: Set(
                    {
                        3182887624,
                        4267882815,
                    },
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
        ]
        "###);
    }
}
