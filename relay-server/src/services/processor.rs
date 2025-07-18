use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::{Arc, Once};
use std::time::Duration;

use anyhow::Context;
use brotli::CompressorWriter as BrotliEncoder;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use futures::FutureExt;
use futures::future::BoxFuture;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_cogs::{AppFeature, Cogs, FeatureWeights, ResourceId, Token};
use relay_common::time::UnixTimestamp;
use relay_config::{Config, HttpEncoding, NormalizationLevel, RelayMode};
use relay_dynamic_config::{CombinedMetricExtractionConfig, ErrorBoundary, Feature, GlobalConfig};
use relay_event_normalization::{
    ClockDriftProcessor, CombinedMeasurementsConfig, EventValidationConfig, GeoIpLookup,
    MeasurementsConfig, NormalizationConfig, RawUserAgentInfo, TransactionNameConfig,
    normalize_event, validate_event,
};
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::{
    ClientReport, Event, EventId, EventType, IpAddr, Metrics, NetworkReportError,
};
use relay_filter::FilterStatKey;
use relay_metrics::{Bucket, BucketMetadata, BucketView, BucketsView, MetricNamespace};
use relay_pii::PiiConfigError;
use relay_protocol::{Annotated, Empty};
use relay_quotas::{DataCategory, Quota, RateLimits, Scoping};
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse, Service};
use reqwest::header;
use smallvec::{SmallVec, smallvec};
use zstd::stream::Encoder as ZstdEncoder;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::envelope::{self, ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::managed::{InvalidProcessingGroupType, ManagedEnvelope, TypedEnvelope};
use crate::metrics::{MetricOutcomes, MetricsLimiter, MinimalTrackableBucket};
use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::transactions::{ExtractedMetrics, TransactionExtractor};
use crate::processing::logs::{LogOutput, LogsProcessor};
use crate::processing::{Forward as _, Output, Processor as _, QuotaRateLimiter};
use crate::service::ServiceError;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::metrics::{Aggregator, FlushBuckets, MergeBuckets, ProjectBuckets};
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::event::FiltersStatus;
use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::projects::project::{ProjectInfo, ProjectState};
use crate::services::test_store::{Capture, TestStore};
use crate::services::upstream::{
    SendRequest, Sign, SignatureType, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{self, CheckLimits, EnvelopeLimiter, SamplingResult};
use crate::{http, processing};
use relay_base_schema::organization::OrganizationId;
use relay_threading::AsyncPool;
#[cfg(feature = "processing")]
use {
    crate::managed::ItemAction,
    crate::services::global_rate_limits::{GlobalRateLimits, GlobalRateLimitsServiceHandle},
    crate::services::processor::nnswitch::SwitchProcessingError,
    crate::services::store::{Store, StoreEnvelope},
    crate::utils::Enforcement,
    itertools::Itertools,
    relay_cardinality::{
        CardinalityLimit, CardinalityLimiter, CardinalityLimitsSplit, RedisSetLimiter,
        RedisSetLimiterOptions,
    },
    relay_dynamic_config::{CardinalityLimiterMode, MetricExtractionGroups},
    relay_quotas::{RateLimitingError, RedisRateLimiter},
    relay_redis::{AsyncRedisClient, RedisClients},
    std::time::Instant,
    symbolic_unreal::{Unreal4Error, Unreal4ErrorKind},
};

mod attachment;
mod dynamic_sampling;
mod event;
mod metrics;
mod nel;
mod profile;
mod profile_chunk;
mod replay;
mod report;
mod session;
mod span;
mod transaction;
pub use span::extract_transaction_span;

#[cfg(all(sentry, feature = "processing"))]
mod playstation;
mod standalone;
#[cfg(feature = "processing")]
mod unreal;

#[cfg(feature = "processing")]
mod nnswitch;

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
    ($ty:ident, $variant:ident$(, $($other:ident),+)?) => {
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
                $($(
                    if matches!(value, ProcessingGroup::$other) {
                        return Ok($ty);
                    }
                )+)?
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
    fn supports_sampling(project_info: &ProjectInfo) -> bool;

    /// Whether reservoir sampling applies to this processing group (a.k.a. data type).
    fn supports_reservoir_sampling() -> bool;
}

processing_group!(TransactionGroup, Transaction);
impl EventProcessing for TransactionGroup {}

impl Sampling for TransactionGroup {
    fn supports_sampling(project_info: &ProjectInfo) -> bool {
        // For transactions, we require transaction metrics to be enabled before sampling.
        matches!(&project_info.config.transaction_metrics, Some(ErrorBoundary::Ok(c)) if c.is_enabled())
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
processing_group!(LogGroup, Log, Nel);
processing_group!(SpanGroup, Span);

impl Sampling for SpanGroup {
    fn supports_sampling(project_info: &ProjectInfo) -> bool {
        // If no metrics could be extracted, do not sample anything.
        matches!(&project_info.config().metric_extraction, ErrorBoundary::Ok(c) if c.is_supported())
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
    /// NEL reports.
    Nel,
    /// Logs.
    Log,
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
        let span_items = envelope.take_items_by(|item| {
            matches!(
                item.ty(),
                &ItemType::Span | &ItemType::OtelSpan | &ItemType::OtelTracesData
            )
        });

        if !span_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Span,
                Envelope::from_parts(headers.clone(), span_items),
            ))
        }

        // Extract logs.
        let logs_items =
            envelope.take_items_by(|item| matches!(item.ty(), &ItemType::Log | &ItemType::OtelLog));

        if !logs_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Log,
                Envelope::from_parts(headers.clone(), logs_items),
            ))
        }

        // NEL items are transformed into logs in their own processing step.
        let nel_items = envelope.take_items_by(|item| matches!(item.ty(), &ItemType::Nel));
        if !nel_items.is_empty() {
            grouped_envelopes.push((
                ProcessingGroup::Nel,
                Envelope::from_parts(headers.clone(), nel_items),
            ))
        }

        // Extract all metric items.
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
            ProcessingGroup::Log => "log",
            ProcessingGroup::Nel => "nel",
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
            ProcessingGroup::Log => AppFeature::Logs,
            ProcessingGroup::Nel => AppFeature::Logs,
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
    PayloadTooLarge(DiscardItemType),

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
    InvalidProcessingGroup(Box<InvalidProcessingGroupType>),

    #[error("invalid replay")]
    InvalidReplay(DiscardReason),

    #[error("replay filtered with reason: {0:?}")]
    ReplayFiltered(FilterStatKey),

    #[cfg(feature = "processing")]
    #[error("nintendo switch dying message processing failed {0:?}")]
    InvalidNintendoDyingMessage(#[source] SwitchProcessingError),

    #[cfg(all(sentry, feature = "processing"))]
    #[error("playstation dump processing failed: {0}")]
    InvalidPlaystationDump(String),

    #[error("processing group does not match specific processor")]
    ProcessingGroupMismatch,
    #[error("new processing pipeline failed")]
    ProcessingFailure,
    #[error("failed to serialize processing result to an envelope")]
    ProcessingEnvelopeSerialization,
}

impl ProcessingError {
    pub fn to_outcome(&self) -> Option<Outcome> {
        match self {
            Self::PayloadTooLarge(payload_type) => {
                Some(Outcome::Invalid(DiscardReason::TooLarge(*payload_type)))
            }
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
            #[cfg(feature = "processing")]
            Self::InvalidNintendoDyingMessage(_) => Some(Outcome::Invalid(DiscardReason::Payload)),
            #[cfg(all(sentry, feature = "processing"))]
            Self::InvalidPlaystationDump(_) => Some(Outcome::Invalid(DiscardReason::Payload)),
            #[cfg(feature = "processing")]
            Self::InvalidUnrealReport(err) if err.kind() == Unreal4ErrorKind::BadCompression => {
                Some(Outcome::Invalid(DiscardReason::InvalidCompression))
            }
            #[cfg(feature = "processing")]
            Self::InvalidUnrealReport(_) => Some(Outcome::Invalid(DiscardReason::ProcessUnreal)),
            Self::SerializeFailed(_) | Self::ProcessingFailed(_) => {
                Some(Outcome::Invalid(DiscardReason::Internal))
            }
            #[cfg(feature = "processing")]
            Self::QuotasFailed(_) => Some(Outcome::Invalid(DiscardReason::Internal)),
            Self::PiiConfigError(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
            Self::MissingProjectId => None,
            Self::EventFiltered(_) => None,
            Self::InvalidProcessingGroup(_) => None,
            Self::InvalidReplay(reason) => Some(Outcome::Invalid(*reason)),
            Self::ReplayFiltered(key) => Some(Outcome::Filtered(key.clone())),

            Self::ProcessingGroupMismatch => Some(Outcome::Invalid(DiscardReason::Internal)),
            // Outcomes are emitted in the new processing pipeline already.
            Self::ProcessingFailure => None,
            Self::ProcessingEnvelopeSerialization => {
                Some(Outcome::Invalid(DiscardReason::Internal))
            }
        }
    }

    fn is_unexpected(&self) -> bool {
        self.to_outcome()
            .is_some_and(|outcome| outcome.is_unexpected())
    }
}

#[cfg(feature = "processing")]
impl From<Unreal4Error> for ProcessingError {
    fn from(err: Unreal4Error) -> Self {
        match err.kind() {
            Unreal4ErrorKind::TooLarge => Self::PayloadTooLarge(ItemType::UnrealReport.into()),
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

impl From<InvalidProcessingGroupType> for ProcessingError {
    fn from(value: InvalidProcessingGroupType) -> Self {
        Self::InvalidProcessingGroup(Box::new(value))
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
}

impl ProcessingExtractedMetrics {
    pub fn new() -> Self {
        Self {
            metrics: ExtractedMetrics::default(),
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
    pub fn extend_project_metrics<I>(
        &mut self,
        buckets: I,
        sampling_decision: Option<SamplingDecision>,
    ) where
        I: IntoIterator<Item = Bucket>,
    {
        self.metrics
            .project_metrics
            .extend(buckets.into_iter().map(|mut bucket| {
                bucket.metadata.extracted_from_indexed =
                    sampling_decision == Some(SamplingDecision::Keep);
                bucket
            }));
    }

    /// Extends the contained sampling metrics.
    pub fn extend_sampling_metrics<I>(
        &mut self,
        buckets: I,
        sampling_decision: Option<SamplingDecision>,
    ) where
        I: IntoIterator<Item = Bucket>,
    {
        self.metrics
            .sampling_metrics
            .extend(buckets.into_iter().map(|mut bucket| {
                bucket.metadata.extracted_from_indexed =
                    sampling_decision == Some(SamplingDecision::Keep);
                bucket
            }));
    }

    /// Applies rate limits to the contained metrics.
    ///
    /// This is used to apply rate limits which have been enforced on sampled items of an envelope
    /// to also consistently apply to the metrics extracted from these items.
    #[cfg(feature = "processing")]
    fn apply_enforcement(&mut self, enforcement: &Enforcement, enforced_consistently: bool) {
        // Metric namespaces which need to be dropped.
        let mut drop_namespaces: SmallVec<[_; 2]> = smallvec![];
        // Metrics belonging to this metric namespace need to have the `extracted_from_indexed`
        // flag reset to `false`.
        let mut reset_extracted_from_indexed: SmallVec<[_; 2]> = smallvec![];

        for (namespace, limit, indexed) in [
            (
                MetricNamespace::Transactions,
                &enforcement.event,
                &enforcement.event_indexed,
            ),
            (
                MetricNamespace::Spans,
                &enforcement.spans,
                &enforcement.spans_indexed,
            ),
        ] {
            if limit.is_active() {
                drop_namespaces.push(namespace);
            } else if indexed.is_active() && !enforced_consistently {
                // If the enforcement was not computed by consistently checking the limits,
                // the quota for the metrics has not yet been incremented.
                // In this case we have a dropped indexed payload but a metric which still needs to
                // be accounted for, make sure the metric will still be rate limited.
                reset_extracted_from_indexed.push(namespace);
            }
        }

        if !drop_namespaces.is_empty() || !reset_extracted_from_indexed.is_empty() {
            self.retain_mut(|bucket| {
                let Some(namespace) = bucket.name.try_namespace() else {
                    return true;
                };

                if drop_namespaces.contains(&namespace) {
                    return false;
                }

                if reset_extracted_from_indexed.contains(&namespace) {
                    bucket.metadata.extracted_from_indexed = false;
                }

                true
            });
        }
    }

    #[cfg(feature = "processing")]
    fn retain_mut(&mut self, mut f: impl FnMut(&mut Bucket) -> bool) {
        self.metrics.project_metrics.retain_mut(&mut f);
        self.metrics.sampling_metrics.retain_mut(&mut f);
    }
}

fn send_metrics(
    metrics: ExtractedMetrics,
    project_key: ProjectKey,
    sampling_key: Option<ProjectKey>,
    aggregator: &Addr<Aggregator>,
) {
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
        let sampling_project_key = sampling_key.unwrap_or(project_key);
        aggregator.send(MergeBuckets {
            project_key: sampling_project_key,
            buckets: sampling_metrics,
        });
    }
}

/// Returns the data category if there is an event.
///
/// The data category is computed from the event type. Both `Default` and `Error` events map to
/// the `Error` data category. If there is no Event, `None` is returned.
fn event_category(event: &Annotated<Event>) -> Option<DataCategory> {
    event_type(event).map(DataCategory::from)
}

/// Returns the event type if there is an event.
///
/// If the event does not have a type, `Some(EventType::Default)` is assumed. If, in contrast, there
/// is no event, `None` is returned.
fn event_type(event: &Annotated<Event>) -> Option<EventType> {
    event
        .value()
        .map(|event| event.ty.value().copied().unwrap_or_default())
}

/// Function for on-off switches that filter specific item types (profiles, spans)
/// based on a feature flag.
///
/// If the project config did not come from the upstream, we keep the items.
fn should_filter(config: &Config, project_info: &ProjectInfo, feature: Feature) -> bool {
    match config.relay_mode() {
        RelayMode::Proxy | RelayMode::Static | RelayMode::Capture => false,
        RelayMode::Managed => !project_info.has_feature(feature),
    }
}

/// New type representing the normalization state of the event.
#[derive(Copy, Clone)]
struct EventFullyNormalized(bool);

impl EventFullyNormalized {
    /// Returns `true` if the event is fully normalized, `false` otherwise.
    pub fn new(envelope: &Envelope) -> Self {
        let event_fully_normalized = envelope.meta().is_from_internal_relay()
            && envelope
                .items()
                .any(|item| item.creates_event() && item.fully_normalized());

        Self(event_fully_normalized)
    }
}

/// New type representing whether metrics were extracted from transactions/spans.
#[derive(Debug, Copy, Clone)]
struct EventMetricsExtracted(bool);

/// New type representing whether spans were extracted.
#[derive(Debug, Copy, Clone)]
struct SpansExtracted(bool);

/// The result of the envelope processing containing the processed envelope along with the partial
/// result.
#[expect(
    clippy::large_enum_variant,
    reason = "until we have a better solution to combat the excessive growth of Item, see #4819"
)]
#[derive(Debug)]
enum ProcessingResult {
    Envelope {
        managed_envelope: TypedEnvelope<Processed>,
        extracted_metrics: ProcessingExtractedMetrics,
    },
    Logs(Output<LogOutput>),
}

impl ProcessingResult {
    /// Creates a [`ProcessingResult`] with no metrics.
    fn no_metrics(managed_envelope: TypedEnvelope<Processed>) -> Self {
        Self::Envelope {
            managed_envelope,
            extracted_metrics: ProcessingExtractedMetrics::new(),
        }
    }
}

/// All items which can be submitted upstream.
#[expect(
    clippy::large_enum_variant,
    reason = "until we have a better solution to combat the excessive growth of Item, see #4819"
)]
enum Submit {
    /// A processed envelope.
    Envelope(TypedEnvelope<Processed>),
    /// The output of the [log processor](LogsProcessor).
    Logs(LogOutput),
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
    /// Sampling reservoir counters.
    pub reservoir_counters: ReservoirCounters,
}

/// Like a [`ProcessEnvelope`], but with an envelope which has been grouped.
#[derive(Debug)]
struct ProcessEnvelopeGrouped {
    /// The group the envelope belongs to.
    pub group: ProcessingGroup,
    /// Envelope to process.
    pub envelope: ManagedEnvelope,
    /// The project info.
    pub project_info: Arc<ProjectInfo>,
    /// Currently active cached rate limits for this project.
    pub rate_limits: Arc<RateLimits>,
    /// Root sampling project info.
    pub sampling_project_info: Option<Arc<ProjectInfo>>,
    /// Sampling reservoir counters.
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
pub struct ProcessMetrics {
    /// A list of metric items.
    pub data: MetricData,
    /// The target project.
    pub project_key: ProjectKey,
    /// Whether to keep or reset the metric metadata.
    pub source: BucketSource,
    /// The wall clock time at which the request was received.
    pub received_at: DateTime<Utc>,
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
    /// Infers the bucket source from [`RequestMeta::is_from_internal_relay`].
    pub fn from_meta(meta: &RequestMeta) -> Self {
        match meta.is_from_internal_relay() {
            true => Self::Internal,
            false => Self::External,
        }
    }
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
    ProcessProjectMetrics(Box<ProcessMetrics>),
    ProcessBatchedMetrics(Box<ProcessBatchedMetrics>),
    FlushBuckets(Box<FlushBuckets>),
    SubmitClientReports(Box<SubmitClientReports>),
}

impl EnvelopeProcessor {
    /// Returns the name of the message variant.
    pub fn variant(&self) -> &'static str {
        match self {
            EnvelopeProcessor::ProcessEnvelope(_) => "ProcessEnvelope",
            EnvelopeProcessor::ProcessProjectMetrics(_) => "ProcessProjectMetrics",
            EnvelopeProcessor::ProcessBatchedMetrics(_) => "ProcessBatchedMetrics",
            EnvelopeProcessor::FlushBuckets(_) => "FlushBuckets",
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

impl FromMessage<FlushBuckets> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: FlushBuckets, _: ()) -> Self {
        Self::FlushBuckets(Box::new(message))
    }
}

impl FromMessage<SubmitClientReports> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: SubmitClientReports, _: ()) -> Self {
        Self::SubmitClientReports(Box::new(message))
    }
}

/// The asynchronous thread pool used for scheduling processing tasks in the processor.
pub type EnvelopeProcessorServicePool = AsyncPool<BoxFuture<'static, ()>>;

/// Service implementing the [`EnvelopeProcessor`] interface.
///
/// This service handles messages in a worker pool with configurable concurrency.
#[derive(Clone)]
pub struct EnvelopeProcessorService {
    inner: Arc<InnerProcessor>,
}

/// Contains the addresses of services that the processor publishes to.
pub struct Addrs {
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub upstream_relay: Addr<UpstreamRelay>,
    pub test_store: Addr<TestStore>,
    #[cfg(feature = "processing")]
    pub store_forwarder: Option<Addr<Store>>,
    pub aggregator: Addr<Aggregator>,
    #[cfg(feature = "processing")]
    pub global_rate_limits: Option<Addr<GlobalRateLimits>>,
}

impl Default for Addrs {
    fn default() -> Self {
        Addrs {
            outcome_aggregator: Addr::dummy(),
            upstream_relay: Addr::dummy(),
            test_store: Addr::dummy(),
            #[cfg(feature = "processing")]
            store_forwarder: None,
            aggregator: Addr::dummy(),
            #[cfg(feature = "processing")]
            global_rate_limits: None,
        }
    }
}

struct InnerProcessor {
    pool: EnvelopeProcessorServicePool,
    config: Arc<Config>,
    global_config: GlobalConfigHandle,
    project_cache: ProjectCacheHandle,
    cogs: Cogs,
    #[cfg(feature = "processing")]
    quotas_client: Option<AsyncRedisClient>,
    addrs: Addrs,
    #[cfg(feature = "processing")]
    rate_limiter: Option<Arc<RedisRateLimiter<GlobalRateLimitsServiceHandle>>>,
    geoip_lookup: Option<GeoIpLookup>,
    #[cfg(feature = "processing")]
    cardinality_limiter: Option<CardinalityLimiter>,
    metric_outcomes: MetricOutcomes,
    processing: Processing,
}

struct Processing {
    logs: LogsProcessor,
}

impl EnvelopeProcessorService {
    /// Creates a multi-threaded envelope processor.
    #[cfg_attr(feature = "processing", expect(clippy::too_many_arguments))]
    pub fn new(
        pool: EnvelopeProcessorServicePool,
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        project_cache: ProjectCacheHandle,
        cogs: Cogs,
        #[cfg(feature = "processing")] redis: Option<RedisClients>,
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
        let (cardinality, quotas) = match redis {
            Some(RedisClients {
                cardinality,
                quotas,
                ..
            }) => (Some(cardinality), Some(quotas)),
            None => (None, None),
        };

        #[cfg(feature = "processing")]
        let global_rate_limits = addrs.global_rate_limits.clone().map(Into::into);

        #[cfg(feature = "processing")]
        let rate_limiter = match (quotas.clone(), global_rate_limits) {
            (Some(redis), Some(global)) => {
                Some(RedisRateLimiter::new(redis, global).max_limit(config.max_rate_limit()))
            }
            _ => None,
        };

        let quota_limiter = Arc::new(QuotaRateLimiter::new(
            #[cfg(feature = "processing")]
            project_cache.clone(),
            #[cfg(feature = "processing")]
            rate_limiter.clone(),
        ));
        #[cfg(feature = "processing")]
        let rate_limiter = rate_limiter.map(Arc::new);

        let inner = InnerProcessor {
            pool,
            global_config,
            project_cache,
            cogs,
            #[cfg(feature = "processing")]
            quotas_client: quotas.clone(),
            #[cfg(feature = "processing")]
            rate_limiter,
            addrs,
            geoip_lookup,
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
            processing: Processing {
                logs: LogsProcessor::new(quota_limiter),
            },
            config,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Normalize monitor check-ins and remove invalid ones.
    #[cfg(feature = "processing")]
    fn normalize_checkins(
        &self,
        managed_envelope: &mut TypedEnvelope<CheckInGroup>,
        project_id: ProjectId,
    ) {
        managed_envelope.retain_items(|item| {
            if item.ty() != &ItemType::CheckIn {
                return ItemAction::Keep;
            }

            match relay_monitors::process_check_in(&item.payload(), project_id) {
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

    async fn enforce_quotas<Group>(
        &self,
        managed_envelope: &mut TypedEnvelope<Group>,
        event: Annotated<Event>,
        extracted_metrics: &mut ProcessingExtractedMetrics,
        project_info: &ProjectInfo,
        rate_limits: &RateLimits,
    ) -> Result<Annotated<Event>, ProcessingError> {
        let global_config = self.inner.global_config.current();
        // Cached quotas first, they are quick to evaluate and some quotas (indexed) are not
        // applied in the fast path, all cached quotas can be applied here.
        let cached_result = RateLimiter::Cached
            .enforce(
                managed_envelope,
                event,
                extracted_metrics,
                &global_config,
                project_info,
                rate_limits,
            )
            .await?;

        if_processing!(self.inner.config, {
            let rate_limiter = match self.inner.rate_limiter.clone() {
                Some(rate_limiter) => rate_limiter,
                None => return Ok(cached_result.event),
            };

            // Enforce all quotas consistently with Redis.
            let consistent_result = RateLimiter::Consistent(rate_limiter)
                .enforce(
                    managed_envelope,
                    cached_result.event,
                    extracted_metrics,
                    &global_config,
                    project_info,
                    rate_limits,
                )
                .await?;

            // Update cached rate limits with the freshly computed ones.
            if !consistent_result.rate_limits.is_empty() {
                self.inner
                    .project_cache
                    .get(managed_envelope.scoping().project_key)
                    .rate_limits()
                    .merge(consistent_result.rate_limits);
            }

            Ok(consistent_result.event)
        } else { Ok(cached_result.event) })
    }

    /// Extract transaction metrics.
    #[allow(clippy::too_many_arguments)]
    fn extract_transaction_metrics(
        &self,
        managed_envelope: &mut TypedEnvelope<TransactionGroup>,
        event: &mut Annotated<Event>,
        extracted_metrics: &mut ProcessingExtractedMetrics,
        project_id: ProjectId,
        project_info: Arc<ProjectInfo>,
        sampling_decision: SamplingDecision,
        event_metrics_extracted: EventMetricsExtracted,
        spans_extracted: SpansExtracted,
    ) -> Result<EventMetricsExtracted, ProcessingError> {
        if event_metrics_extracted.0 {
            return Ok(event_metrics_extracted);
        }
        let Some(event) = event.value_mut() else {
            return Ok(event_metrics_extracted);
        };

        // NOTE: This function requires a `metric_extraction` in the project config. Legacy configs
        // will upsert this configuration from transaction and conditional tagging fields, even if
        // it is not present in the actual project config payload.
        let global = self.inner.global_config.current();
        let combined_config = {
            let config = match &project_info.config.metric_extraction {
                ErrorBoundary::Ok(config) if config.is_supported() => config,
                _ => return Ok(event_metrics_extracted),
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
                        return Ok(event_metrics_extracted);
                    })
                }
            };
            CombinedMetricExtractionConfig::new(global_config, config)
        };

        // Require a valid transaction metrics config.
        let tx_config = match &project_info.config.transaction_metrics {
            Some(ErrorBoundary::Ok(tx_config)) => tx_config,
            Some(ErrorBoundary::Err(e)) => {
                relay_log::debug!("Failed to parse legacy transaction metrics config: {e}");
                return Ok(event_metrics_extracted);
            }
            None => {
                relay_log::debug!("Legacy transaction metrics config is missing");
                return Ok(event_metrics_extracted);
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

            return Ok(event_metrics_extracted);
        }

        // If spans were already extracted for an event, we rely on span processing to extract metrics.
        let extract_spans = !spans_extracted.0
            && project_info.config.features.produces_spans()
            && utils::sample(global.options.span_extraction_sample_rate.unwrap_or(1.0)).is_keep();

        let metrics = crate::metrics_extraction::event::extract_metrics(
            event,
            combined_config,
            sampling_decision,
            project_id,
            self.inner
                .config
                .aggregator_config_for(MetricNamespace::Spans)
                .max_tag_value_length,
            extract_spans,
        );

        extracted_metrics.extend(metrics, Some(sampling_decision));

        if !project_info.has_feature(Feature::DiscardTransaction) {
            let transaction_from_dsc = managed_envelope
                .envelope()
                .dsc()
                .and_then(|dsc| dsc.transaction.as_deref());

            let extractor = TransactionExtractor {
                config: tx_config,
                generic_config: Some(combined_config),
                transaction_from_dsc,
                sampling_decision,
                target_project_id: project_id,
            };

            extracted_metrics.extend(extractor.extract(event)?, Some(sampling_decision));
        }

        Ok(EventMetricsExtracted(true))
    }

    fn normalize_event<Group: EventProcessing>(
        &self,
        managed_envelope: &mut TypedEnvelope<Group>,
        event: &mut Annotated<Event>,
        project_id: ProjectId,
        project_info: Arc<ProjectInfo>,
        mut event_fully_normalized: EventFullyNormalized,
    ) -> Result<EventFullyNormalized, ProcessingError> {
        if event.value().is_empty() {
            // NOTE(iker): only processing relays create events from
            // attachments, so these events won't be normalized in
            // non-processing relays even if the config is set to run full
            // normalization.
            return Ok(event_fully_normalized);
        }

        let full_normalization = match self.inner.config.normalization_level() {
            NormalizationLevel::Full => true,
            NormalizationLevel::Default => {
                if self.inner.config.processing_enabled() && event_fully_normalized.0 {
                    return Ok(event_fully_normalized);
                }

                self.inner.config.processing_enabled()
            }
        };

        let request_meta = managed_envelope.envelope().meta();
        let client_ipaddr = request_meta.client_addr().map(IpAddr::from);

        let transaction_aggregator_config = self
            .inner
            .config
            .aggregator_config_for(MetricNamespace::Transactions);

        let global_config = self.inner.global_config.current();
        let ai_model_costs = global_config.ai_model_costs.clone().ok();
        let http_span_allowed_hosts = global_config.options.http_span_allowed_hosts.as_slice();

        let retention_days: i64 = project_info
            .config
            .event_retention
            .unwrap_or(DEFAULT_EVENT_RETENTION)
            .into();

        utils::log_transaction_name_metrics(event, |event| {
            let event_validation_config = EventValidationConfig {
                received_at: Some(managed_envelope.received_at()),
                max_secs_in_past: Some(retention_days * 24 * 3600),
                max_secs_in_future: Some(self.inner.config.max_secs_in_future()),
                transaction_timestamp_range: Some(transaction_aggregator_config.timestamp_range()),
                is_validated: false,
            };

            let key_id = project_info
                .get_public_key_config()
                .and_then(|key| Some(key.numeric_id?.to_string()));
            if full_normalization && key_id.is_none() {
                relay_log::error!(
                    "project state for key {} is missing key id",
                    managed_envelope.envelope().meta().public_key()
                );
            }

            let normalization_config = NormalizationConfig {
                project_id: Some(project_id.value()),
                client: request_meta.client().map(str::to_owned),
                key_id,
                protocol_version: Some(request_meta.version().to_string()),
                grouping_config: project_info.config.grouping_config.clone(),
                client_ip: client_ipaddr.as_ref(),
                // if the setting is enabled we do not want to infer the ip address
                infer_ip_address: !project_info
                    .config
                    .datascrubbing_settings
                    .scrub_ip_addresses,
                client_sample_rate: managed_envelope
                    .envelope()
                    .dsc()
                    .and_then(|ctx| ctx.sample_rate),
                user_agent: RawUserAgentInfo {
                    user_agent: request_meta.user_agent(),
                    client_hints: request_meta.client_hints().as_deref(),
                },
                max_name_and_unit_len: Some(
                    transaction_aggregator_config
                        .max_name_length
                        .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),
                ),
                breakdowns_config: project_info.config.breakdowns_v2.as_ref(),
                performance_score: project_info.config.performance_score.as_ref(),
                normalize_user_agent: Some(true),
                transaction_name_config: TransactionNameConfig {
                    rules: &project_info.config.tx_name_rules,
                },
                device_class_synthesis_config: project_info
                    .has_feature(Feature::DeviceClassSynthesis),
                enrich_spans: project_info.has_feature(Feature::ExtractSpansFromEvent),
                max_tag_value_length: self
                    .inner
                    .config
                    .aggregator_config_for(MetricNamespace::Spans)
                    .max_tag_value_length,
                is_renormalize: false,
                remove_other: full_normalization,
                emit_event_errors: full_normalization,
                span_description_rules: project_info.config.span_description_rules.as_ref(),
                geoip_lookup: self.inner.geoip_lookup.as_ref(),
                ai_model_costs: ai_model_costs.as_ref(),
                enable_trimming: true,
                measurements: Some(CombinedMeasurementsConfig::new(
                    project_info.config().measurements.as_ref(),
                    global_config.measurements.as_ref(),
                )),
                normalize_spans: true,
                replay_id: managed_envelope
                    .envelope()
                    .dsc()
                    .and_then(|ctx| ctx.replay_id),
                span_allowed_hosts: http_span_allowed_hosts,
                span_op_defaults: global_config.span_op_defaults.borrow(),
                performance_issues_spans: project_info.has_feature(Feature::PerformanceIssuesSpans),
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

        event_fully_normalized.0 |= full_normalization;

        Ok(event_fully_normalized)
    }

    /// Processes the general errors, and the items which require or create the events.
    async fn process_errors(
        &self,
        managed_envelope: &mut TypedEnvelope<ErrorGroup>,
        project_id: ProjectId,
        project_info: Arc<ProjectInfo>,
        mut sampling_project_info: Option<Arc<ProjectInfo>>,
        rate_limits: Arc<RateLimits>,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        let mut event_fully_normalized = EventFullyNormalized::new(managed_envelope.envelope());
        let mut metrics = Metrics::default();
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        // Events can also contain user reports.
        report::process_user_reports(managed_envelope);

        if_processing!(self.inner.config, {
            unreal::expand(managed_envelope, &self.inner.config)?;
            #[cfg(sentry)]
            playstation::expand(managed_envelope, &self.inner.config, &project_info)?;
            nnswitch::expand(managed_envelope)?;
        });

        let extraction_result = event::extract(
            managed_envelope,
            &mut metrics,
            event_fully_normalized,
            &self.inner.config,
        )?;
        let mut event = extraction_result.event;

        if_processing!(self.inner.config, {
            if let Some(inner_event_fully_normalized) =
                unreal::process(managed_envelope, &mut event)?
            {
                event_fully_normalized = inner_event_fully_normalized;
            }
            #[cfg(sentry)]
            if let Some(inner_event_fully_normalized) =
                playstation::process(managed_envelope, &mut event, &project_info)?
            {
                event_fully_normalized = inner_event_fully_normalized;
            }
            if let Some(inner_event_fully_normalized) =
                attachment::create_placeholders(managed_envelope, &mut event, &mut metrics)
            {
                event_fully_normalized = inner_event_fully_normalized;
            }
        });

        sampling_project_info = dynamic_sampling::validate_and_set_dsc(
            managed_envelope,
            &mut event,
            project_info.clone(),
            sampling_project_info,
        );
        event::finalize(
            managed_envelope,
            &mut event,
            &mut metrics,
            &self.inner.config,
        )?;
        event_fully_normalized = self.normalize_event(
            managed_envelope,
            &mut event,
            project_id,
            project_info.clone(),
            event_fully_normalized,
        )?;
        let filter_run = event::filter(
            managed_envelope,
            &mut event,
            project_info.clone(),
            &self.inner.global_config.current(),
        )?;

        if self.inner.config.processing_enabled() || matches!(filter_run, FiltersStatus::Ok) {
            dynamic_sampling::tag_error_with_sampling_decision(
                managed_envelope,
                &mut event,
                sampling_project_info,
                &self.inner.config,
            )
            .await;
        }

        event = self
            .enforce_quotas(
                managed_envelope,
                event,
                &mut extracted_metrics,
                &project_info,
                &rate_limits,
            )
            .await?;

        if event.value().is_some() {
            event::scrub(&mut event, project_info.clone())?;
            event::serialize(
                managed_envelope,
                &mut event,
                event_fully_normalized,
                EventMetricsExtracted(false),
                SpansExtracted(false),
            )?;
            event::emit_feedback_metrics(managed_envelope.envelope());
        }

        attachment::scrub(managed_envelope, project_info);

        if self.inner.config.processing_enabled() && !event_fully_normalized.0 {
            relay_log::error!(
                tags.project = %project_id,
                tags.ty = event_type(&event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                "ingested event without normalizing"
            );
        }

        Ok(Some(extracted_metrics))
    }

    /// Processes only transactions and transaction-related items.
    #[allow(unused_assignments)]
    #[allow(clippy::too_many_arguments)]
    async fn process_transactions(
        &self,
        managed_envelope: &mut TypedEnvelope<TransactionGroup>,
        cogs: &mut Token,
        config: Arc<Config>,
        project_id: ProjectId,
        project_info: Arc<ProjectInfo>,
        mut sampling_project_info: Option<Arc<ProjectInfo>>,
        rate_limits: Arc<RateLimits>,
        reservoir_counters: ReservoirCounters,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        let mut event_fully_normalized = EventFullyNormalized::new(managed_envelope.envelope());
        let mut event_metrics_extracted = EventMetricsExtracted(false);
        let mut spans_extracted = SpansExtracted(false);
        let mut metrics = Metrics::default();
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        let global_config = self.inner.global_config.current();

        transaction::drop_invalid_items(managed_envelope, &global_config);

        relay_cogs::with!(cogs, "event_extract", {
            // We extract the main event from the envelope.
            let extraction_result = event::extract(
                managed_envelope,
                &mut metrics,
                event_fully_normalized,
                &self.inner.config,
            )?;
        });

        // If metrics were extracted we mark that.
        if let Some(inner_event_metrics_extracted) = extraction_result.event_metrics_extracted {
            event_metrics_extracted = inner_event_metrics_extracted;
        }
        if let Some(inner_spans_extracted) = extraction_result.spans_extracted {
            spans_extracted = inner_spans_extracted;
        };

        // We take the main event out of the result.
        let mut event = extraction_result.event;

        relay_cogs::with!(cogs, "profile_filter", {
            let profile_id = profile::filter(
                managed_envelope,
                &event,
                config.clone(),
                project_id,
                &project_info,
            );
            profile::transfer_id(&mut event, profile_id);
        });

        relay_cogs::with!(cogs, "dynamic_sampling_dsc", {
            sampling_project_info = dynamic_sampling::validate_and_set_dsc(
                managed_envelope,
                &mut event,
                project_info.clone(),
                sampling_project_info,
            );
        });

        relay_cogs::with!(cogs, "event_finalize", {
            event::finalize(
                managed_envelope,
                &mut event,
                &mut metrics,
                &self.inner.config,
            )?;
        });

        relay_cogs::with!(cogs, "event_normalize", {
            event_fully_normalized = self.normalize_event(
                managed_envelope,
                &mut event,
                project_id,
                project_info.clone(),
                event_fully_normalized,
            )?;
        });

        relay_cogs::with!(cogs, "filter", {
            let filter_run = event::filter(
                managed_envelope,
                &mut event,
                project_info.clone(),
                &self.inner.global_config.current(),
            )?;
        });

        // Always run dynamic sampling on processing Relays,
        // but delay decision until inbound filters have been fully processed.
        let run_dynamic_sampling =
            matches!(filter_run, FiltersStatus::Ok) || self.inner.config.processing_enabled();

        let reservoir = self.new_reservoir_evaluator(
            managed_envelope.scoping().organization_id,
            reservoir_counters,
        );

        relay_cogs::with!(cogs, "dynamic_sampling_run", {
            let sampling_result = match run_dynamic_sampling {
                true => {
                    dynamic_sampling::run(
                        managed_envelope,
                        &mut event,
                        config.clone(),
                        project_info.clone(),
                        sampling_project_info,
                        &reservoir,
                    )
                    .await
                }
                false => SamplingResult::Pending,
            };
        });

        #[cfg(feature = "processing")]
        let server_sample_rate = match sampling_result {
            SamplingResult::Match(ref sampling_match) => Some(sampling_match.sample_rate()),
            SamplingResult::NoMatch | SamplingResult::Pending => None,
        };

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            // Process profiles before dropping the transaction, if necessary.
            // Before metric extraction to make sure the profile count is reflected correctly.
            profile::process(
                managed_envelope,
                &mut event,
                &global_config,
                config.clone(),
                project_info.clone(),
            );
            // Extract metrics here, we're about to drop the event/transaction.
            event_metrics_extracted = self.extract_transaction_metrics(
                managed_envelope,
                &mut event,
                &mut extracted_metrics,
                project_id,
                project_info.clone(),
                SamplingDecision::Drop,
                event_metrics_extracted,
                spans_extracted,
            )?;

            dynamic_sampling::drop_unsampled_items(
                managed_envelope,
                event,
                outcome,
                spans_extracted,
            );

            // At this point we have:
            //  - An empty envelope.
            //  - An envelope containing only processed profiles.
            // We need to make sure there are enough quotas for these profiles.
            event = self
                .enforce_quotas(
                    managed_envelope,
                    Annotated::empty(),
                    &mut extracted_metrics,
                    &project_info,
                    &rate_limits,
                )
                .await?;

            return Ok(Some(extracted_metrics));
        }

        let _post_ds = cogs.start_category("post_ds");

        // Need to scrub the transaction before extracting spans.
        //
        // Unconditionally scrub to make sure PII is removed as early as possible.
        event::scrub(&mut event, project_info.clone())?;

        // TODO: remove once `relay.drop-transaction-attachments` has graduated.
        attachment::scrub(managed_envelope, project_info.clone());

        if_processing!(self.inner.config, {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            let profile_id = profile::process(
                managed_envelope,
                &mut event,
                &global_config,
                config.clone(),
                project_info.clone(),
            );
            profile::transfer_id(&mut event, profile_id);
            profile::scrub_profiler_id(&mut event);

            // Always extract metrics in processing Relays for sampled items.
            event_metrics_extracted = self.extract_transaction_metrics(
                managed_envelope,
                &mut event,
                &mut extracted_metrics,
                project_id,
                project_info.clone(),
                SamplingDecision::Keep,
                event_metrics_extracted,
                spans_extracted,
            )?;

            if project_info.has_feature(Feature::ExtractSpansFromEvent) {
                spans_extracted = span::extract_from_event(
                    managed_envelope,
                    &event,
                    &global_config,
                    config,
                    server_sample_rate,
                    event_metrics_extracted,
                    spans_extracted,
                );
            }
        });

        event = self
            .enforce_quotas(
                managed_envelope,
                event,
                &mut extracted_metrics,
                &project_info,
                &rate_limits,
            )
            .await?;

        if_processing!(self.inner.config, {
            event = span::maybe_discard_transaction(managed_envelope, event, project_info);
        });

        // Event may have been dropped because of a quota and the envelope can be empty.
        if event.value().is_some() {
            event::serialize(
                managed_envelope,
                &mut event,
                event_fully_normalized,
                event_metrics_extracted,
                spans_extracted,
            )?;
        }

        if self.inner.config.processing_enabled() && !event_fully_normalized.0 {
            relay_log::error!(
                tags.project = %project_id,
                tags.ty = event_type(&event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                "ingested event without normalizing"
            );
        };

        Ok(Some(extracted_metrics))
    }

    async fn process_profile_chunks(
        &self,
        managed_envelope: &mut TypedEnvelope<ProfileChunkGroup>,
        project_info: Arc<ProjectInfo>,
        rate_limits: Arc<RateLimits>,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        profile_chunk::filter(managed_envelope, project_info.clone());

        if_processing!(self.inner.config, {
            profile_chunk::process(
                managed_envelope,
                &project_info,
                &self.inner.global_config.current(),
                &self.inner.config,
            );
        });

        self.enforce_quotas(
            managed_envelope,
            Annotated::empty(),
            &mut ProcessingExtractedMetrics::new(),
            &project_info,
            &rate_limits,
        )
        .await?;

        Ok(None)
    }

    /// Processes standalone items that require an event ID, but do not have an event on the same envelope.
    async fn process_standalone(
        &self,
        managed_envelope: &mut TypedEnvelope<StandaloneGroup>,
        config: Arc<Config>,
        project_id: ProjectId,
        project_info: Arc<ProjectInfo>,
        rate_limits: Arc<RateLimits>,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        standalone::process(managed_envelope);

        profile::filter(
            managed_envelope,
            &Annotated::empty(),
            config,
            project_id,
            &project_info,
        );

        self.enforce_quotas(
            managed_envelope,
            Annotated::empty(),
            &mut extracted_metrics,
            &project_info,
            &rate_limits,
        )
        .await?;

        report::process_user_reports(managed_envelope);
        attachment::scrub(managed_envelope, project_info);

        Ok(Some(extracted_metrics))
    }

    /// Processes user sessions.
    async fn process_sessions(
        &self,
        managed_envelope: &mut TypedEnvelope<SessionGroup>,
        config: &Config,
        project_info: &ProjectInfo,
        rate_limits: &RateLimits,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        session::process(
            managed_envelope,
            &self.inner.global_config.current(),
            config,
            &mut extracted_metrics,
            project_info,
        );

        self.enforce_quotas(
            managed_envelope,
            Annotated::empty(),
            &mut extracted_metrics,
            project_info,
            rate_limits,
        )
        .await?;

        Ok(Some(extracted_metrics))
    }

    /// Processes user and client reports.
    async fn process_client_reports(
        &self,
        managed_envelope: &mut TypedEnvelope<ClientReportGroup>,
        config: Arc<Config>,
        project_info: Arc<ProjectInfo>,
        rate_limits: Arc<RateLimits>,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        self.enforce_quotas(
            managed_envelope,
            Annotated::empty(),
            &mut extracted_metrics,
            &project_info,
            &rate_limits,
        )
        .await?;

        report::process_client_reports(
            managed_envelope,
            &config,
            &project_info,
            self.inner.addrs.outcome_aggregator.clone(),
        );

        Ok(Some(extracted_metrics))
    }

    /// Processes replays.
    async fn process_replays(
        &self,
        managed_envelope: &mut TypedEnvelope<ReplayGroup>,
        config: Arc<Config>,
        project_info: Arc<ProjectInfo>,
        rate_limits: Arc<RateLimits>,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        replay::process(
            managed_envelope,
            &self.inner.global_config.current(),
            &config,
            &project_info,
            self.inner.geoip_lookup.as_ref(),
        )?;

        self.enforce_quotas(
            managed_envelope,
            Annotated::empty(),
            &mut extracted_metrics,
            &project_info,
            &rate_limits,
        )
        .await?;

        Ok(Some(extracted_metrics))
    }

    /// Processes cron check-ins.
    async fn process_checkins(
        &self,
        managed_envelope: &mut TypedEnvelope<CheckInGroup>,
        _project_id: ProjectId,
        project_info: Arc<ProjectInfo>,
        rate_limits: Arc<RateLimits>,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        self.enforce_quotas(
            managed_envelope,
            Annotated::empty(),
            &mut ProcessingExtractedMetrics::new(),
            &project_info,
            &rate_limits,
        )
        .await?;

        if_processing!(self.inner.config, {
            self.normalize_checkins(managed_envelope, _project_id);
        });

        Ok(None)
    }

    async fn process_nel(
        &self,
        mut managed_envelope: ManagedEnvelope,
        ctx: processing::Context<'_>,
    ) -> Result<ProcessingResult, ProcessingError> {
        nel::convert_to_logs(&mut managed_envelope);
        self.process_logs(managed_envelope, ctx).await
    }

    /// Process logs
    ///
    async fn process_logs(
        &self,
        mut managed_envelope: ManagedEnvelope,
        ctx: processing::Context<'_>,
    ) -> Result<ProcessingResult, ProcessingError> {
        let processor = &self.inner.processing.logs;
        let Some(logs) = processor.prepare_envelope(&mut managed_envelope) else {
            debug_assert!(
                false,
                "there must be work for the logs processor in the logs processing group"
            );
            return Err(ProcessingError::ProcessingGroupMismatch);
        };

        managed_envelope.update();
        match managed_envelope.envelope().is_empty() {
            true => managed_envelope.accept(),
            false => managed_envelope.reject(Outcome::Invalid(DiscardReason::Internal)),
        }

        processor
            .process(logs, ctx)
            .await
            .map_err(|_| ProcessingError::ProcessingFailure)
            .map(ProcessingResult::Logs)
    }

    /// Processes standalone spans.
    ///
    /// This function does *not* run for spans extracted from transactions.
    #[allow(clippy::too_many_arguments)]
    async fn process_standalone_spans(
        &self,
        managed_envelope: &mut TypedEnvelope<SpanGroup>,
        config: Arc<Config>,
        _project_id: ProjectId,
        project_info: Arc<ProjectInfo>,
        _sampling_project_info: Option<Arc<ProjectInfo>>,
        rate_limits: Arc<RateLimits>,
        _reservoir_counters: ReservoirCounters,
    ) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        span::expand_v2_spans(managed_envelope)?;
        span::filter(managed_envelope, config.clone(), project_info.clone());
        span::convert_otel_traces_data(managed_envelope);

        if_processing!(self.inner.config, {
            let global_config = self.inner.global_config.current();
            let reservoir = self.new_reservoir_evaluator(
                managed_envelope.scoping().organization_id,
                _reservoir_counters,
            );

            span::process(
                managed_envelope,
                &mut Annotated::empty(),
                &mut extracted_metrics,
                &global_config,
                config,
                _project_id,
                project_info.clone(),
                _sampling_project_info,
                self.inner.geoip_lookup.as_ref(),
                &reservoir,
            )
            .await;
        });

        self.enforce_quotas(
            managed_envelope,
            Annotated::empty(),
            &mut extracted_metrics,
            &project_info,
            &rate_limits,
        )
        .await?;

        Ok(Some(extracted_metrics))
    }

    async fn process_envelope(
        &self,
        cogs: &mut Token,
        project_id: ProjectId,
        message: ProcessEnvelopeGrouped,
    ) -> Result<ProcessingResult, ProcessingError> {
        let ProcessEnvelopeGrouped {
            group,
            envelope: mut managed_envelope,
            project_info,
            rate_limits,
            sampling_project_info,
            reservoir_counters,
        } = message;

        // Pre-process the envelope headers.
        if let Some(sampling_state) = sampling_project_info.as_ref() {
            // Both transactions and standalone span envelopes need a normalized DSC header
            // to make sampling rules based on the segment/transaction name work correctly.
            managed_envelope
                .envelope_mut()
                .parametrize_dsc_transaction(&sampling_state.config.tx_name_rules);
        }

        // Set the event retention. Effectively, this value will only be available in processing
        // mode when the full project config is queried from the upstream.
        if let Some(retention) = project_info.config.event_retention {
            managed_envelope.envelope_mut().set_retention(retention);
        }

        // Ensure the project ID is updated to the stored instance for this project cache. This can
        // differ in two cases:
        //  1. The envelope was sent to the legacy `/store/` endpoint without a project ID.
        //  2. The DSN was moved and the envelope sent to the old project ID.
        managed_envelope
            .envelope_mut()
            .meta_mut()
            .set_project_id(project_id);

        macro_rules! run {
            ($fn_name:ident $(, $args:expr)*) => {
                async {
                    let mut managed_envelope = (managed_envelope, group).try_into()?;
                    match self.$fn_name(&mut managed_envelope, $($args),*).await {
                        Ok(extracted_metrics) => Ok(ProcessingResult::Envelope {
                            managed_envelope: managed_envelope.into_processed(),
                            extracted_metrics: extracted_metrics.map_or(ProcessingExtractedMetrics::new(), |e| e)
                        }),
                        Err(error) => {
                            relay_log::trace!("Executing {fn} failed: {error}", fn = stringify!($fn_name), error = error);
                            if let Some(outcome) = error.to_outcome() {
                                managed_envelope.reject(outcome);
                            }

                            return Err(error);
                        }
                    }
                }.await
            };
        }

        let global_config = self.inner.global_config.current();
        let ctx = processing::Context {
            config: &self.inner.config,
            global_config: &global_config,
            project_info: &project_info,
            rate_limits: &rate_limits,
        };

        relay_log::trace!("Processing {group} group", group = group.variant());

        match group {
            ProcessingGroup::Error => run!(
                process_errors,
                project_id,
                project_info,
                sampling_project_info,
                rate_limits
            ),
            ProcessingGroup::Transaction => {
                run!(
                    process_transactions,
                    cogs,
                    self.inner.config.clone(),
                    project_id,
                    project_info,
                    sampling_project_info,
                    rate_limits,
                    reservoir_counters
                )
            }
            ProcessingGroup::Session => run!(
                process_sessions,
                &self.inner.config.clone(),
                &project_info,
                &rate_limits
            ),
            ProcessingGroup::Standalone => run!(
                process_standalone,
                self.inner.config.clone(),
                project_id,
                project_info,
                rate_limits
            ),
            ProcessingGroup::ClientReport => run!(
                process_client_reports,
                self.inner.config.clone(),
                project_info,
                rate_limits
            ),
            ProcessingGroup::Replay => {
                run!(
                    process_replays,
                    self.inner.config.clone(),
                    project_info,
                    rate_limits
                )
            }
            ProcessingGroup::CheckIn => {
                run!(process_checkins, project_id, project_info, rate_limits)
            }
            ProcessingGroup::Log => self.process_logs(managed_envelope, ctx).await,
            ProcessingGroup::Nel => self.process_nel(managed_envelope, ctx).await,
            ProcessingGroup::Span => run!(
                process_standalone_spans,
                self.inner.config.clone(),
                project_id,
                project_info,
                sampling_project_info,
                rate_limits,
                reservoir_counters
            ),
            ProcessingGroup::ProfileChunk => {
                run!(process_profile_chunks, project_info, rate_limits)
            }
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

                Ok(ProcessingResult::no_metrics(
                    managed_envelope.into_processed(),
                ))
            }
            // Fallback to the legacy process_state implementation for Ungrouped events.
            ProcessingGroup::Ungrouped => {
                relay_log::error!(
                    tags.project = %project_id,
                    items = ?managed_envelope.envelope().items().next().map(Item::ty),
                    "could not identify the processing group based on the envelope's items"
                );

                Ok(ProcessingResult::no_metrics(
                    managed_envelope.into_processed(),
                ))
            }
            // Leave this group unchanged.
            //
            // This will later be forwarded to upstream.
            ProcessingGroup::ForwardUnknown => Ok(ProcessingResult::no_metrics(
                managed_envelope.into_processed(),
            )),
        }
    }

    /// Processes the envelope and returns the processed envelope back.
    ///
    /// Returns `Some` if the envelope passed inbound filtering and rate limiting. Invalid items are
    /// removed from the envelope. Otherwise, if the envelope is empty or the entire envelope needs
    /// to be dropped, this is `None`.
    async fn process(
        &self,
        cogs: &mut Token,
        mut message: ProcessEnvelopeGrouped,
    ) -> Result<Option<Submit>, ProcessingError> {
        let ProcessEnvelopeGrouped {
            ref mut envelope,
            ref project_info,
            ..
        } = message;

        // Prefer the project's project ID, and fall back to the stated project id from the
        // envelope. The project ID is available in all modes, other than in proxy mode, where
        // envelopes for unknown projects are forwarded blindly.
        //
        // Neither ID can be available in proxy mode on the /store/ endpoint. This is not supported,
        // since we cannot process an envelope without project ID, so drop it.
        let Some(project_id) = project_info
            .project_id
            .or_else(|| envelope.envelope().meta().project_id())
        else {
            envelope.reject(Outcome::Invalid(DiscardReason::Internal));
            return Err(ProcessingError::MissingProjectId);
        };

        let client = envelope.envelope().meta().client().map(str::to_owned);
        let user_agent = envelope.envelope().meta().user_agent().map(str::to_owned);
        let project_key = envelope.envelope().meta().public_key();
        let sampling_key = envelope.envelope().sampling_key();

        let has_ourlogs_new_byte_count =
            project_info.has_feature(Feature::OurLogsCalculatedByteCount);

        // We set additional information on the scope, which will be removed after processing the
        // envelope.
        relay_log::configure_scope(|scope| {
            scope.set_tag("project", project_id);
            if let Some(client) = client {
                scope.set_tag("sdk", client);
            }
            if let Some(user_agent) = user_agent {
                scope.set_extra("user_agent", user_agent.into());
            }
        });

        let result = match self.process_envelope(cogs, project_id, message).await {
            Ok(ProcessingResult::Envelope {
                mut managed_envelope,
                extracted_metrics,
            }) => {
                // The envelope could be modified or even emptied during processing, which
                // requires re-computation of the context.
                managed_envelope.update();

                let has_metrics = !extracted_metrics.metrics.project_metrics.is_empty();
                send_metrics(
                    extracted_metrics.metrics,
                    project_key,
                    sampling_key,
                    &self.inner.addrs.aggregator,
                );

                let envelope_response = if managed_envelope.envelope().is_empty() {
                    if !has_metrics {
                        // Individual rate limits have already been issued
                        managed_envelope.reject(Outcome::RateLimited(None));
                    } else {
                        managed_envelope.accept();
                    }

                    None
                } else {
                    Some(managed_envelope)
                };

                Ok(envelope_response.map(Submit::Envelope))
            }
            Ok(ProcessingResult::Logs(Output { main, metrics })) => {
                send_metrics(
                    metrics.metrics,
                    project_key,
                    sampling_key,
                    &self.inner.addrs.aggregator,
                );

                if has_ourlogs_new_byte_count {
                    Ok(Some(Submit::Logs(main)))
                } else {
                    let envelope = main
                        .serialize_envelope()
                        .map_err(|_| ProcessingError::ProcessingEnvelopeSerialization)?;
                    let managed_envelope = ManagedEnvelope::from(envelope);

                    Ok(Some(Submit::Envelope(managed_envelope.into_processed())))
                }
            }
            Err(err) => Err(err),
        };

        relay_log::configure_scope(|scope| {
            scope.remove_tag("project");
            scope.remove_tag("sdk");
            scope.remove_tag("user_agent");
        });

        result
    }

    async fn handle_process_envelope(&self, cogs: &mut Token, message: ProcessEnvelope) {
        let project_key = message.envelope.envelope().meta().public_key();
        let wait_time = message.envelope.age();
        metric!(timer(RelayTimers::EnvelopeWaitTime) = wait_time);

        // This COGS handling may need an overhaul in the future:
        // Cancel the passed in token, to start individual measurements per envelope instead.
        cogs.cancel();

        let scoping = message.envelope.scoping();
        for (group, envelope) in ProcessingGroup::split_envelope(*message.envelope.into_envelope())
        {
            let mut cogs = self
                .inner
                .cogs
                .timed(ResourceId::Relay, AppFeature::from(group));

            let mut envelope = ManagedEnvelope::new(
                envelope,
                self.inner.addrs.outcome_aggregator.clone(),
                self.inner.addrs.test_store.clone(),
            );
            envelope.scope(scoping);

            let message = ProcessEnvelopeGrouped {
                group,
                envelope,
                project_info: Arc::clone(&message.project_info),
                rate_limits: Arc::clone(&message.rate_limits),
                sampling_project_info: message.sampling_project_info.as_ref().map(Arc::clone),
                reservoir_counters: Arc::clone(&message.reservoir_counters),
            };

            let result = metric!(
                timer(RelayTimers::EnvelopeProcessingTime),
                group = group.variant(),
                { self.process(&mut cogs, message).await }
            );

            match result {
                Ok(Some(envelope)) => self.submit_upstream(&mut cogs, envelope),
                Ok(None) => {}
                Err(error) if error.is_unexpected() => {
                    relay_log::error!(
                        tags.project_key = %project_key,
                        error = &error as &dyn Error,
                        "error processing envelope"
                    )
                }
                Err(error) => {
                    relay_log::debug!(
                        tags.project_key = %project_key,
                        error = &error as &dyn Error,
                        "error processing envelope"
                    )
                }
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

        let mut buckets = data.into_buckets(received_timestamp);
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

            if !self::metrics::is_valid_namespace(bucket, source) {
                return false;
            }

            clock_drift_processor.process_timestamp(&mut bucket.timestamp);

            if !matches!(source, BucketSource::Internal) {
                bucket.metadata = BucketMetadata::new(received_timestamp);
            }

            true
        });

        let project = self.inner.project_cache.get(project_key);

        // Best effort check to filter and rate limit buckets, if there is no project state
        // available at the current time, we will check again after flushing.
        let buckets = match project.state() {
            ProjectState::Enabled(project_info) => {
                let rate_limits = project.rate_limits().current_limits();
                self.check_buckets(project_key, project_info, &rate_limits, buckets)
            }
            _ => buckets,
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

    fn submit_upstream(&self, cogs: &mut Token, submit: Submit) {
        let _submit = cogs.start_category("submit");

        #[cfg(feature = "processing")]
        if self.inner.config.processing_enabled() {
            if let Some(store_forwarder) = &self.inner.addrs.store_forwarder {
                match submit {
                    Submit::Envelope(envelope) => store_forwarder.send(StoreEnvelope { envelope }),
                    Submit::Logs(output) => output
                        .forward_store(store_forwarder)
                        .unwrap_or_else(|err| err.into_inner()),
                }
                return;
            }
        }

        let mut envelope = match submit {
            Submit::Envelope(envelope) => envelope,
            Submit::Logs(output) => match output.serialize_envelope() {
                Ok(envelope) => ManagedEnvelope::from(envelope).into_processed(),
                Err(_) => {
                    relay_log::error!("failed to serialize output to an envelope");
                    return;
                }
            },
        };

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

    fn handle_submit_client_reports(&self, cogs: &mut Token, message: SubmitClientReports) {
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
        );
        self.submit_upstream(cogs, Submit::Envelope(envelope.into_processed()));
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
                bucket_limiter.enforce_limits(rate_limits, &self.inner.metric_outcomes);
                bucket_limiter.into_buckets()
            }
            Err(buckets) => buckets,
        }
    }

    #[cfg(feature = "processing")]
    async fn rate_limit_buckets(
        &self,
        scoping: Scoping,
        project_info: &ProjectInfo,
        mut buckets: Vec<Bucket>,
    ) -> Vec<Bucket> {
        let Some(rate_limiter) = &self.inner.rate_limiter else {
            return buckets;
        };

        let global_config = self.inner.global_config.current();
        let namespaces = buckets
            .iter()
            .filter_map(|bucket| bucket.name.try_namespace())
            .counts();

        let quotas = CombinedQuotas::new(&global_config, project_info.get_quotas());

        for (namespace, quantity) in namespaces {
            let item_scoping = scoping.metric_bucket(namespace);

            let limits = match rate_limiter
                .is_rate_limited(quotas, item_scoping, quantity, false)
                .await
            {
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

                self.inner
                    .project_cache
                    .get(item_scoping.scoping.project_key)
                    .rate_limits()
                    .merge(limits);
            }
        }

        match MetricsLimiter::create(buckets, project_info.config.quotas.clone(), scoping) {
            Err(buckets) => buckets,
            Ok(bucket_limiter) => self.apply_other_rate_limits(bucket_limiter).await,
        }
    }

    /// Check and apply rate limits to metrics buckets for transactions and spans.
    #[cfg(feature = "processing")]
    async fn apply_other_rate_limits(&self, mut bucket_limiter: MetricsLimiter) -> Vec<Bucket> {
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
                    match rate_limiter
                        .is_rate_limited(quotas, scoping.item(category), count, over_accept_once)
                        .await
                    {
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
                let was_enforced =
                    bucket_limiter.enforce_limits(&rate_limits, &self.inner.metric_outcomes);

                if was_enforced {
                    // Update the rate limits in the project cache.
                    self.inner
                        .project_cache
                        .get(scoping.project_key)
                        .rate_limits()
                        .merge(rate_limits);
                }
            }
        }

        bucket_limiter.into_buckets()
    }

    /// Cardinality limits the passed buckets and returns a filtered vector of only accepted buckets.
    #[cfg(feature = "processing")]
    async fn cardinality_limit_buckets(
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

        let limits = match limiter
            .check_cardinality_limits(scope, limits, buckets)
            .await
        {
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
        if !limits.exceeded_limits().is_empty() && utils::sample(error_sample_rate).is_keep() {
            for limit in limits.exceeded_limits() {
                relay_log::with_scope(
                    |scope| {
                        // Set the organization as user so we can alert on distinct org_ids.
                        scope.set_user(Some(relay_log::sentry::User {
                            id: Some(scoping.organization_id.to_string()),
                            ..Default::default()
                        }));
                    },
                    || {
                        relay_log::error!(
                            tags.organization_id = scoping.organization_id.value(),
                            tags.limit_id = limit.id,
                            tags.passive = limit.passive,
                            "Cardinality Limit"
                        );
                    },
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
    async fn encode_metrics_processing(
        &self,
        message: FlushBuckets,
        store_forwarder: &Addr<Store>,
    ) {
        use crate::constants::DEFAULT_EVENT_RETENTION;
        use crate::services::store::StoreMetrics;

        for ProjectBuckets {
            buckets,
            scoping,
            project_info,
            ..
        } in message.buckets.into_values()
        {
            let buckets = self
                .rate_limit_buckets(scoping, &project_info, buckets)
                .await;

            let limits = project_info.get_cardinality_limits();
            let buckets = self
                .cardinality_limit_buckets(scoping, limits, buckets)
                .await;

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
    fn encode_metrics_envelope(&self, cogs: &mut Token, message: FlushBuckets) {
        let FlushBuckets {
            partition_key,
            buckets,
        } = message;

        let batch_size = self.inner.config.metrics_max_batch_size_bytes();
        let upstream = self.inner.config.upstream_descriptor();

        for ProjectBuckets {
            buckets, scoping, ..
        } in buckets.values()
        {
            let dsn = PartialDsn::outbound(scoping, upstream);

            relay_statsd::metric!(
                histogram(RelayHistograms::PartitionKeys) = u64::from(partition_key)
            );

            let mut num_batches = 0;
            for batch in BucketsView::from(buckets).by_size(batch_size) {
                let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn.clone()));

                let mut item = Item::new(ItemType::MetricBuckets);
                item.set_source_quantities(crate::metrics::extract_quantities(batch));
                item.set_payload(ContentType::Json, serde_json::to_vec(&buckets).unwrap());
                envelope.add_item(item);

                let mut envelope = ManagedEnvelope::new(
                    envelope,
                    self.inner.addrs.outcome_aggregator.clone(),
                    self.inner.addrs.test_store.clone(),
                );
                envelope
                    .set_partition_key(Some(partition_key))
                    .scope(*scoping);

                relay_statsd::metric!(
                    histogram(RelayHistograms::BucketsPerBatch) = batch.len() as u64
                );

                self.submit_upstream(cogs, Submit::Envelope(envelope.into_processed()));
                num_batches += 1;
            }

            relay_statsd::metric!(histogram(RelayHistograms::BatchesPerPartition) = num_batches);
        }
    }

    /// Creates a [`SendMetricsRequest`] and sends it to the upstream relay.
    fn send_global_partition(&self, partition_key: u32, partition: &mut Partition<'_>) {
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
            partition_key: partition_key.to_string(),
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
    fn encode_metrics_global(&self, message: FlushBuckets) {
        let FlushBuckets {
            partition_key,
            buckets,
        } = message;

        let batch_size = self.inner.config.metrics_max_batch_size_bytes();
        let mut partition = Partition::new(batch_size);
        let mut partition_splits = 0;

        for ProjectBuckets {
            buckets, scoping, ..
        } in buckets.values()
        {
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

    async fn handle_flush_buckets(&self, cogs: &mut Token, mut message: FlushBuckets) {
        for (project_key, pb) in message.buckets.iter_mut() {
            let buckets = std::mem::take(&mut pb.buckets);
            pb.buckets =
                self.check_buckets(*project_key, &pb.project_info, &pb.rate_limits, buckets);
        }

        #[cfg(feature = "processing")]
        if self.inner.config.processing_enabled() {
            if let Some(ref store_forwarder) = self.inner.addrs.store_forwarder {
                return self
                    .encode_metrics_processing(message, store_forwarder)
                    .await;
            }
        }

        if self.inner.config.http_global_metrics() {
            self.encode_metrics_global(message)
        } else {
            self.encode_metrics_envelope(cogs, message)
        }
    }

    #[cfg(all(test, feature = "processing"))]
    fn redis_rate_limiter_enabled(&self) -> bool {
        self.inner.rate_limiter.is_some()
    }

    async fn handle_message(&self, message: EnvelopeProcessor) {
        let ty = message.variant();
        let feature_weights = self.feature_weights(&message);

        metric!(timer(RelayTimers::ProcessMessageDuration), message = ty, {
            let mut cogs = self.inner.cogs.timed(ResourceId::Relay, feature_weights);

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
                EnvelopeProcessor::FlushBuckets(m) => {
                    self.handle_flush_buckets(&mut cogs, *m).await
                }
                EnvelopeProcessor::SubmitClientReports(m) => {
                    self.handle_submit_client_reports(&mut cogs, *m)
                }
            }
        });
    }

    fn feature_weights(&self, message: &EnvelopeProcessor) -> FeatureWeights {
        match message {
            // Envelope is split later and tokens are attributed then.
            EnvelopeProcessor::ProcessEnvelope(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::ProcessProjectMetrics(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::ProcessBatchedMetrics(_) => AppFeature::Unattributed.into(),
            EnvelopeProcessor::FlushBuckets(v) => v
                .buckets
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
            EnvelopeProcessor::SubmitClientReports(_) => AppFeature::ClientReports.into(),
        }
    }

    fn new_reservoir_evaluator(
        &self,
        _organization_id: OrganizationId,
        reservoir_counters: ReservoirCounters,
    ) -> ReservoirEvaluator {
        #[cfg_attr(not(feature = "processing"), expect(unused_mut))]
        let mut reservoir = ReservoirEvaluator::new(reservoir_counters);

        #[cfg(feature = "processing")]
        if let Some(quotas_client) = self.inner.quotas_client.as_ref() {
            reservoir.set_redis(_organization_id, quotas_client);
        }

        reservoir
    }
}

impl Service for EnvelopeProcessorService {
    type Interface = EnvelopeProcessor;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            let service = self.clone();
            self.inner
                .pool
                .spawn_async(
                    async move {
                        service.handle_message(message).await;
                    }
                    .boxed(),
                )
                .await;
        }
    }
}

/// Result of the enforcement of rate limiting.
///
/// If the event is already `None` or it's rate limited, it will be `None`
/// within the [`Annotated`].
struct EnforcementResult {
    event: Annotated<Event>,
    #[cfg_attr(not(feature = "processing"), expect(dead_code))]
    rate_limits: RateLimits,
}

impl EnforcementResult {
    /// Creates a new [`EnforcementResult`].
    pub fn new(event: Annotated<Event>, rate_limits: RateLimits) -> Self {
        Self { event, rate_limits }
    }
}

#[derive(Clone)]
enum RateLimiter {
    Cached,
    #[cfg(feature = "processing")]
    Consistent(Arc<RedisRateLimiter<GlobalRateLimitsServiceHandle>>),
}

impl RateLimiter {
    async fn enforce<Group>(
        &self,
        managed_envelope: &mut TypedEnvelope<Group>,
        event: Annotated<Event>,
        _extracted_metrics: &mut ProcessingExtractedMetrics,
        global_config: &GlobalConfig,
        project_info: &ProjectInfo,
        rate_limits: &RateLimits,
    ) -> Result<EnforcementResult, ProcessingError> {
        if managed_envelope.envelope().is_empty() && event.value().is_none() {
            return Ok(EnforcementResult::new(event, RateLimits::default()));
        }

        let quotas = CombinedQuotas::new(global_config, project_info.get_quotas());
        if quotas.is_empty() {
            return Ok(EnforcementResult::new(event, RateLimits::default()));
        }

        let event_category = event_category(&event);

        // We extract the rate limiters, in case we perform consistent rate limiting, since we will
        // need Redis access.
        //
        // When invoking the rate limiter, capture if the event item has been rate limited to also
        // remove it from the processing state eventually.
        let this = self.clone();
        let rate_limits_clone = rate_limits.clone();
        let mut envelope_limiter =
            EnvelopeLimiter::new(CheckLimits::All, move |item_scope, _quantity| {
                let this = this.clone();
                let rate_limits_clone = rate_limits_clone.clone();

                async move {
                    match this {
                        #[cfg(feature = "processing")]
                        RateLimiter::Consistent(rate_limiter) => Ok::<_, ProcessingError>(
                            rate_limiter
                                .is_rate_limited(quotas, item_scope, _quantity, false)
                                .await?,
                        ),
                        _ => Ok::<_, ProcessingError>(
                            rate_limits_clone.check_with_quotas(quotas, item_scope),
                        ),
                    }
                }
            });

        // Tell the envelope limiter about the event, since it has been removed from the Envelope at
        // this stage in processing.
        if let Some(category) = event_category {
            envelope_limiter.assume_event(category);
        }

        let scoping = managed_envelope.scoping();
        let (enforcement, rate_limits) =
            metric!(timer(RelayTimers::EventProcessingRateLimiting), {
                envelope_limiter
                    .compute(managed_envelope.envelope_mut(), &scoping)
                    .await
            })?;
        let event_active = enforcement.is_event_active();

        // Use the same rate limits as used for the envelope on the metrics.
        // Those rate limits should not be checked for expiry or similar to ensure a consistent
        // limiting of envelope items and metrics.
        #[cfg(feature = "processing")]
        _extracted_metrics.apply_enforcement(&enforcement, matches!(self, Self::Consistent(_)));
        enforcement.apply_with_outcomes(managed_envelope);

        if event_active {
            debug_assert!(managed_envelope.envelope().is_empty());
            return Ok(EnforcementResult::new(Annotated::empty(), rate_limits));
        }

        Ok(EnforcementResult::new(event, rate_limits))
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
        HttpEncoding::Zstd => {
            // Use the fastest compression level, our main objective here is to get the best
            // compression ratio for least amount of time spent.
            let mut encoder = ZstdEncoder::new(Vec::new(), 1)?;
            encoder.write_all(body.as_ref())?;
            encoder.finish()?
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
    project_cache: ProjectCacheHandle,
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

    fn sign(&mut self) -> Option<Sign> {
        Some(Sign::Optional(SignatureType::RequestSign))
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
                        self.project_cache
                            .get(scoping.project_key)
                            .rate_limits()
                            .merge(limits.scope(&scoping));
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
/// [`FlushBuckets`] message if the `http.global_metrics` option is enabled. The container monitors
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
    partition_key: String,
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

    fn sign(&mut self) -> Option<Sign> {
        Some(Sign::Required(SignatureType::Body(self.unencoded.clone())))
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
            .header("X-Sentry-Relay-Shard", self.partition_key.as_bytes())
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
#[derive(Copy, Clone, Debug)]
struct CombinedQuotas<'a> {
    global_quotas: &'a [Quota],
    project_quotas: &'a [Quota],
}

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

impl<'a> IntoIterator for CombinedQuotas<'a> {
    type Item = &'a Quota;
    type IntoIter = std::iter::Chain<std::slice::Iter<'a, Quota>, std::slice::Iter<'a, Quota>>;

    fn into_iter(self) -> Self::IntoIter {
        self.global_quotas.iter().chain(self.project_quotas.iter())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::env;

    use insta::assert_debug_snapshot;
    use relay_base_schema::metrics::{DurationUnit, MetricUnit};
    use relay_common::glob2::LazyGlob;
    use relay_dynamic_config::ProjectConfig;
    use relay_event_normalization::{RedactionRule, TransactionNameRule};
    use relay_event_schema::protocol::TransactionSource;
    use relay_pii::DataScrubbingConfig;
    use similar_asserts::assert_eq;

    use crate::metrics_extraction::IntoMetric;
    use crate::metrics_extraction::transactions::types::{
        CommonTags, TransactionMeasurementTags, TransactionMetric,
    };
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

    /// Ensures that if we ratelimit one batch of buckets in [`FlushBuckets`] message, it won't
    /// also ratelimit the next batches in the same message automatically.
    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_ratelimit_per_batch() {
        use relay_base_schema::organization::OrganizationId;
        use relay_protocol::FiniteF64;

        let rate_limited_org = Scoping {
            organization_id: OrganizationId::new(1),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("00000000000000000000000000000000").unwrap(),
            key_id: Some(17),
        };

        let not_rate_limited_org = Scoping {
            organization_id: OrganizationId::new(2),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("11111111111111111111111111111111").unwrap(),
            key_id: Some(17),
        };

        let message = {
            let project_info = {
                let quota = Quota {
                    id: Some("testing".into()),
                    categories: vec![DataCategory::MetricBucket].into(),
                    scope: relay_quotas::QuotaScope::Organization,
                    scope_id: Some(rate_limited_org.organization_id.to_string()),
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

            let project_metrics = |scoping| ProjectBuckets {
                buckets: vec![Bucket {
                    name: "d:transactions/bar".into(),
                    value: BucketValue::Counter(FiniteF64::new(1.0).unwrap()),
                    timestamp: UnixTimestamp::now(),
                    tags: Default::default(),
                    width: 10,
                    metadata: BucketMetadata::default(),
                }],
                rate_limits: Default::default(),
                project_info: project_info.clone(),
                scoping,
            };

            let buckets = hashbrown::HashMap::from([
                (
                    rate_limited_org.project_key,
                    project_metrics(rate_limited_org),
                ),
                (
                    not_rate_limited_org.project_key,
                    project_metrics(not_rate_limited_org),
                ),
            ]);

            FlushBuckets {
                partition_key: 0,
                buckets,
            }
        };

        // ensure the order of the map while iterating is as expected.
        assert_eq!(message.buckets.keys().count(), 2);

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
            let f = |org_ids: &mut Vec<OrganizationId>, msg: Store| {
                let org_id = match msg {
                    Store::Metrics(x) => x.scoping.organization_id,
                    _ => panic!("received envelope when expecting only metrics"),
                };
                org_ids.push(org_id);
            };

            mock_service("store_forwarder", vec![], f)
        };

        let processor = create_test_processor(config).await;
        assert!(processor.redis_rate_limiter_enabled());

        processor.encode_metrics_processing(message, &store).await;

        drop(store);
        let orgs_not_ratelimited = handle.await.unwrap();

        assert_eq!(
            orgs_not_ratelimited,
            vec![not_rate_limited_org.organization_id]
        );
    }

    #[tokio::test]
    async fn test_browser_version_extraction_with_pii_like_data() {
        let processor = create_test_processor(Default::default()).await;
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
        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator, test_store);

        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            project_info: Arc::new(project_info),
            rate_limits: Default::default(),
            sampling_project_info: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let Ok(Some(Submit::Envelope(mut new_envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };
        let new_envelope = new_envelope.envelope_mut();

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
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "e12d836b15bb49d7bbf99e64295d995b",
            "sample_rate": "0.2"
        }"#;
        envelope.set_dsc(serde_json::from_str(dsc).unwrap());

        let mut item = Item::new(ItemType::Event);
        item.set_payload(ContentType::Json, r#"{}"#);
        envelope.add_item(item);

        let (outcome_aggregator, test_store) = testutils::processor_services();
        let managed_envelope = ManagedEnvelope::new(envelope, outcome_aggregator, test_store);

        let mut project_info = ProjectInfo::default();
        project_info.public_keys.push(PublicKeyConfig {
            public_key: ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
            numeric_id: Some(1),
        });
        let project_info = Arc::new(project_info);

        let message = ProcessEnvelopeGrouped {
            group: ProcessingGroup::Transaction,
            envelope: managed_envelope,
            project_info: project_info.clone(),
            rate_limits: Default::default(),
            sampling_project_info: Some(project_info),
            reservoir_counters: ReservoirCounters::default(),
        };

        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let processor = create_test_processor(config).await;
        let Ok(Some(Submit::Envelope(envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };
        let event = envelope
            .envelope()
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
                    "00000000000000000000000000000000",
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

    /// Confirms that the hardcoded value we use for the fixed length of the measurement MRI is
    /// correct. Unit test is placed here because it has dependencies to relay-server and therefore
    /// cannot be called from relay-metrics.
    #[test]
    fn test_mri_overhead_constant() {
        let hardcoded_value = MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD;

        let derived_value = {
            let name = "foobar".to_owned();
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

        let mut item = Item::new(ItemType::Statsd);
        item.set_payload(
            ContentType::Text,
            "transactions/foo:3182887624:4267882815|s",
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
            (
                ProjectKey("22222222222222222222222222222222"),
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
        ]
        "###);
    }
}
