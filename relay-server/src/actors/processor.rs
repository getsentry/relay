use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::error::Error;
use std::io::Write;
use std::net;
use std::net::IpAddr as NetIPAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use brotli::CompressorWriter as BrotliEncoder;
use bytes::Bytes;
use chrono::{DateTime, Duration as SignedDuration, Utc};
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use once_cell::sync::OnceCell;
use relay_auth::RelayVersion;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_common::time::UnixTimestamp;
use relay_config::{Config, HttpEncoding};
use relay_dynamic_config::{
    ErrorBoundary, Feature, GlobalConfig, ProjectConfig, SessionMetricsConfig,
};
use relay_event_normalization::replay::{self, ReplayError};
use relay_event_normalization::{
    ClockDriftProcessor, DynamicMeasurementsConfig, LightNormalizationConfig, MeasurementsConfig,
    TransactionNameConfig,
};
use relay_event_normalization::{GeoIpLookup, RawUserAgentInfo};
use relay_event_schema::processor::{self, ProcessingAction, ProcessingState};
use relay_event_schema::protocol::{
    Breadcrumb, ClientReport, Contexts, Csp, Event, EventType, ExpectCt, ExpectStaple, Hpkp,
    IpAddr, LenientString, Metrics, OtelContext, RelayInfo, Replay, SecurityReportType,
    SessionAggregates, SessionAttributes, SessionStatus, SessionUpdate, Timestamp, TraceContext,
    UserReport, Values,
};
use relay_filter::FilterStatKey;
use relay_metrics::{Bucket, MergeBuckets, MetricNamespace};
use relay_pii::{PiiAttachmentsProcessor, PiiConfigError, PiiProcessor};
use relay_profiling::ProfileError;
use relay_protocol::{Annotated, Array, Empty, FromValue, Object, Value};
use relay_quotas::{DataCategory, ReasonCode};
use relay_redis::RedisPool;
use relay_replays::recording::RecordingScrubber;
use relay_sampling::evaluation::MatchedRuleIds;
use relay_sampling::DynamicSamplingContext;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse, Service};
use serde_json::Value as SerdeValue;
use tokio::sync::Semaphore;

#[cfg(feature = "processing")]
use {
    crate::actors::envelopes::SendMetrics,
    crate::actors::project_cache::UpdateRateLimits,
    crate::utils::{EnvelopeLimiter, MetricsLimiter},
    relay_event_normalization::{StoreConfig, StoreProcessor},
    relay_event_schema::protocol::ProfileContext,
    relay_quotas::{RateLimitingError, RedisRateLimiter},
    symbolic_unreal::{Unreal4Error, Unreal4ErrorKind},
};

use crate::actors::envelopes::{EnvelopeManager, SendEnvelope, SendEnvelopeError, SubmitEnvelope};
use crate::actors::global_config::{GlobalConfigManager, Subscribe};
use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::project::ProjectState;
use crate::actors::project_cache::ProjectCache;
use crate::actors::upstream::{SendRequest, UpstreamRelay};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::transactions::{ExtractedMetrics, TransactionExtractor};
use crate::service::ServiceError;
use crate::statsd::{PlatformTag, RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{
    self, ChunkedFormDataAggregator, FormDataIter, ItemAction, ManagedEnvelope, SamplingResult,
};

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

    #[error("invalid security report type")]
    InvalidSecurityType,

    #[error("invalid security report")]
    InvalidSecurityReport(#[source] serde_json::Error),

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
            Self::InvalidSecurityType => Some(Outcome::Invalid(DiscardReason::SecurityReportType)),
            Self::InvalidSecurityReport(_) => Some(Outcome::Invalid(DiscardReason::SecurityReport)),
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
struct ProcessEnvelopeState {
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
    /// This defaults to [`SamplingResult::Keep`] and is determined based on dynamic sampling rules
    /// in the project configuration. In the drop case, this contains a list of rules that applied
    /// on the envelope.
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

    /// Whether there is a profiling item in the envelope.
    has_profile: bool,
}

impl ProcessEnvelopeState {
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

/// Fields of client reports that map to specific [`Outcome`]s without content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ClientReportField {
    /// The event has been filtered by an inbound data filter.
    Filtered,

    /// The event has been filtered by a sampling rule.
    FilteredSampling,

    /// The event has been rate limited.
    RateLimited,

    /// The event has already been discarded on the client side.
    ClientDiscard,
}

/// Parse an outcome from an outcome ID and a reason string.
///
/// Currently only used to reconstruct outcomes encoded in client reports.
fn outcome_from_parts(field: ClientReportField, reason: &str) -> Result<Outcome, ()> {
    match field {
        ClientReportField::FilteredSampling => match reason.strip_prefix("Sampled:") {
            Some(rule_ids) => MatchedRuleIds::parse(rule_ids)
                .map(Outcome::FilteredSampling)
                .map_err(|_| ()),
            None => Err(()),
        },
        ClientReportField::ClientDiscard => Ok(Outcome::ClientDiscard(reason.into())),
        ClientReportField::Filtered => Ok(Outcome::Filtered(
            FilterStatKey::try_from(reason).map_err(|_| ())?,
        )),
        ClientReportField::RateLimited => Ok(Outcome::RateLimited(match reason {
            "" => None,
            other => Some(ReasonCode::new(other)),
        })),
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

/// Applies rate limits to metrics buckets and forwards them to the envelope manager.
#[cfg(feature = "processing")]
#[derive(Debug)]
pub struct RateLimitFlushBuckets {
    pub bucket_limiter: MetricsLimiter,
    pub partition_key: Option<u64>,
}

/// CPU-intensive processing tasks for envelopes.
#[derive(Debug)]
pub enum EnvelopeProcessor {
    ProcessEnvelope(Box<ProcessEnvelope>),
    ProcessMetrics(Box<ProcessMetrics>),
    EncodeEnvelope(Box<EncodeEnvelope>),
    #[cfg(feature = "processing")]
    RateLimitFlushBuckets(RateLimitFlushBuckets),
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

impl FromMessage<EncodeEnvelope> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: EncodeEnvelope, _: ()) -> Self {
        Self::EncodeEnvelope(Box::new(message))
    }
}

#[cfg(feature = "processing")]
impl FromMessage<RateLimitFlushBuckets> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: RateLimitFlushBuckets, _: ()) -> Self {
        Self::RateLimitFlushBuckets(message)
    }
}

/// Service implementing the [`EnvelopeProcessor`] interface.
///
/// This service handles messages in a worker pool with configurable concurrency.
#[derive(Clone)]
pub struct EnvelopeProcessorService {
    global_config: Arc<GlobalConfig>,
    inner: Arc<InnerProcessor>,
}

struct InnerProcessor {
    config: Arc<Config>,
    envelope_manager: Addr<EnvelopeManager>,
    project_cache: Addr<ProjectCache>,
    global_config: Addr<GlobalConfigManager>,
    outcome_aggregator: Addr<TrackOutcome>,
    upstream_relay: Addr<UpstreamRelay>,
    #[cfg(feature = "processing")]
    rate_limiter: Option<RedisRateLimiter>,
    geoip_lookup: Option<GeoIpLookup>,
}

impl EnvelopeProcessorService {
    /// Creates a multi-threaded envelope processor.
    pub fn new(
        config: Arc<Config>,
        _redis: Option<RedisPool>,
        envelope_manager: Addr<EnvelopeManager>,
        outcome_aggregator: Addr<TrackOutcome>,
        project_cache: Addr<ProjectCache>,
        global_config: Addr<GlobalConfigManager>,
        upstream_relay: Addr<UpstreamRelay>,
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
            rate_limiter: _redis
                .map(|pool| RedisRateLimiter::new(pool).max_limit(config.max_rate_limit())),
            config,
            envelope_manager,
            project_cache,
            global_config,
            outcome_aggregator,
            upstream_relay,
            geoip_lookup,
        };

        Self {
            global_config: Arc::default(),
            inner: Arc::new(inner),
        }
    }

    /// Returns Ok(true) if attributes were modified.
    /// Returns Err if the session should be dropped.
    fn validate_attributes(
        &self,
        client_addr: &Option<net::IpAddr>,
        attributes: &mut SessionAttributes,
    ) -> Result<bool, ()> {
        let mut changed = false;

        let release = &attributes.release;
        if let Err(e) = relay_event_normalization::validate_release(release) {
            relay_log::trace!(
                error = &e as &dyn Error,
                release,
                "skipping session with invalid release"
            );
            return Err(());
        }

        if let Some(ref env) = attributes.environment {
            if let Err(e) = relay_event_normalization::validate_environment(env) {
                relay_log::trace!(
                    error = &e as &dyn Error,
                    env,
                    "removing invalid environment"
                );
                attributes.environment = None;
                changed = true;
            }
        }

        if let Some(ref ip_address) = attributes.ip_address {
            if ip_address.is_auto() {
                attributes.ip_address = client_addr.map(IpAddr::from);
                changed = true;
            }
        }

        Ok(changed)
    }

    fn is_valid_session_timestamp(
        &self,
        received: DateTime<Utc>,
        timestamp: DateTime<Utc>,
    ) -> bool {
        let max_age = SignedDuration::seconds(self.inner.config.max_session_secs_in_past());
        if (received - timestamp) > max_age {
            relay_log::trace!("skipping session older than {} days", max_age.num_days());
            return false;
        }

        let max_future = SignedDuration::seconds(self.inner.config.max_secs_in_future());
        if (timestamp - received) > max_future {
            relay_log::trace!(
                "skipping session more than {}s in the future",
                max_future.num_seconds()
            );
            return false;
        }

        true
    }

    /// Returns true if the item should be kept.
    #[allow(clippy::too_many_arguments)]
    fn process_session(
        &self,
        item: &mut Item,
        received: DateTime<Utc>,
        client: Option<&str>,
        client_addr: Option<net::IpAddr>,
        metrics_config: SessionMetricsConfig,
        clock_drift_processor: &ClockDriftProcessor,
        extracted_metrics: &mut Vec<Bucket>,
    ) -> bool {
        let mut changed = false;
        let payload = item.payload();

        // sessionupdate::parse is already tested
        let mut session = match SessionUpdate::parse(&payload) {
            Ok(session) => session,
            Err(error) => {
                relay_log::trace!(
                    error = &error as &dyn Error,
                    "skipping invalid session payload"
                );
                return false;
            }
        };

        if session.sequence == u64::MAX {
            relay_log::trace!("skipping session due to sequence overflow");
            return false;
        };

        if clock_drift_processor.is_drifted() {
            relay_log::trace!("applying clock drift correction to session");
            clock_drift_processor.process_datetime(&mut session.started);
            clock_drift_processor.process_datetime(&mut session.timestamp);
            changed = true;
        }

        if session.timestamp < session.started {
            relay_log::trace!("fixing session timestamp to {}", session.timestamp);
            session.timestamp = session.started;
            changed = true;
        }

        // Log the timestamp delay for all sessions after clock drift correction.
        let session_delay = received - session.timestamp;
        if session_delay > SignedDuration::minutes(1) {
            metric!(
                timer(RelayTimers::TimestampDelay) = session_delay.to_std().unwrap(),
                category = "session",
            );
        }

        // Validate timestamps
        for t in [session.timestamp, session.started] {
            if !self.is_valid_session_timestamp(received, t) {
                return false;
            }
        }

        // Validate attributes
        match self.validate_attributes(&client_addr, &mut session.attributes) {
            Err(_) => return false,
            Ok(changed_attributes) => {
                changed |= changed_attributes;
            }
        }

        if self.inner.config.processing_enabled()
            && matches!(session.status, SessionStatus::Unknown(_))
        {
            return false;
        }

        // Extract metrics if they haven't been extracted by a prior Relay
        if metrics_config.is_enabled()
            && !item.metrics_extracted()
            && !matches!(session.status, SessionStatus::Unknown(_))
        {
            crate::metrics_extraction::sessions::extract_session_metrics(
                &session.attributes,
                &session,
                client,
                extracted_metrics,
                metrics_config.should_extract_abnormal_mechanism(),
            );
            item.set_metrics_extracted(true);
        }

        // Drop the session if metrics have been extracted in this or a prior Relay
        if metrics_config.should_drop() && item.metrics_extracted() {
            return false;
        }

        if changed {
            let json_string = match serde_json::to_string(&session) {
                Ok(json) => json,
                Err(err) => {
                    relay_log::error!(error = &err as &dyn Error, "failed to serialize session");
                    return false;
                }
            };

            item.set_payload(ContentType::Json, json_string);
        }

        true
    }

    #[allow(clippy::too_many_arguments)]
    fn process_session_aggregates(
        &self,
        item: &mut Item,
        received: DateTime<Utc>,
        client: Option<&str>,
        client_addr: Option<net::IpAddr>,
        metrics_config: SessionMetricsConfig,
        clock_drift_processor: &ClockDriftProcessor,
        extracted_metrics: &mut Vec<Bucket>,
    ) -> bool {
        let mut changed = false;
        let payload = item.payload();

        let mut session = match SessionAggregates::parse(&payload) {
            Ok(session) => session,
            Err(error) => {
                relay_log::trace!(
                    error = &error as &dyn Error,
                    "skipping invalid sessions payload"
                );
                return false;
            }
        };

        if clock_drift_processor.is_drifted() {
            relay_log::trace!("applying clock drift correction to session");
            for aggregate in &mut session.aggregates {
                clock_drift_processor.process_datetime(&mut aggregate.started);
            }
            changed = true;
        }

        // Validate timestamps
        session
            .aggregates
            .retain(|aggregate| self.is_valid_session_timestamp(received, aggregate.started));

        // Aftter timestamp validation, aggregates could now be empty
        if session.aggregates.is_empty() {
            return false;
        }

        // Validate attributes
        match self.validate_attributes(&client_addr, &mut session.attributes) {
            Err(_) => return false,
            Ok(changed_attributes) => {
                changed |= changed_attributes;
            }
        }

        // Extract metrics if they haven't been extracted by a prior Relay
        if metrics_config.is_enabled() && !item.metrics_extracted() {
            for aggregate in &session.aggregates {
                crate::metrics_extraction::sessions::extract_session_metrics(
                    &session.attributes,
                    aggregate,
                    client,
                    extracted_metrics,
                    metrics_config.should_extract_abnormal_mechanism(),
                );
                item.set_metrics_extracted(true);
            }
        }

        // Drop the aggregate if metrics have been extracted in this or a prior Relay
        if metrics_config.should_drop() && item.metrics_extracted() {
            return false;
        }

        if changed {
            let json_string = match serde_json::to_string(&session) {
                Ok(json) => json,
                Err(err) => {
                    relay_log::error!(error = &err as &dyn Error, "failed to serialize session");
                    return false;
                }
            };

            item.set_payload(ContentType::Json, json_string);
        }

        true
    }

    /// Validates all sessions and session aggregates in the envelope, if any.
    ///
    /// Both are removed from the envelope if they contain invalid JSON or if their timestamps
    /// are out of range after clock drift correction.
    fn process_sessions(&self, state: &mut ProcessEnvelopeState) {
        let received = state.managed_envelope.received_at();
        let extracted_metrics = &mut state.extracted_metrics.project_metrics;
        let metrics_config = state.project_state.config().session_metrics;
        let envelope = state.managed_envelope.envelope_mut();
        let client = envelope.meta().client().map(|x| x.to_owned());
        let client_addr = envelope.meta().client_addr();

        let clock_drift_processor =
            ClockDriftProcessor::new(envelope.sent_at(), received).at_least(MINIMUM_CLOCK_DRIFT);

        state.managed_envelope.retain_items(|item| {
            let should_keep = match item.ty() {
                ItemType::Session => self.process_session(
                    item,
                    received,
                    client.as_deref(),
                    client_addr,
                    metrics_config,
                    &clock_drift_processor,
                    extracted_metrics,
                ),
                ItemType::Sessions => self.process_session_aggregates(
                    item,
                    received,
                    client.as_deref(),
                    client_addr,
                    metrics_config,
                    &clock_drift_processor,
                    extracted_metrics,
                ),
                _ => true, // Keep all other item types
            };
            if should_keep {
                ItemAction::Keep
            } else {
                ItemAction::DropSilently // sessions never log outcomes.
            }
        });
    }

    /// Validates and normalizes all user report items in the envelope.
    ///
    /// User feedback items are removed from the envelope if they contain invalid JSON or if the
    /// JSON violates the schema (basic type validation). Otherwise, their normalized representation
    /// is written back into the item.
    fn process_user_reports(&self, state: &mut ProcessEnvelopeState) {
        state.managed_envelope.retain_items(|item| {
            if item.ty() != &ItemType::UserReport {
                return ItemAction::Keep;
            };

            let report = match serde_json::from_slice::<UserReport>(&item.payload()) {
                Ok(session) => session,
                Err(error) => {
                    relay_log::error!(error = &error as &dyn Error, "failed to store user report");
                    return ItemAction::DropSilently;
                }
            };

            let json_string = match serde_json::to_string(&report) {
                Ok(json) => json,
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to serialize user report"
                    );
                    return ItemAction::DropSilently;
                }
            };

            item.set_payload(ContentType::Json, json_string);
            ItemAction::Keep
        });
    }

    /// Validates and extracts client reports.
    ///
    /// At the moment client reports are primarily used to transfer outcomes from
    /// client SDKs.  The outcomes are removed here and sent directly to the outcomes
    /// system.
    fn process_client_reports(&self, state: &mut ProcessEnvelopeState) {
        // if client outcomes are disabled we leave the the client reports unprocessed
        // and pass them on.
        if !self.inner.config.emit_outcomes().any() || !self.inner.config.emit_client_outcomes() {
            // if a processing relay has client outcomes disabled we drop them.
            if self.inner.config.processing_enabled() {
                state.managed_envelope.retain_items(|item| match item.ty() {
                    ItemType::ClientReport => ItemAction::DropSilently,
                    _ => ItemAction::Keep,
                });
            }
            return;
        }

        let mut timestamp = None;
        let mut output_events = BTreeMap::new();
        let received = state.managed_envelope.received_at();

        let clock_drift_processor = ClockDriftProcessor::new(state.envelope().sent_at(), received)
            .at_least(MINIMUM_CLOCK_DRIFT);

        // we're going through all client reports but we're effectively just merging
        // them into the first one.
        state.managed_envelope.retain_items(|item| {
            if item.ty() != &ItemType::ClientReport {
                return ItemAction::Keep;
            };
            match ClientReport::parse(&item.payload()) {
                Ok(ClientReport {
                    timestamp: report_timestamp,
                    discarded_events,
                    rate_limited_events,
                    filtered_events,
                    filtered_sampling_events,
                }) => {
                    // Glue all discarded events together and give them the appropriate outcome type
                    let input_events = discarded_events
                        .into_iter()
                        .map(|discarded_event| (ClientReportField::ClientDiscard, discarded_event))
                        .chain(
                            filtered_events.into_iter().map(|discarded_event| {
                                (ClientReportField::Filtered, discarded_event)
                            }),
                        )
                        .chain(filtered_sampling_events.into_iter().map(|discarded_event| {
                            (ClientReportField::FilteredSampling, discarded_event)
                        }))
                        .chain(rate_limited_events.into_iter().map(|discarded_event| {
                            (ClientReportField::RateLimited, discarded_event)
                        }));

                    for (outcome_type, discarded_event) in input_events {
                        if discarded_event.reason.len() > 200 {
                            relay_log::trace!("ignored client outcome with an overlong reason");
                            continue;
                        }
                        *output_events
                            .entry((
                                outcome_type,
                                discarded_event.reason,
                                discarded_event.category,
                            ))
                            .or_insert(0) += discarded_event.quantity;
                    }
                    if let Some(ts) = report_timestamp {
                        timestamp.get_or_insert(ts);
                    }
                }
                Err(err) => {
                    relay_log::trace!(error = &err as &dyn Error, "invalid client report received")
                }
            }
            ItemAction::DropSilently
        });

        if output_events.is_empty() {
            return;
        }

        let timestamp =
            timestamp.get_or_insert_with(|| UnixTimestamp::from_secs(received.timestamp() as u64));

        if clock_drift_processor.is_drifted() {
            relay_log::trace!("applying clock drift correction to client report");
            clock_drift_processor.process_timestamp(timestamp);
        }

        let max_age = SignedDuration::seconds(self.inner.config.max_secs_in_past());
        // also if we unable to parse the timestamp, we assume it's way too old here.
        let in_past = timestamp
            .as_datetime()
            .map(|ts| (received - ts) > max_age)
            .unwrap_or(true);
        if in_past {
            relay_log::trace!(
                "skipping client outcomes older than {} days",
                max_age.num_days()
            );
            return;
        }

        let max_future = SignedDuration::seconds(self.inner.config.max_secs_in_future());
        // also if we unable to parse the timestamp, we assume it's way far in the future here.
        let in_future = timestamp
            .as_datetime()
            .map(|ts| (ts - received) > max_future)
            .unwrap_or(true);
        if in_future {
            relay_log::trace!(
                "skipping client outcomes more than {}s in the future",
                max_future.num_seconds()
            );
            return;
        }

        for ((outcome_type, reason, category), quantity) in output_events.into_iter() {
            let outcome = match outcome_from_parts(outcome_type, &reason) {
                Ok(outcome) => outcome,
                Err(_) => {
                    relay_log::trace!(?outcome_type, reason, "invalid outcome combination");
                    continue;
                }
            };

            self.inner.outcome_aggregator.send(TrackOutcome {
                // If we get to this point, the unwrap should not be used anymore, since we know by
                // now that the timestamp can be parsed, but just incase we fallback to UTC current
                // `DateTime`.
                timestamp: timestamp.as_datetime().unwrap_or_else(Utc::now),
                scoping: state.managed_envelope.scoping(),
                outcome,
                event_id: None,
                remote_addr: None, // omitting the client address allows for better aggregation
                category,
                quantity,
            });
        }
    }

    /// Remove profiles from the envelope if they can not be parsed
    fn filter_profiles(&self, state: &mut ProcessEnvelopeState) {
        let transaction_count: usize = state
            .managed_envelope
            .envelope()
            .items()
            .filter(|item| item.ty() == &ItemType::Transaction)
            .count();
        let mut found_profile = false;
        state.managed_envelope.retain_items(|item| match item.ty() {
            // Drop profile without a transaction in the same envelope.
            ItemType::Profile if transaction_count == 0 => ItemAction::DropSilently,
            ItemType::Profile => {
                if !found_profile {
                    match relay_profiling::parse_metadata(&item.payload()) {
                        Ok(_) => {
                            found_profile = true;
                            ItemAction::Keep
                        }
                        Err(err) => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                            relay_profiling::discard_reason(err),
                        ))),
                    }
                } else {
                    // We found a second profile, drop it.
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                        relay_profiling::discard_reason(ProfileError::TooManyProfiles),
                    )));
                }
            }
            _ => ItemAction::Keep,
        });
        state.has_profile = found_profile;
    }

    /// Normalize monitor check-ins and remove invalid ones.
    #[cfg(feature = "processing")]
    fn process_check_ins(&self, state: &mut ProcessEnvelopeState) {
        state.managed_envelope.retain_items(|item| {
            if item.ty() != &ItemType::CheckIn {
                return ItemAction::Keep;
            }

            match relay_monitors::process_check_in(&item.payload()) {
                Ok(processed) => {
                    item.set_payload(ContentType::Json, processed);
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

    /// Process profiles and set the profile ID in the profile context on the transaction if successful
    #[cfg(feature = "processing")]
    fn process_profiles(&self, state: &mut ProcessEnvelopeState) {
        state.managed_envelope.retain_items(|item| match item.ty() {
            ItemType::Profile => {
                match relay_profiling::expand_profile(&item.payload(), state.event.value()) {
                    Ok((profile_id, payload)) => {
                        if payload.len() <= self.inner.config.max_profile_size() {
                            if let Some(event) = state.event.value_mut() {
                                if event.ty.value() == Some(&EventType::Transaction) {
                                    let contexts = event.contexts.get_or_insert_with(Contexts::new);
                                    contexts.add(ProfileContext {
                                        profile_id: Annotated::new(profile_id),
                                    });
                                }
                            }
                            item.set_payload(ContentType::Json, payload);
                            ItemAction::Keep
                        } else {
                            if let Some(event) = state.event.value_mut() {
                                if event.ty.value() == Some(&EventType::Transaction) {
                                    if let Some(ref mut contexts) = event.contexts.value_mut() {
                                        contexts.remove::<ProfileContext>();
                                    }
                                }
                            }
                            ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                                relay_profiling::discard_reason(
                                    relay_profiling::ProfileError::ExceedSizeLimit,
                                ),
                            )))
                        }
                    }
                    Err(err) => {
                        if let Some(event) = state.event.value_mut() {
                            if event.ty.value() == Some(&EventType::Transaction) {
                                let contexts = event.contexts.get_or_insert_with(Contexts::new);
                                contexts.remove::<ProfileContext>();
                            }
                        }

                        match err {
                            relay_profiling::ProfileError::InvalidJson(_) => {
                                relay_log::warn!(error = &err as &dyn Error, "invalid profile");
                            }
                            _ => relay_log::debug!(error = &err as &dyn Error, "invalid profile"),
                        };
                        ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                            relay_profiling::discard_reason(err),
                        )))
                    }
                }
            }
            _ => ItemAction::Keep,
        });
    }

    /// Remove replays if the feature flag is not enabled.
    fn process_replays(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let project_state = &state.project_state;
        let replays_enabled = project_state.has_feature(Feature::SessionReplay);
        let scrubbing_enabled = project_state.has_feature(Feature::SessionReplayRecordingScrubbing);

        let meta = state.envelope().meta().clone();
        let client_addr = meta.client_addr();
        let event_id = state.envelope().event_id();

        let limit = self.inner.config.max_replay_uncompressed_size();
        let config = project_state.config();
        let datascrubbing_config = config
            .datascrubbing_settings
            .pii_config()
            .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?
            .as_ref();
        let mut scrubber =
            RecordingScrubber::new(limit, config.pii_config.as_ref(), datascrubbing_config);

        let user_agent = &RawUserAgentInfo {
            user_agent: meta.user_agent(),
            client_hints: meta.client_hints().as_deref(),
        };

        state.managed_envelope.retain_items(|item| match item.ty() {
            ItemType::ReplayEvent => {
                if !replays_enabled {
                    return ItemAction::DropSilently;
                }

                match self.process_replay_event(&item.payload(), config, client_addr, user_agent) {
                    Ok(replay) => match replay.to_json() {
                        Ok(json) => {
                            item.set_payload(ContentType::Json, json);
                            ItemAction::Keep
                        }
                        Err(error) => {
                            relay_log::error!(
                                error = &error as &dyn Error,
                                "failed to serialize replay"
                            );
                            ItemAction::Keep
                        }
                    },
                    Err(error) => {
                        relay_log::warn!(error = &error as &dyn Error, "invalid replay event");
                        ItemAction::Drop(Outcome::Invalid(match error {
                            ReplayError::NoContent => DiscardReason::InvalidReplayEventNoPayload,
                            ReplayError::CouldNotScrub(_) => DiscardReason::InvalidReplayEventPii,
                            ReplayError::CouldNotParse(_) => DiscardReason::InvalidReplayEvent,
                            ReplayError::InvalidPayload(_) => DiscardReason::InvalidReplayEvent,
                        }))
                    }
                }
            }
            ItemType::ReplayRecording => {
                if !replays_enabled {
                    return ItemAction::DropSilently;
                }

                // XXX: Processing is there just for data scrubbing. Skip the entire expensive
                // processing step if we do not need to scrub.
                if !scrubbing_enabled || scrubber.is_empty() {
                    return ItemAction::Keep;
                }

                // Limit expansion of recordings to the max replay size. The payload is
                // decompressed temporarily and then immediately re-compressed. However, to
                // limit memory pressure, we use the replay limit as a good overall limit for
                // allocations.
                let parsed_recording = metric!(timer(RelayTimers::ReplayRecordingProcessing), {
                    scrubber.process_recording(&item.payload())
                });

                match parsed_recording {
                    Ok(recording) => {
                        item.set_payload(ContentType::OctetStream, recording);
                        ItemAction::Keep
                    }
                    Err(e) => {
                        relay_log::warn!("replay-recording-event: {e} {event_id:?}");
                        ItemAction::Drop(Outcome::Invalid(
                            DiscardReason::InvalidReplayRecordingEvent,
                        ))
                    }
                }
            }
            _ => ItemAction::Keep,
        });

        Ok(())
    }

    /// Validates, normalizes, and scrubs PII from a replay event.
    fn process_replay_event(
        &self,
        payload: &Bytes,
        config: &ProjectConfig,
        client_ip: Option<NetIPAddr>,
        user_agent: &RawUserAgentInfo<&str>,
    ) -> Result<Annotated<Replay>, ReplayError> {
        let mut replay =
            Annotated::<Replay>::from_json_bytes(payload).map_err(ReplayError::CouldNotParse)?;

        if let Some(replay_value) = replay.value_mut() {
            replay::validate(replay_value)?;
            replay::normalize(replay_value, client_ip, user_agent);
        } else {
            return Err(ReplayError::NoContent);
        }

        if let Some(ref config) = config.pii_config {
            let mut processor = PiiProcessor::new(config.compiled());
            processor::process_value(&mut replay, &mut processor, ProcessingState::root())
                .map_err(|e| ReplayError::CouldNotScrub(e.to_string()))?;
        }

        let pii_config = config
            .datascrubbing_settings
            .pii_config()
            .map_err(|e| ReplayError::CouldNotScrub(e.to_string()))?;
        if let Some(config) = pii_config {
            let mut processor = PiiProcessor::new(config.compiled());
            processor::process_value(&mut replay, &mut processor, ProcessingState::root())
                .map_err(|e| ReplayError::CouldNotScrub(e.to_string()))?;
        }

        Ok(replay)
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

        Ok(ProcessEnvelopeState {
            event: Annotated::empty(),
            event_metrics_extracted: false,
            metrics: Metrics::default(),
            sample_rates: None,
            sampling_result: SamplingResult::Keep,
            extracted_metrics: Default::default(),
            project_state,
            sampling_project_state,
            project_id,
            managed_envelope,
            has_profile: false,
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

    fn event_from_json_payload(
        &self,
        item: Item,
        event_type: Option<EventType>,
    ) -> Result<ExtractedEvent, ProcessingError> {
        let mut event = Annotated::<Event>::from_json_bytes(&item.payload())
            .map_err(ProcessingError::InvalidJson)?;

        if let Some(event_value) = event.value_mut() {
            event_value.ty.set_value(event_type);
        }

        Ok((event, item.len()))
    }

    fn event_from_security_report(
        &self,
        item: Item,
        meta: &RequestMeta,
    ) -> Result<ExtractedEvent, ProcessingError> {
        let len = item.len();
        let mut event = Event::default();

        let data = &item.payload();
        let report_type = SecurityReportType::from_json(data)
            .map_err(ProcessingError::InvalidJson)?
            .ok_or(ProcessingError::InvalidSecurityType)?;

        let apply_result = match report_type {
            SecurityReportType::Csp => Csp::apply_to_event(data, &mut event),
            SecurityReportType::ExpectCt => ExpectCt::apply_to_event(data, &mut event),
            SecurityReportType::ExpectStaple => ExpectStaple::apply_to_event(data, &mut event),
            SecurityReportType::Hpkp => Hpkp::apply_to_event(data, &mut event),
        };

        if let Err(json_error) = apply_result {
            // logged in extract_event
            relay_log::configure_scope(|scope| {
                scope.set_extra("payload", String::from_utf8_lossy(data).into());
            });

            return Err(ProcessingError::InvalidSecurityReport(json_error));
        }

        if let Some(release) = item.get_header("sentry_release").and_then(Value::as_str) {
            event.release = Annotated::from(LenientString(release.to_owned()));
        }

        if let Some(env) = item
            .get_header("sentry_environment")
            .and_then(Value::as_str)
        {
            event.environment = Annotated::from(env.to_owned());
        }

        if let Some(origin) = meta.origin() {
            event
                .request
                .get_or_insert_with(Default::default)
                .headers
                .get_or_insert_with(Default::default)
                .insert("Origin".into(), Annotated::new(origin.to_string().into()));
        }

        // Explicitly set the event type. This is required so that a `Security` item can be created
        // instead of a regular `Event` item.
        event.ty = Annotated::new(match report_type {
            SecurityReportType::Csp => EventType::Csp,
            SecurityReportType::ExpectCt => EventType::ExpectCt,
            SecurityReportType::ExpectStaple => EventType::ExpectStaple,
            SecurityReportType::Hpkp => EventType::Hpkp,
        });

        Ok((Annotated::new(event), len))
    }

    fn merge_formdata(&self, target: &mut SerdeValue, item: Item) {
        let payload = item.payload();
        let mut aggregator = ChunkedFormDataAggregator::new();

        for entry in FormDataIter::new(&payload) {
            if entry.key() == "sentry" || entry.key().starts_with("sentry___") {
                // Custom clients can submit longer payloads and should JSON encode event data into
                // the optional `sentry` field or a `sentry___<namespace>` field.
                match serde_json::from_str(entry.value()) {
                    Ok(event) => utils::merge_values(target, event),
                    Err(_) => relay_log::debug!("invalid json event payload in sentry form field"),
                }
            } else if let Some(index) = utils::get_sentry_chunk_index(entry.key(), "sentry__") {
                // Electron SDK splits up long payloads into chunks starting at sentry__1 with an
                // incrementing counter. Assemble these chunks here and then decode them below.
                aggregator.insert(index, entry.value());
            } else if let Some(keys) = utils::get_sentry_entry_indexes(entry.key()) {
                // Try to parse the nested form syntax `sentry[key][key]` This is required for the
                // Breakpad client library, which only supports string values of up to 64
                // characters.
                utils::update_nested_value(target, &keys, entry.value());
            } else {
                // Merge additional form fields from the request with `extra` data from the event
                // payload and set defaults for processing. This is sent by clients like Breakpad or
                // Crashpad.
                utils::update_nested_value(target, &["extra", entry.key()], entry.value());
            }
        }

        if !aggregator.is_empty() {
            match serde_json::from_str(&aggregator.join()) {
                Ok(event) => utils::merge_values(target, event),
                Err(_) => relay_log::debug!("invalid json event payload in sentry__* form fields"),
            }
        }
    }

    fn extract_attached_event(
        config: &Config,
        item: Option<Item>,
    ) -> Result<Annotated<Event>, ProcessingError> {
        let item = match item {
            Some(item) if !item.is_empty() => item,
            _ => return Ok(Annotated::new(Event::default())),
        };

        // Protect against blowing up during deserialization. Attachments can have a significantly
        // larger size than regular events and may cause significant processing delays.
        if item.len() > config.max_event_size() {
            return Err(ProcessingError::PayloadTooLarge);
        }

        let payload = item.payload();
        let deserializer = &mut rmp_serde::Deserializer::from_read_ref(payload.as_ref());
        Annotated::deserialize_with_meta(deserializer).map_err(ProcessingError::InvalidMsgpack)
    }

    fn parse_msgpack_breadcrumbs(
        config: &Config,
        item: Option<Item>,
    ) -> Result<Array<Breadcrumb>, ProcessingError> {
        let mut breadcrumbs = Array::new();
        let item = match item {
            Some(item) if !item.is_empty() => item,
            _ => return Ok(breadcrumbs),
        };

        // Validate that we do not exceed the maximum breadcrumb payload length. Breadcrumbs are
        // truncated to a maximum of 100 in event normalization, but this is to protect us from
        // blowing up during deserialization. As approximation, we use the maximum event payload
        // size as bound, which is roughly in the right ballpark.
        if item.len() > config.max_event_size() {
            return Err(ProcessingError::PayloadTooLarge);
        }

        let payload = item.payload();
        let mut deserializer = rmp_serde::Deserializer::new(payload.as_ref());

        while !deserializer.get_ref().is_empty() {
            let breadcrumb = Annotated::deserialize_with_meta(&mut deserializer)?;
            breadcrumbs.push(breadcrumb);
        }

        Ok(breadcrumbs)
    }

    fn event_from_attachments(
        config: &Config,
        event_item: Option<Item>,
        breadcrumbs_item1: Option<Item>,
        breadcrumbs_item2: Option<Item>,
    ) -> Result<ExtractedEvent, ProcessingError> {
        let len = event_item.as_ref().map_or(0, |item| item.len())
            + breadcrumbs_item1.as_ref().map_or(0, |item| item.len())
            + breadcrumbs_item2.as_ref().map_or(0, |item| item.len());

        let mut event = Self::extract_attached_event(config, event_item)?;
        let mut breadcrumbs1 = Self::parse_msgpack_breadcrumbs(config, breadcrumbs_item1)?;
        let mut breadcrumbs2 = Self::parse_msgpack_breadcrumbs(config, breadcrumbs_item2)?;

        let timestamp1 = breadcrumbs1
            .iter()
            .rev()
            .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

        let timestamp2 = breadcrumbs2
            .iter()
            .rev()
            .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

        // Sort breadcrumbs by date. We presume that last timestamp from each row gives the
        // relative sequence of the whole sequence, i.e., we don't need to splice the sequences
        // to get the breadrumbs sorted.
        if timestamp1 > timestamp2 {
            std::mem::swap(&mut breadcrumbs1, &mut breadcrumbs2);
        }

        // Limit the total length of the breadcrumbs. We presume that if we have both
        // breadcrumbs with items one contains the maximum number of breadcrumbs allowed.
        let max_length = std::cmp::max(breadcrumbs1.len(), breadcrumbs2.len());

        breadcrumbs1.extend(breadcrumbs2);

        if breadcrumbs1.len() > max_length {
            // Keep only the last max_length elements from the vectors
            breadcrumbs1.drain(0..(breadcrumbs1.len() - max_length));
        }

        if !breadcrumbs1.is_empty() {
            event.get_or_insert_with(Event::default).breadcrumbs = Annotated::new(Values {
                values: Annotated::new(breadcrumbs1),
                other: Object::default(),
            });
        }

        Ok((event, len))
    }

    /// Checks for duplicate items in an envelope.
    ///
    /// An item is considered duplicate if it was not removed by sanitation in `process_event` and
    /// `extract_event`. This partially depends on the `processing_enabled` flag.
    fn is_duplicate(&self, item: &Item) -> bool {
        match item.ty() {
            // These should always be removed by `extract_event`:
            ItemType::Event => true,
            ItemType::Transaction => true,
            ItemType::Security => true,
            ItemType::FormData => true,
            ItemType::RawSecurity => true,

            // These should be removed conditionally:
            ItemType::UnrealReport => self.inner.config.processing_enabled(),

            // These may be forwarded to upstream / store:
            ItemType::Attachment => false,
            ItemType::UserReport => false,

            // Aggregate data is never considered as part of deduplication
            ItemType::Session => false,
            ItemType::Sessions => false,
            ItemType::Statsd => false,
            ItemType::MetricBuckets => false,
            ItemType::ClientReport => false,
            ItemType::Profile => false,
            ItemType::ReplayEvent => false,
            ItemType::ReplayRecording => false,
            ItemType::CheckIn => false,
            ItemType::Span => false,

            // Without knowing more, `Unknown` items are allowed to be repeated
            ItemType::Unknown(_) => false,
        }
    }

    /// Extracts the primary event payload from an envelope.
    ///
    /// The event is obtained from only one source in the following precedence:
    ///  1. An explicit event item. This is also the case for JSON uploads.
    ///  2. A security report item.
    ///  3. Attachments `__sentry-event` and `__sentry-breadcrumb1/2`.
    ///  4. A multipart form data body.
    ///  5. If none match, `Annotated::empty()`.
    fn extract_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let envelope = &mut state.envelope_mut();

        // Remove all items first, and then process them. After this function returns, only
        // attachments can remain in the envelope. The event will be added again at the end of
        // `process_event`.
        let event_item = envelope.take_item_by(|item| item.ty() == &ItemType::Event);
        let transaction_item = envelope.take_item_by(|item| item.ty() == &ItemType::Transaction);
        let security_item = envelope.take_item_by(|item| item.ty() == &ItemType::Security);
        let raw_security_item = envelope.take_item_by(|item| item.ty() == &ItemType::RawSecurity);
        let form_item = envelope.take_item_by(|item| item.ty() == &ItemType::FormData);
        let attachment_item = envelope
            .take_item_by(|item| item.attachment_type() == Some(&AttachmentType::EventPayload));
        let breadcrumbs1 = envelope
            .take_item_by(|item| item.attachment_type() == Some(&AttachmentType::Breadcrumbs));
        let breadcrumbs2 = envelope
            .take_item_by(|item| item.attachment_type() == Some(&AttachmentType::Breadcrumbs));

        // Event items can never occur twice in an envelope.
        if let Some(duplicate) = envelope.get_item_by(|item| self.is_duplicate(item)) {
            return Err(ProcessingError::DuplicateItem(duplicate.ty().clone()));
        }

        let mut sample_rates = None;
        let (event, event_len) = if let Some(mut item) = event_item.or(security_item) {
            relay_log::trace!("processing json event");
            sample_rates = item.take_sample_rates();
            metric!(timer(RelayTimers::EventProcessingDeserialize), {
                // Event items can never include transactions, so retain the event type and let
                // inference deal with this during store normalization.
                self.event_from_json_payload(item, None)?
            })
        } else if let Some(mut item) = transaction_item {
            relay_log::trace!("processing json transaction");
            sample_rates = item.take_sample_rates();
            state.event_metrics_extracted = item.metrics_extracted();
            metric!(timer(RelayTimers::EventProcessingDeserialize), {
                // Transaction items can only contain transaction events. Force the event type to
                // hint to normalization that we're dealing with a transaction now.
                self.event_from_json_payload(item, Some(EventType::Transaction))?
            })
        } else if let Some(mut item) = raw_security_item {
            relay_log::trace!("processing security report");
            sample_rates = item.take_sample_rates();
            self.event_from_security_report(item, envelope.meta())
                .map_err(|error| {
                    relay_log::error!(
                        error = &error as &dyn Error,
                        "failed to extract security report"
                    );
                    error
                })?
        } else if attachment_item.is_some() || breadcrumbs1.is_some() || breadcrumbs2.is_some() {
            relay_log::trace!("extracting attached event data");
            Self::event_from_attachments(
                &self.inner.config,
                attachment_item,
                breadcrumbs1,
                breadcrumbs2,
            )?
        } else if let Some(item) = form_item {
            relay_log::trace!("extracting form data");
            let len = item.len();

            let mut value = SerdeValue::Object(Default::default());
            self.merge_formdata(&mut value, item);
            let event = Annotated::deserialize_with_meta(value).unwrap_or_default();

            (event, len)
        } else {
            relay_log::trace!("no event in envelope");
            (Annotated::empty(), 0)
        };

        state.event = event;
        state.sample_rates = sample_rates;
        state.metrics.bytes_ingested_event = Annotated::new(event_len as u64);

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

    fn finalize_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let is_transaction = state.event_type() == Some(EventType::Transaction);
        let envelope = state.managed_envelope.envelope_mut();

        let event = match state.event.value_mut() {
            Some(event) => event,
            None if !self.inner.config.processing_enabled() => return Ok(()),
            None => return Err(ProcessingError::NoEventPayload),
        };

        if !self.inner.config.processing_enabled() {
            static MY_VERSION_STRING: OnceCell<String> = OnceCell::new();
            let my_version = MY_VERSION_STRING.get_or_init(|| RelayVersion::current().to_string());

            event
                .ingest_path
                .get_or_insert_with(Default::default)
                .push(Annotated::new(RelayInfo {
                    version: Annotated::new(my_version.clone()),
                    public_key: self
                        .inner
                        .config
                        .public_key()
                        .map_or(Annotated::empty(), |pk| Annotated::new(pk.to_string())),
                    other: Default::default(),
                }));
        }

        // Event id is set statically in the ingest path.
        let event_id = envelope.event_id().unwrap_or_default();
        debug_assert!(!event_id.is_nil());

        // Ensure that the event id in the payload is consistent with the envelope. If an event
        // id was ingested, this will already be the case. Otherwise, this will insert a new
        // event id. To be defensive, we always overwrite to ensure consistency.
        event.id = Annotated::new(event_id);

        // In processing mode, also write metrics into the event. Most metrics have already been
        // collected at this state, except for the combined size of all attachments.
        if self.inner.config.processing_enabled() {
            let mut metrics = std::mem::take(&mut state.metrics);

            let attachment_size = envelope
                .items()
                .filter(|item| item.attachment_type() == Some(&AttachmentType::Attachment))
                .map(|item| item.len() as u64)
                .sum::<u64>();

            if attachment_size > 0 {
                metrics.bytes_ingested_event_attachment = Annotated::new(attachment_size);
            }

            let sample_rates = state
                .sample_rates
                .take()
                .and_then(|value| Array::from_value(Annotated::new(value)).into_value());

            if let Some(rates) = sample_rates {
                metrics
                    .sample_rates
                    .get_or_insert_with(Array::new)
                    .extend(rates)
            }

            event._metrics = Annotated::new(metrics);

            if event.ty.value() == Some(&EventType::Transaction) {
                metric!(
                    counter(RelayCounters::EventTransaction) += 1,
                    source = utils::transaction_source_tag(event),
                    platform =
                        PlatformTag::from(event.platform.as_str().unwrap_or("other")).as_str(),
                    contains_slashes =
                        if event.transaction.as_str().unwrap_or_default().contains('/') {
                            "true"
                        } else {
                            "false"
                        }
                );

                let span_count = event.spans.value().map(Vec::len).unwrap_or(0) as u64;
                metric!(
                    histogram(RelayHistograms::EventSpans) = span_count,
                    sdk = envelope.meta().client_name().unwrap_or("proprietary"),
                    platform = event.platform.as_str().unwrap_or("other"),
                );

                let has_otel = event
                    .contexts
                    .value()
                    .map_or(false, |contexts| contexts.contains::<OtelContext>());

                if has_otel {
                    metric!(
                        counter(RelayCounters::OpenTelemetryEvent) += 1,
                        sdk = envelope.meta().client_name().unwrap_or("proprietary"),
                        platform = event.platform.as_str().unwrap_or("other"),
                    );
                }
            }
        }

        // TODO: Temporary workaround before processing. Experimental SDKs relied on a buggy
        // clock drift correction that assumes the event timestamp is the sent_at time. This
        // should be removed as soon as legacy ingestion has been removed.
        let sent_at = match envelope.sent_at() {
            Some(sent_at) => Some(sent_at),
            None if is_transaction => event.timestamp.value().copied().map(Timestamp::into_inner),
            None => None,
        };

        let mut processor = ClockDriftProcessor::new(sent_at, state.managed_envelope.received_at())
            .at_least(MINIMUM_CLOCK_DRIFT);
        processor::process_value(&mut state.event, &mut processor, ProcessingState::root())
            .map_err(|_| ProcessingError::InvalidTransaction)?;

        // Log timestamp delays for all events after clock drift correction. This happens before
        // store processing, which could modify the timestamp if it exceeds a threshold. We are
        // interested in the actual delay before this correction.
        if let Some(timestamp) = state.event.value().and_then(|e| e.timestamp.value()) {
            let event_delay = state.managed_envelope.received_at() - timestamp.into_inner();
            if event_delay > SignedDuration::minutes(1) {
                let category = state.event_category().unwrap_or(DataCategory::Unknown);
                metric!(
                    timer(RelayTimers::TimestampDelay) = event_delay.to_std().unwrap(),
                    category = category.name(),
                );
            }
        }

        Ok(())
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

        if let Some(dsc) = DynamicSamplingContext::from_transaction(key_config.public_key, event) {
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
                state.managed_envelope.reject(Outcome::Filtered(err));
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
                        has_profile: state.has_profile,
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
    fn extract_spans(&self, state: &mut ProcessEnvelopeState) {
        // For now, drop any spans submitted by the SDK.

        use relay_event_schema::protocol::Span;
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

        // Extract transaction as a span.
        let Some(event) = state.event.value() else {
            return;
        };
        let transaction_span: Span = event.into();

        let mut add_span = |span: Annotated<Span>| {
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

        let all_modules_enabled = state
            .project_state
            .has_feature(Feature::SpanMetricsExtractionAllModules);
        let ga_modules_enabled = state
            .project_state
            .has_feature(Feature::SpanMetricsExtractionGAModules);

        // Add child spans as envelope items.
        if let Some(child_spans) = event.spans.value() {
            for span in child_spans {
                if let Some(inner_span) = span.value() {
                    // HACK: filter spans based on module until we figure out grouping.
                    let Some(span_op) = inner_span.op.value() else {
                        continue;
                    };
                    if all_modules_enabled || ga_modules_enabled && span_op.starts_with("db") {
                        // HACK: clone the span to set the segment_id. This should happen
                        // as part of normalization once standalone spans reach wider adoption.
                        let mut new_span = inner_span.clone();
                        new_span.segment_id = transaction_span.segment_id.clone();
                        new_span.is_segment = Annotated::new(false);
                        add_span(Annotated::new(new_span));
                    }
                }
            }
        }

        // Add transaction span as an envelope item.
        // HACK: temporarily omit this span.
        //add_span(transaction_span.into());
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
                if let Some(ErrorBoundary::Ok(config)) =
                    &state.project_state.config.transaction_metrics
                {
                    if config.is_enabled() {
                        self.compute_sampling_decision(state);
                    }
                }
            }

            _ => {}
        }
    }

    /// Computes the sampling decision on the incoming transaction.
    fn compute_sampling_decision(&self, state: &mut ProcessEnvelopeState) {
        state.sampling_result = utils::get_sampling_result(
            self.inner.config.processing_enabled(),
            Some(&state.project_state),
            state.sampling_project_state.as_deref(),
            state.envelope().dsc(),
            state.event.value(),
        );
    }

    /// Runs dynamic sampling on an incoming error and tags it in case of successful sampling
    /// decision.
    ///
    /// This execution of dynamic sampling is technically a "simulation" since we will use the result
    /// only for tagging errors and not for actually sampling incoming events.
    fn tag_error_with_sampling_decision(&self, state: &mut ProcessEnvelopeState) {
        if state.event.is_empty() {
            return;
        }

        let sampled = utils::is_trace_fully_sampled(
            self.inner.config.processing_enabled(),
            state.sampling_project_state.as_deref(),
            state.envelope().dsc(),
        );

        let (Some(event), Some(sampled)) = (state.event.value_mut(), sampled) else {
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
        match std::mem::take(&mut state.sampling_result) {
            // We assume that sampling is only supposed to work on transactions.
            SamplingResult::Drop(rule_ids)
                if state.event_type() == Some(EventType::Transaction) =>
            {
                state
                    .managed_envelope
                    .reject(Outcome::FilteredSampling(rule_ids.clone()));

                Err(ProcessingError::Sampled(rule_ids))
            }
            _ => Ok(()),
        }
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

        utils::log_transaction_name_metrics(&mut state.event, |event| {
            let config = LightNormalizationConfig {
                client_ip: client_ipaddr.as_ref(),
                user_agent: RawUserAgentInfo {
                    user_agent: request_meta.user_agent(),
                    client_hints: request_meta.client_hints().as_deref(),
                },
                received_at: Some(state.managed_envelope.received_at()),
                max_secs_in_past: Some(self.inner.config.max_secs_in_past()),
                max_secs_in_future: Some(self.inner.config.max_secs_in_future()),
                transaction_range: Some(
                    self.inner
                        .config
                        .aggregator_config_for(MetricNamespace::Transactions)
                        .timestamp_range(),
                ),
                max_name_and_unit_len: Some(
                    self.inner
                        .config
                        .aggregator_config()
                        .max_name_length
                        .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),
                ),
                breakdowns_config: state.project_state.config.breakdowns_v2.as_ref(),
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
                    self.global_config.measurements.as_ref(),
                )),
            };

            metric!(timer(RelayTimers::EventProcessingLightNormalization), {
                relay_event_normalization::light_normalize_event(event, config)
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

        self.process_sessions(state);
        self.process_client_reports(state);
        self.process_user_reports(state);
        self.process_replays(state)?;
        self.filter_profiles(state);

        if state.creates_event() {
            // Some envelopes only create events in processing relays; for example, unreal events.
            // This makes it possible to get in this code block while not really having an event in
            // the envelope.

            if_processing!({
                self.expand_unreal(state)?;
            });

            self.extract_event(state)?;

            if_processing!({
                self.process_unreal(state)?;
                self.create_placeholders(state);
            });

            self.finalize_event(state)?;
            self.light_normalize_event(state)?;
            self.normalize_dsc(state);
            self.filter_event(state)?;
            self.run_dynamic_sampling(state);
            self.extract_metrics(state)?;
            self.sample_envelope(state)?;

            if_processing!({
                self.store_process_event(state)?;
            });
        }

        if_processing!({
            self.enforce_quotas(state)?;
            // We need the event parsed in order to set the profile context on it
            self.process_profiles(state);
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

    /// Check and apply rate limits to metrics buckets.
    #[cfg(feature = "processing")]
    fn handle_rate_limit_flush_buckets(&self, message: RateLimitFlushBuckets) {
        use relay_quotas::ItemScoping;

        let RateLimitFlushBuckets {
            mut bucket_limiter,
            partition_key,
        } = message;

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

        let buckets = bucket_limiter.into_metrics();
        if !buckets.is_empty() {
            // Forward buckets to envelope manager to send them to upstream or kafka:
            self.inner.envelope_manager.send(SendMetrics {
                buckets,
                scoping,
                partition_key,
            });
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

    fn handle_message(&self, message: EnvelopeProcessor) {
        match message {
            EnvelopeProcessor::ProcessEnvelope(message) => self.handle_process_envelope(*message),
            EnvelopeProcessor::ProcessMetrics(message) => self.handle_process_metrics(*message),
            EnvelopeProcessor::EncodeEnvelope(message) => self.handle_encode_envelope(*message),
            #[cfg(feature = "processing")]
            EnvelopeProcessor::RateLimitFlushBuckets(message) => {
                self.handle_rate_limit_flush_buckets(message);
            }
        }
    }
}

impl Service for EnvelopeProcessorService {
    type Interface = EnvelopeProcessor;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let thread_count = self.inner.config.cpu_concurrency();
        relay_log::info!("starting {thread_count} envelope processing workers");

        tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::new(thread_count));

            let Ok(mut subscription) = self.inner.global_config.send(Subscribe).await else {
                // TODO(iker): we accept this sub-optimal error handling. TBD
                // the approach to deal with failures on the subscription
                // mechanism.
                relay_log::error!("failed to subscribe to GlobalConfigService");
                return;
            };

            // In case we use static global config, the watch wont be updated repeatedly, so we
            // should immediatly use the content of the watch.
            self.global_config = subscription.borrow().clone();

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

                    Ok(()) = subscription.changed() => self.global_config = subscription.borrow().clone(),
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::str::FromStr;

    use chrono::{DateTime, TimeZone, Utc};
    use relay_base_schema::metrics::{DurationUnit, MetricUnit};
    use relay_common::glob2::LazyGlob;
    use relay_event_normalization::{MeasurementsConfig, RedactionRule, TransactionNameRule};
    use relay_event_schema::protocol::{EventId, TransactionSource};
    use relay_pii::DataScrubbingConfig;
    use relay_sampling::condition::RuleCondition;
    use relay_sampling::config::{
        RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule, SamplingValue,
    };
    use relay_test::mock_service;
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use crate::actors::test_store::TestStore;
    use crate::extractors::RequestMeta;
    use crate::metrics_extraction::transactions::types::{
        CommonTags, TransactionMeasurementTags, TransactionMetric,
    };
    use crate::metrics_extraction::IntoMetric;
    use crate::testutils::{new_envelope, state_with_rule_and_condition};
    use crate::utils::Semaphore as TestSemaphore;

    use super::*;

    struct TestProcessSessionArguments<'a> {
        item: Item,
        received: DateTime<Utc>,
        client: Option<&'a str>,
        client_addr: Option<net::IpAddr>,
        metrics_config: SessionMetricsConfig,
        clock_drift_processor: ClockDriftProcessor,
        extracted_metrics: Vec<Bucket>,
    }

    impl<'a> TestProcessSessionArguments<'a> {
        fn run_session_producer(&mut self) -> bool {
            let proc = create_test_processor(Default::default());
            proc.process_session(
                &mut self.item,
                self.received,
                self.client,
                self.client_addr,
                self.metrics_config,
                &self.clock_drift_processor,
                &mut self.extracted_metrics,
            )
        }

        fn default() -> Self {
            let mut item = Item::new(ItemType::Event);

            let session = r#"{
            "init": false,
            "started": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "attrs": {
                "release": "1.0.0"
            },
            "did": "user123",
            "status": "this is not a valid status!",
            "duration": 123.4
        }"#;

            item.set_payload(ContentType::Json, session);
            let received = DateTime::from_str("2021-04-26T08:00:00+0100").unwrap();

            Self {
                item,
                received,
                client: None,
                client_addr: None,
                metrics_config: serde_json::from_str(
                    "
        {
            \"version\": 0,
            \"drop\": true
        }",
                )
                .unwrap(),
                clock_drift_processor: ClockDriftProcessor::new(None, received),
                extracted_metrics: vec![],
            }
        }
    }

    /// Checks that the default test-arguments leads to the item being kept, which helps ensure the
    /// other tests are valid.
    #[tokio::test]
    async fn test_process_session_keep_item() {
        let mut args = TestProcessSessionArguments::default();
        assert!(args.run_session_producer());
    }

    #[tokio::test]
    async fn test_process_session_invalid_json() {
        let mut args = TestProcessSessionArguments::default();
        args.item
            .set_payload(ContentType::Json, "this isnt valid json");
        assert!(!args.run_session_producer());
    }

    #[tokio::test]
    async fn test_process_session_sequence_overflow() {
        let mut args = TestProcessSessionArguments::default();
        args.item.set_payload(
            ContentType::Json,
            r#"{
            "init": false,
            "started": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "seq": 18446744073709551615,
            "attrs": {
                "release": "1.0.0"
            },
            "did": "user123",
            "status": "this is not a valid status!",
            "duration": 123.4
        }"#,
        );
        assert!(!args.run_session_producer());
    }

    #[tokio::test]
    async fn test_process_session_invalid_timestamp() {
        let mut args = TestProcessSessionArguments::default();
        args.received = DateTime::from_str("2021-05-26T08:00:00+0100").unwrap();
        assert!(!args.run_session_producer());
    }

    #[tokio::test]
    async fn test_process_session_metrics_extracted() {
        let mut args = TestProcessSessionArguments::default();
        args.item.set_metrics_extracted(true);
        assert!(!args.run_session_producer());
    }

    fn create_breadcrumbs_item(breadcrumbs: &[(Option<DateTime<Utc>>, &str)]) -> Item {
        let mut data = Vec::new();

        for (date, message) in breadcrumbs {
            let mut breadcrumb = BTreeMap::new();
            breadcrumb.insert("message", (*message).to_string());
            if let Some(date) = date {
                breadcrumb.insert("timestamp", date.to_rfc3339());
            }

            rmp_serde::encode::write(&mut data, &breadcrumb).expect("write msgpack");
        }

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::MsgPack, data);
        item
    }

    fn breadcrumbs_from_event(event: &Annotated<Event>) -> &Vec<Annotated<Breadcrumb>> {
        event
            .value()
            .unwrap()
            .breadcrumbs
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()
    }

    fn services() -> (Addr<TrackOutcome>, Addr<TestStore>) {
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});
        (outcome_aggregator, test_store)
    }

    #[tokio::test]
    async fn test_dsc_respects_metrics_extracted() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = services();

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
                sampling_result: SamplingResult::Keep,
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
                has_profile: false,
                event_metrics_extracted: false,
            }
        };

        // None represents no TransactionMetricsConfig, DS will not be run
        let mut state = get_state(None);
        service.run_dynamic_sampling(&mut state);
        assert!(matches!(state.sampling_result, SamplingResult::Keep));

        // Current version is 1, so it won't run DS if it's outdated
        let mut state = get_state(Some(0));
        service.run_dynamic_sampling(&mut state);
        assert!(matches!(state.sampling_result, SamplingResult::Keep));

        // Dynamic sampling is run, as the transactionmetrics version is up to date.
        let mut state = get_state(Some(1));
        service.run_dynamic_sampling(&mut state);
        assert!(matches!(state.sampling_result, SamplingResult::Drop(_)));
    }

    #[tokio::test]
    async fn test_it_keeps_or_drops_transactions() {
        relay_test::setup();

        let (outcome_aggregator, test_store) = services();

        // an empty json still produces a valid config
        let json_config = serde_json::json!({});

        let config = Config::from_json_value(json_config).unwrap();

        let service = create_test_processor(config);

        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("testing".to_owned()),
            ..Event::default()
        };

        for (sample_rate, expected_result) in [
            (0.0, SamplingResult::Drop(MatchedRuleIds(vec![RuleId(1)]))),
            (1.0, SamplingResult::Keep),
        ] {
            let project_state = state_with_rule_and_condition(
                Some(sample_rate),
                RuleType::Transaction,
                RuleCondition::all(),
            );

            let mut state = ProcessEnvelopeState {
                event: Annotated::from(event.clone()),
                event_metrics_extracted: false,
                metrics: Default::default(),
                sample_rates: None,
                sampling_result: SamplingResult::Keep,
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
                has_profile: false,
            };

            // TODO: This does not test if the sampling decision is actually applied. This should be
            // refactored to send a proper Envelope in and call process_state to cover the full
            // pipeline.
            service.compute_sampling_decision(&mut state);
            assert_eq!(state.sampling_result, expected_result);
        }
    }

    #[test]
    fn test_breadcrumbs_file1() {
        let item = create_breadcrumbs_item(&[(None, "item1")]);

        // NOTE: using (Some, None) here:
        let result = EnvelopeProcessorService::event_from_attachments(
            &Config::default(),
            None,
            Some(item),
            None,
        );

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);

        assert_eq!(breadcrumbs.len(), 1);
        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("item1", first_breadcrumb_message);
    }

    #[test]
    fn test_breadcrumbs_file2() {
        let item = create_breadcrumbs_item(&[(None, "item2")]);

        // NOTE: using (None, Some) here:
        let result = EnvelopeProcessorService::event_from_attachments(
            &Config::default(),
            None,
            None,
            Some(item),
        );

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 1);

        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("item2", first_breadcrumb_message);
    }

    #[test]
    fn test_breadcrumbs_truncation() {
        let item1 = create_breadcrumbs_item(&[(None, "crumb1")]);
        let item2 = create_breadcrumbs_item(&[(None, "crumb2"), (None, "crumb3")]);

        let result = EnvelopeProcessorService::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        );

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);
    }

    #[test]
    fn test_breadcrumbs_order_with_none() {
        let d1 = Utc.with_ymd_and_hms(2019, 10, 10, 12, 10, 10).unwrap();
        let d2 = Utc.with_ymd_and_hms(2019, 10, 11, 12, 10, 10).unwrap();

        let item1 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);
        let item2 = create_breadcrumbs_item(&[(Some(d2), "d2")]);

        let result = EnvelopeProcessorService::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        );

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);

        assert_eq!(Some("d1"), breadcrumbs[0].value().unwrap().message.as_str());
        assert_eq!(Some("d2"), breadcrumbs[1].value().unwrap().message.as_str());
    }

    #[test]
    fn test_breadcrumbs_reversed_with_none() {
        let d1 = Utc.with_ymd_and_hms(2019, 10, 10, 12, 10, 10).unwrap();
        let d2 = Utc.with_ymd_and_hms(2019, 10, 11, 12, 10, 10).unwrap();

        let item1 = create_breadcrumbs_item(&[(Some(d2), "d2")]);
        let item2 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);

        let result = EnvelopeProcessorService::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        );

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);

        assert_eq!(Some("d1"), breadcrumbs[0].value().unwrap().message.as_str());
        assert_eq!(Some("d2"), breadcrumbs[1].value().unwrap().message.as_str());
    }

    #[test]
    fn test_empty_breadcrumbs_item() {
        let item1 = create_breadcrumbs_item(&[]);
        let item2 = create_breadcrumbs_item(&[]);
        let item3 = create_breadcrumbs_item(&[]);

        let result = EnvelopeProcessorService::event_from_attachments(
            &Config::default(),
            Some(item1),
            Some(item2),
            Some(item3),
        );

        // regression test to ensure we don't fail parsing an empty file
        result.expect("event_from_attachments");
    }

    fn create_test_processor(config: Config) -> EnvelopeProcessorService {
        let (envelope_manager, _) = mock_service("envelope_manager", (), |&mut (), _| {});
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
        let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});
        let (global_config, _) = mock_service("global_config", (), |&mut (), _| {});
        let inner = InnerProcessor {
            config: Arc::new(config),
            envelope_manager,
            project_cache,
            outcome_aggregator,
            upstream_relay,
            #[cfg(feature = "processing")]
            rate_limiter: None,
            geoip_lookup: None,
            global_config,
        };

        EnvelopeProcessorService {
            global_config: Arc::default(),
            inner: Arc::new(inner),
        }
    }

    #[tokio::test]
    async fn test_user_report_invalid() {
        let processor = create_test_processor(Default::default());
        let (outcome_aggregator, test_store) = services();
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::UserReport);
            item.set_payload(ContentType::Json, r#"{"foo": "bar"}"#);
            item
        });

        envelope.add_item({
            let mut item = Item::new(ItemType::Event);
            item.set_payload(ContentType::Json, "{}");
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let new_envelope = ctx.envelope();

        assert_eq!(new_envelope.len(), 1);
        assert_eq!(new_envelope.items().next().unwrap().ty(), &ItemType::Event);
    }

    fn process_envelope_with_root_project_state(
        envelope: Box<Envelope>,
        sampling_project_state: Option<Arc<ProjectState>>,
    ) -> Envelope {
        let processor = create_test_processor(Default::default());
        let (outcome_aggregator, test_store) = services();

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state,
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
            rules: vec![],
            rules_v2: vec![SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::SampleRate { value: sample_rate },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            mode: SamplingMode::Received,
        };
        let mut sampling_project_state = ProjectState::allowed();
        sampling_project_state.config.dynamic_sampling = Some(sampling_config);
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
        let (outcome_aggregator, test_store) = services();
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
    async fn test_client_report_removal() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = services();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": true,
                "emit_client_outcomes": true
            }
        }))
        .unwrap();

        let processor = create_test_processor(config);

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
        };

        let envelope_response = processor.process(message).unwrap();
        assert!(envelope_response.envelope.is_none());
    }

    #[tokio::test]
    async fn test_client_report_forwarding() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = services();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": false,
                // a relay need to emit outcomes at all to not process.
                "emit_client_outcomes": true
            }
        }))
        .unwrap();

        let processor = create_test_processor(config);

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let item = ctx.envelope().items().next().unwrap();
        assert_eq!(item.ty(), &ItemType::ClientReport);

        ctx.accept(); // do not try to capture or emit outcomes
    }

    #[tokio::test]
    #[cfg(feature = "processing")]
    async fn test_client_report_removal_in_processing() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = services();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": true,
                "emit_client_outcomes": false,
            },
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let processor = create_test_processor(config);

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, outcome_aggregator, test_store),
            project_state: Arc::new(ProjectState::allowed()),
            sampling_project_state: None,
        };

        let envelope_response = processor.process(message).unwrap();
        assert!(envelope_response.envelope.is_none());
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

    #[test]
    fn test_from_outcome_type_sampled() {
        assert!(outcome_from_parts(ClientReportField::FilteredSampling, "adsf").is_err());

        assert!(outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:").is_err());

        assert!(outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:foo").is_err());

        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:"),
            Err(())
        ));

        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:;"),
            Err(())
        ));

        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:ab;12"),
            Err(())
        ));

        assert_eq!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:123,456"),
            Ok(Outcome::FilteredSampling(MatchedRuleIds(vec![
                RuleId(123),
                RuleId(456),
            ])))
        );

        assert_eq!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:123"),
            Ok(Outcome::FilteredSampling(MatchedRuleIds(vec![RuleId(123)])))
        );
    }

    #[test]
    fn test_from_outcome_type_filtered() {
        assert!(matches!(
            outcome_from_parts(ClientReportField::Filtered, "error-message"),
            Ok(Outcome::Filtered(FilterStatKey::ErrorMessage))
        ));
        assert!(outcome_from_parts(ClientReportField::Filtered, "adsf").is_err());
    }

    #[test]
    fn test_from_outcome_type_client_discard() {
        assert_eq!(
            outcome_from_parts(ClientReportField::ClientDiscard, "foo_reason").unwrap(),
            Outcome::ClientDiscard("foo_reason".into())
        );
    }

    #[test]
    fn test_from_outcome_type_rate_limited() {
        assert!(matches!(
            outcome_from_parts(ClientReportField::RateLimited, ""),
            Ok(Outcome::RateLimited(None))
        ));
        assert_eq!(
            outcome_from_parts(ClientReportField::RateLimited, "foo_reason").unwrap(),
            Outcome::RateLimited(Some(ReasonCode::new("foo_reason")))
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
                let config = LightNormalizationConfig {
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
                relay_event_normalization::light_normalize_event(event, config)
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
}
