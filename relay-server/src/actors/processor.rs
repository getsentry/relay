use std::cmp::max;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::io::Write;
use std::net;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::SystemService;
use brotli2::write::BrotliEncoder;
use chrono::{DateTime, Duration as SignedDuration, Utc};
use failure::Fail;
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use once_cell::sync::OnceCell;
use serde_json::Value as SerdeValue;
use tokio::sync::Semaphore;

use relay_auth::RelayVersion;
use relay_common::{ProjectId, ProjectKey, UnixTimestamp};
use relay_config::{Config, HttpEncoding};
use relay_filter::FilterStatKey;
use relay_general::pii::PiiConfigError;
use relay_general::pii::{PiiAttachmentsProcessor, PiiProcessor};
use relay_general::processor::{process_value, ProcessingState};
use relay_general::protocol::{
    self, Breadcrumb, ClientReport, Csp, Event, EventType, ExpectCt, ExpectStaple, Hpkp, IpAddr,
    LenientString, Metrics, RelayInfo, SecurityReportType, SessionAggregates, SessionAttributes,
    SessionUpdate, Timestamp, UserReport, Values,
};
use relay_general::store::{ClockDriftProcessor, LightNormalizationConfig};
use relay_general::types::{Annotated, Array, FromValue, Object, ProcessingAction, Value};
use relay_log::LogError;
use relay_metrics::{Bucket, InsertMetrics, MergeBuckets, Metric};
use relay_quotas::{DataCategory, ReasonCode};
use relay_redis::RedisPool;
use relay_sampling::{DynamicSamplingContext, RuleId};
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse, Service};

use crate::actors::envelopes::{EnvelopeManager, SendEnvelope, SendEnvelopeError, SubmitEnvelope};
use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::project::{Feature, ProjectState};
use crate::actors::project_cache::ProjectCache;
use crate::actors::upstream::{SendRequest, UpstreamRelay};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::metrics_extraction::sessions::{extract_session_metrics, SessionMetricsConfig};
use crate::metrics_extraction::transactions::extract_transaction_metrics;
use crate::service::{ServerError, REGISTRY};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{
    self, ChunkedFormDataAggregator, EnvelopeContext, ErrorBoundary, FormDataIter, SamplingResult,
};

#[cfg(feature = "processing")]
use {
    crate::actors::project_cache::UpdateRateLimits,
    crate::service::ServerErrorKind,
    crate::utils::EnvelopeLimiter,
    failure::ResultExt,
    relay_general::store::{GeoIpLookup, StoreConfig, StoreProcessor},
    relay_quotas::{RateLimitingError, RedisRateLimiter},
    symbolic_unreal::{Unreal4Error, Unreal4ErrorKind},
};

/// The minimum clock drift for correction to apply.
const MINIMUM_CLOCK_DRIFT: Duration = Duration::from_secs(55 * 60);

/// An error returned when handling [`ProcessEnvelope`].
#[derive(Debug, Fail)]
pub enum ProcessingError {
    #[fail(display = "invalid json in event")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "invalid message pack event payload")]
    InvalidMsgpack(#[cause] rmp_serde::decode::Error),

    #[cfg(feature = "processing")]
    #[fail(display = "invalid unreal crash report")]
    InvalidUnrealReport(#[cause] Unreal4Error),

    #[fail(display = "event payload too large")]
    PayloadTooLarge,

    #[fail(display = "invalid transaction event")]
    InvalidTransaction,

    #[fail(display = "envelope processor failed")]
    ProcessingFailed(#[cause] ProcessingAction),

    #[fail(display = "duplicate {} in event", _0)]
    DuplicateItem(ItemType),

    #[fail(display = "failed to extract event payload")]
    NoEventPayload,

    #[fail(display = "missing project id in DSN")]
    MissingProjectId,

    #[fail(display = "invalid security report type")]
    InvalidSecurityType,

    #[fail(display = "invalid security report")]
    InvalidSecurityReport(#[cause] serde_json::Error),

    #[fail(display = "event filtered with reason: {:?}", _0)]
    EventFiltered(FilterStatKey),

    #[fail(display = "could not serialize event payload")]
    SerializeFailed(#[cause] serde_json::Error),

    #[cfg(feature = "processing")]
    #[fail(display = "failed to apply quotas")]
    QuotasFailed(#[cause] RateLimitingError),

    #[fail(display = "event dropped by sampling rule {}", _0)]
    Sampled(RuleId),

    #[fail(display = "invalid pii config")]
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

    fn is_internal(&self) -> bool {
        self.to_outcome() == Some(Outcome::Invalid(DiscardReason::Internal))
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

/// A state container for envelope processing.
#[derive(Debug)]
struct ProcessEnvelopeState {
    /// The envelope.
    ///
    /// The pipeline can mutate the envelope and remove or add items. In particular, event items are
    /// removed at the beginning of processing and re-added in the end.
    envelope: Envelope,

    /// The extracted event payload.
    ///
    /// For Envelopes without event payloads, this contains `Annotated::empty`. If a single item has
    /// `creates_event`, the event is required and the pipeline errors if no payload can be
    /// extracted.
    event: Annotated<Event>,

    /// Track whether transaction metrics were already extracted.
    transaction_metrics_extracted: bool,

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
    extracted_metrics: Vec<Metric>,

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

    /// The envelope context before processing.
    envelope_context: EnvelopeContext,
}

impl ProcessEnvelopeState {
    /// Returns whether any item in the envelope creates an event in any relay.
    ///
    /// This is used to branch into the processing pipeline. If this function returns false, only
    /// rate limits are executed. If this function returns true, an event is created either in the
    /// current relay or in an upstream processing relay.
    fn creates_event(&self) -> bool {
        self.envelope.items().any(Item::creates_event)
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
            Some(rule_id) => rule_id
                .parse()
                .map(|id| Outcome::FilteredSampling(RuleId(id)))
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

fn outcome_from_profile_error(err: relay_profiling::ProfileError) -> Outcome {
    let discard_reason = match err {
        relay_profiling::ProfileError::CannotSerializePayload => DiscardReason::Internal,
        relay_profiling::ProfileError::NotEnoughSamples => DiscardReason::InvalidProfile,
        _ => DiscardReason::ProcessProfile,
    };
    Outcome::Invalid(discard_reason)
}

fn track_sampling_metrics(
    project_state: &ProjectState,
    context: &DynamicSamplingContext,
    event: &Event,
) {
    // We only collect this metric for the root transaction event, so ignore secondary projects.
    if !project_state.is_matching_key(context.public_key) {
        return;
    }

    let transaction_info = match event.transaction_info.value() {
        Some(info) => info,
        None => return,
    };

    let changes = match transaction_info.changes.value() {
        Some(value) => value.as_slice(),
        None => return,
    };

    let last_change = changes
        .iter()
        .rev()
        // skip all broken change records
        .filter_map(|a| a.value())
        // skip records without a timestamp
        .filter(|c| c.timestamp.value().is_some())
        // take the last that did not occur when the event was sent
        .find(|c| c.timestamp.value() != event.timestamp.value());

    let source = event.get_transaction_source().as_str();
    let platform = event.platform.as_str().unwrap_or("other");
    let sdk_name = event.sdk_name();
    let sdk_version = event.sdk_version();

    metric!(
        histogram(RelayHistograms::DynamicSamplingChanges) = changes.len() as u64,
        source = source,
        platform = platform,
        sdk_name = sdk_name,
        sdk_version = sdk_version,
    );

    if let Some(&total) = transaction_info.propagations.value() {
        // If there was no change, there were no propagations that happened with a wrong name.
        let change = last_change
            .and_then(|c| c.propagations.value())
            .map_or(0, |v| *v);

        metric!(
            histogram(RelayHistograms::DynamicSamplingPropagationCount) = change,
            source = source,
            platform = platform,
            sdk_name = sdk_name,
            sdk_version = sdk_version,
        );

        let percentage = match (change, total) {
            (0, 0) => 0.0, // 0% indicates no premature changes.
            _ => ((change as f64) / (total as f64)).min(1.0) * 100.0,
        };

        metric!(
            histogram(RelayHistograms::DynamicSamplingPropagationPercentage) = percentage,
            source = source,
            platform = platform,
            sdk_name = sdk_name,
            sdk_version = sdk_version,
        );
    }

    if let (Some(&start), Some(&change), Some(&end)) = (
        event.start_timestamp.value(),
        last_change
            .and_then(|c| c.timestamp.value())
            .or_else(|| event.start_timestamp.value()), // default to start if there was no change
        event.timestamp.value(),
    ) {
        let delay_ms = (change - start).num_milliseconds();
        if delay_ms >= 0 {
            metric!(
                histogram(RelayHistograms::DynamicSamplingChangeDuration) = delay_ms as u64,
                source = source,
                platform = platform,
                sdk_name = sdk_name,
                sdk_version = sdk_version,
            );
        }

        let duration_ms = (end - start).num_milliseconds() as f64;
        if delay_ms >= 0 && duration_ms >= 0.0 {
            let percentage = ((delay_ms as f64) / duration_ms).min(1.0) * 100.0;
            metric!(
                histogram(RelayHistograms::DynamicSamplingChangePercentage) = percentage,
                source = source,
                platform = platform,
                sdk_name = sdk_name,
                sdk_version = sdk_version,
            );
        }
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
    pub envelope: Option<(Envelope, EnvelopeContext)>,
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
    pub envelope: Envelope,
    pub envelope_context: EnvelopeContext,
    pub project_state: Arc<ProjectState>,
    pub sampling_project_state: Option<Arc<ProjectState>>,
}

/// Parses a list of metrics or metric buckets and pushes them to the project's aggregator.
///
/// This parses and validates the metrics:
///  - For [`Metrics`](ItemType::Metrics), each metric is parsed separately, and invalid metrics are
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
/// This message is a workaround for a single-threaded upstream actor.
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

/// CPU-intensive processing tasks for envelopes.
#[derive(Debug)]
pub enum EnvelopeProcessor {
    ProcessEnvelope(Box<ProcessEnvelope>),
    ProcessMetrics(Box<ProcessMetrics>),
    EncodeEnvelope(Box<EncodeEnvelope>),
}

impl EnvelopeProcessor {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().processor.clone()
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

impl FromMessage<EncodeEnvelope> for EnvelopeProcessor {
    type Response = NoResponse;

    fn from_message(message: EncodeEnvelope, _: ()) -> Self {
        Self::EncodeEnvelope(Box::new(message))
    }
}

/// Service implementing the [`EnvelopeProcessor`] interface.
///
/// This service handles messages in a worker pool with configurable concurrency.
pub struct EnvelopeProcessorService {
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    rate_limiter: Option<RedisRateLimiter>,
    #[cfg(feature = "processing")]
    geoip_lookup: Option<GeoIpLookup>,
}

impl EnvelopeProcessorService {
    /// Creates a multi-threaded envelope processor.
    pub fn new(config: Arc<Config>, _redis: Option<RedisPool>) -> Result<Self, ServerError> {
        #[cfg(feature = "processing")]
        {
            let geoip_lookup = match config.geoip_path() {
                Some(p) => Some(GeoIpLookup::open(p).context(ServerErrorKind::GeoIpError)?),
                None => None,
            };

            let rate_limiter =
                _redis.map(|pool| RedisRateLimiter::new(pool).max_limit(config.max_rate_limit()));

            Ok(Self {
                config,
                rate_limiter,
                geoip_lookup,
            })
        }

        #[cfg(not(feature = "processing"))]
        Ok(Self { config })
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
        if let Err(e) = protocol::validate_release(release) {
            relay_log::trace!("skipping session with invalid release '{}': {}", release, e);
            return Err(());
        }

        if let Some(ref env) = attributes.environment {
            if let Err(e) = protocol::validate_environment(env) {
                relay_log::trace!("removing invalid environment '{}': {}", env, e);
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
        let max_age = SignedDuration::seconds(self.config.max_session_secs_in_past());
        if (received - timestamp) > max_age {
            relay_log::trace!("skipping session older than {} days", max_age.num_days());
            return false;
        }

        let max_future = SignedDuration::seconds(self.config.max_secs_in_future());
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
        extracted_metrics: &mut Vec<Metric>,
    ) -> bool {
        let mut changed = false;
        let payload = item.payload();

        let mut session = match SessionUpdate::parse(&payload) {
            Ok(session) => session,
            Err(error) => {
                relay_log::trace!("skipping invalid session payload: {}", LogError(&error));
                return false;
            }
        };

        if session.sequence == u64::MAX {
            relay_log::trace!("skipping session due to sequence overflow");
            return false;
        }

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

        // Extract metrics if they haven't been extracted by a prior Relay
        if metrics_config.is_enabled() && !item.metrics_extracted() {
            extract_session_metrics(&session.attributes, &session, client, extracted_metrics);
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
                    relay_log::error!("failed to serialize session: {}", LogError(&err));
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
        extracted_metrics: &mut Vec<Metric>,
    ) -> bool {
        let mut changed = false;
        let payload = item.payload();

        let mut session = match SessionAggregates::parse(&payload) {
            Ok(session) => session,
            Err(error) => {
                relay_log::trace!("skipping invalid sessions payload: {}", LogError(&error));
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
                extract_session_metrics(&session.attributes, aggregate, client, extracted_metrics);
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
                    relay_log::error!("failed to serialize session: {}", LogError(&err));
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
        let received = state.envelope_context.received_at();
        let extracted_metrics = &mut state.extracted_metrics;
        let metrics_config = state.project_state.config().session_metrics;
        let envelope = &mut state.envelope;
        let client = envelope.meta().client().map(|x| x.to_owned());
        let client_addr = envelope.meta().client_addr();

        let clock_drift_processor =
            ClockDriftProcessor::new(envelope.sent_at(), received).at_least(MINIMUM_CLOCK_DRIFT);

        envelope.retain_items(|item| {
            match item.ty() {
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
            }
        });
    }

    /// Validates and normalizes all user report items in the envelope.
    ///
    /// User feedback items are removed from the envelope if they contain invalid JSON or if the
    /// JSON violates the schema (basic type validation). Otherwise, their normalized representation
    /// is written back into the item.
    fn process_user_reports(&self, state: &mut ProcessEnvelopeState) {
        state.envelope.retain_items(|item| {
            if item.ty() != &ItemType::UserReport {
                return true;
            };

            let report = match serde_json::from_slice::<UserReport>(&item.payload()) {
                Ok(session) => session,
                Err(error) => {
                    relay_log::error!("failed to store user report: {}", LogError(&error));
                    return false;
                }
            };

            let json_string = match serde_json::to_string(&report) {
                Ok(json) => json,
                Err(err) => {
                    relay_log::error!("failed to serialize user report: {}", LogError(&err));
                    return false;
                }
            };

            item.set_payload(ContentType::Json, json_string);
            true
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
        if !self.config.emit_outcomes().any() || !self.config.emit_client_outcomes() {
            // if a processing relay has client outcomes disabled we drop them.
            if self.config.processing_enabled() {
                state
                    .envelope
                    .retain_items(|item| item.ty() != &ItemType::ClientReport);
            }
            return;
        }

        let mut timestamp = None;
        let mut output_events = BTreeMap::new();
        let received = state.envelope_context.received_at();

        let clock_drift_processor = ClockDriftProcessor::new(state.envelope.sent_at(), received)
            .at_least(MINIMUM_CLOCK_DRIFT);

        // we're going through all client reports but we're effectively just merging
        // them into the first one.
        state.envelope.retain_items(|item| {
            if item.ty() != &ItemType::ClientReport {
                return true;
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
                Err(err) => relay_log::trace!("invalid client report received: {}", LogError(&err)),
            }
            false
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

        let max_age = SignedDuration::seconds(self.config.max_secs_in_past());
        if (received - timestamp.as_datetime()) > max_age {
            relay_log::trace!(
                "skipping client outcomes older than {} days",
                max_age.num_days()
            );
            return;
        }

        let max_future = SignedDuration::seconds(self.config.max_secs_in_future());
        if (timestamp.as_datetime() - received) > max_future {
            relay_log::trace!(
                "skipping client outcomes more than {}s in the future",
                max_future.num_seconds()
            );
            return;
        }

        let producer = TrackOutcome::from_registry();
        for ((outcome_type, reason, category), quantity) in output_events.into_iter() {
            let outcome = match outcome_from_parts(outcome_type, &reason) {
                Ok(outcome) => outcome,
                Err(_) => {
                    relay_log::trace!(
                        "Invalid outcome_type / reason: ({:?}, {})",
                        outcome_type,
                        reason
                    );
                    continue;
                }
            };

            producer.send(TrackOutcome {
                timestamp: timestamp.as_datetime(),
                scoping: state.envelope_context.scoping(),
                outcome,
                event_id: None,
                remote_addr: None, // omitting the client address allows for better aggregation
                category,
                quantity,
            });
        }
    }

    /// Remove profiles if the feature flag is not enabled
    fn process_profiles(&self, state: &mut ProcessEnvelopeState) {
        let profiling_enabled = state.project_state.has_feature(Feature::Profiling);
        let envelope = &mut state.envelope;

        envelope.retain_items(|item| match item.ty() {
            ItemType::Profile => profiling_enabled,
            _ => true,
        });

        if !self.config.processing_enabled() {
            return;
        }

        let context = &state.envelope_context;
        let mut new_profiles = Vec::new();

        while let Some(item) = envelope.take_item_by(|item| item.ty() == &ItemType::Profile) {
            match relay_profiling::expand_profile(&item.payload()[..]) {
                Ok(payloads) => new_profiles.extend(payloads),
                Err(err) => {
                    relay_log::debug!("invalid profile: {:#?}", err);
                    context.track_outcome(
                        outcome_from_profile_error(err),
                        DataCategory::Profile,
                        1,
                    );
                }
            }
        }

        for payload in new_profiles {
            let mut item = Item::new(ItemType::Profile);
            item.set_payload(ContentType::Json, &payload[..]);
            envelope.add_item(item);
        }
    }

    /// Remove replays if the feature flag is not enabled
    fn process_replays(&self, state: &mut ProcessEnvelopeState) {
        let replays_enabled = state.project_state.has_feature(Feature::Replays);
        let context = &state.envelope_context;
        let envelope = &mut state.envelope;
        let client_addr = envelope.meta().client_addr();

        state.envelope.retain_items(|item| match item.ty() {
            ItemType::ReplayEvent => {
                if !replays_enabled {
                    return false;
                }

                let parsed_replay =
                    relay_replays::normalize_replay_event(&item.payload(), client_addr);
                match parsed_replay {
                    Ok(replay) => {
                        item.set_payload(ContentType::Json, &replay[..]);
                        true
                    }
                    Err(_) => {
                        context.track_outcome(
                            Outcome::Invalid(DiscardReason::InvalidReplayEvent),
                            DataCategory::Replay,
                            1,
                        );
                        false
                    }
                }
            }
            ItemType::ReplayRecording => replays_enabled,
            _ => true,
        });
    }

    /// Creates and initializes the processing state.
    ///
    /// This applies defaults to the envelope and initializes empty rate limits.
    fn prepare_state(
        &self,
        message: ProcessEnvelope,
    ) -> Result<ProcessEnvelopeState, ProcessingError> {
        let ProcessEnvelope {
            mut envelope,
            mut envelope_context,
            project_state,
            sampling_project_state,
        } = message;

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
                envelope_context.reject(Outcome::Invalid(DiscardReason::Internal));
                return Err(ProcessingError::MissingProjectId);
            }
        };

        // Ensure the project ID is updated to the stored instance for this project cache. This can
        // differ in two cases:
        //  1. The envelope was sent to the legacy `/store/` endpoint without a project ID.
        //  2. The DSN was moved and the envelope sent to the old project ID.
        envelope.meta_mut().set_project_id(project_id);

        Ok(ProcessEnvelopeState {
            envelope,
            event: Annotated::empty(),
            transaction_metrics_extracted: false,
            metrics: Metrics::default(),
            sample_rates: None,
            extracted_metrics: Vec::new(),
            project_state,
            sampling_project_state,
            project_id,
            envelope_context,
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
        let envelope = &mut state.envelope;

        if let Some(item) = envelope.take_item_by(|item| item.ty() == &ItemType::UnrealReport) {
            utils::expand_unreal_envelope(item, envelope, &self.config)?;
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

    fn event_from_security_report(&self, item: Item) -> Result<ExtractedEvent, ProcessingError> {
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
            let breadcrumb = Annotated::deserialize_with_meta(&mut deserializer)
                .map_err(ProcessingError::InvalidMsgpack)?;
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
            ItemType::UnrealReport => self.config.processing_enabled(),

            // These may be forwarded to upstream / store:
            ItemType::Attachment => false,
            ItemType::UserReport => false,

            // Aggregate data is never considered as part of deduplication
            ItemType::Session => false,
            ItemType::Sessions => false,
            ItemType::Metrics => false,
            ItemType::MetricBuckets => false,
            ItemType::ClientReport => false,
            ItemType::Profile => false,
            ItemType::ReplayEvent => false,
            ItemType::ReplayRecording => false,
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
        let envelope = &mut state.envelope;

        // Remove all items first, and then process them. After this function returns, only
        // attachments can remain in the envelope. The event will be added again at the end of
        // `process_event`.
        let event_item = envelope.take_item_by(|item| item.ty() == &ItemType::Event);
        let transaction_item = envelope.take_item_by(|item| item.ty() == &ItemType::Transaction);
        let security_item = envelope.take_item_by(|item| item.ty() == &ItemType::Security);
        let raw_security_item = envelope.take_item_by(|item| item.ty() == &ItemType::RawSecurity);
        let form_item = envelope.take_item_by(|item| item.ty() == &ItemType::FormData);
        let attachment_item = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::EventPayload));
        let breadcrumbs1 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));
        let breadcrumbs2 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));

        // Event items can never occur twice in an envelope.
        if let Some(duplicate) = envelope.get_item_by(|item| self.is_duplicate(item)) {
            return Err(ProcessingError::DuplicateItem(duplicate.ty().clone()));
        }

        let (event, event_len) = if let Some(mut item) = event_item.or(security_item) {
            relay_log::trace!("processing json event");
            state.sample_rates = item.take_sample_rates();
            metric!(timer(RelayTimers::EventProcessingDeserialize), {
                // Event items can never include transactions, so retain the event type and let
                // inference deal with this during store normalization.
                self.event_from_json_payload(item, None)?
            })
        } else if let Some(mut item) = transaction_item {
            relay_log::trace!("processing json transaction");
            state.sample_rates = item.take_sample_rates();
            state.transaction_metrics_extracted = item.metrics_extracted();
            metric!(timer(RelayTimers::EventProcessingDeserialize), {
                // Transaction items can only contain transaction events. Force the event type to
                // hint to normalization that we're dealing with a transaction now.
                self.event_from_json_payload(item, Some(EventType::Transaction))?
            })
        } else if let Some(mut item) = raw_security_item {
            relay_log::trace!("processing security report");
            state.sample_rates = item.take_sample_rates();
            self.event_from_security_report(item).map_err(|error| {
                relay_log::error!("failed to extract security report: {}", LogError(&error));
                error
            })?
        } else if attachment_item.is_some() || breadcrumbs1.is_some() || breadcrumbs2.is_some() {
            relay_log::trace!("extracting attached event data");
            Self::event_from_attachments(&self.config, attachment_item, breadcrumbs1, breadcrumbs2)?
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
        state.metrics.bytes_ingested_event = Annotated::new(event_len as u64);

        Ok(())
    }

    /// Extracts event information from an unreal context.
    ///
    /// If the event does not contain an unreal context, this function does not perform any action.
    /// If there was no event payload prior to this function, it is created.
    #[cfg(feature = "processing")]
    fn process_unreal(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        utils::process_unreal_envelope(&mut state.event, &mut state.envelope)
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
        let envelope = &mut state.envelope;

        let minidump_attachment =
            envelope.get_item_by(|item| item.attachment_type() == Some(AttachmentType::Minidump));
        let apple_crash_report_attachment = envelope
            .get_item_by(|item| item.attachment_type() == Some(AttachmentType::AppleCrashReport));

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
        let envelope = &mut state.envelope;

        let event = match state.event.value_mut() {
            Some(event) => event,
            None if !self.config.processing_enabled() => return Ok(()),
            None => return Err(ProcessingError::NoEventPayload),
        };

        if !self.config.processing_enabled() {
            static MY_VERSION_STRING: OnceCell<String> = OnceCell::new();
            let my_version = MY_VERSION_STRING.get_or_init(|| RelayVersion::current().to_string());

            event
                .ingest_path
                .get_or_insert_with(Default::default)
                .push(Annotated::new(RelayInfo {
                    version: Annotated::new(my_version.clone()),
                    public_key: self
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
        if self.config.processing_enabled() {
            let mut metrics = std::mem::take(&mut state.metrics);

            let attachment_size = envelope
                .items()
                .filter(|item| item.attachment_type() == Some(AttachmentType::Attachment))
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
                let source = event.get_transaction_source();

                metric!(
                    counter(RelayCounters::EventTransactionSource) += 1,
                    source = &source.to_string(),
                    sdk = envelope.meta().client_name().unwrap_or("proprietary"),
                    platform = event.platform.as_str().unwrap_or("other"),
                );
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

        let mut processor = ClockDriftProcessor::new(sent_at, state.envelope_context.received_at())
            .at_least(MINIMUM_CLOCK_DRIFT);
        process_value(&mut state.event, &mut processor, ProcessingState::root())
            .map_err(|_| ProcessingError::InvalidTransaction)?;

        // Log timestamp delays for all events after clock drift correction. This happens before
        // store processing, which could modify the timestamp if it exceeds a threshold. We are
        // interested in the actual delay before this correction.
        if let Some(timestamp) = state.event.value().and_then(|e| e.timestamp.value()) {
            let event_delay = state.envelope_context.received_at() - timestamp.into_inner();
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
            ref envelope,
            ref mut event,
            ref project_state,
            ref envelope_context,
            ..
        } = *state;

        let key_id = project_state
            .get_public_key_config()
            .and_then(|k| Some(k.numeric_id?.to_string()));

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
            max_secs_in_future: Some(self.config.max_secs_in_future()),
            max_secs_in_past: Some(self.config.max_secs_in_past()),
            enable_trimming: Some(true),
            is_renormalize: Some(false),
            remove_other: Some(true),
            normalize_user_agent: Some(true),
            sent_at: envelope.sent_at(),
            received_at: Some(envelope_context.received_at()),
            breakdowns: project_state.config.breakdowns_v2.clone(),
            span_attributes: project_state.config.span_attributes.clone(),
            client_sample_rate: envelope.sampling_context().and_then(|ctx| ctx.sample_rate),
        };

        let mut store_processor = StoreProcessor::new(store_config, self.geoip_lookup.as_ref());
        metric!(timer(RelayTimers::EventProcessingProcess), {
            process_value(event, &mut store_processor, ProcessingState::root())
                .map_err(|_| ProcessingError::InvalidTransaction)?;
            if has_unprintable_fields(event) {
                metric!(counter(RelayCounters::EventCorrupted) += 1);
            }
        });

        Ok(())
    }

    fn filter_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let event = match state.event.value_mut() {
            Some(event) => event,
            // Some events are created by processing relays (e.g. unreal), so they do not yet
            // exist at this point in non-processing relays.
            None => return Ok(()),
        };

        let client_ip = state.envelope.meta().client_addr();
        let filter_settings = &state.project_state.config.filter_settings;

        metric!(timer(RelayTimers::EventProcessingFiltering), {
            relay_filter::should_filter(event, client_ip, filter_settings).map_err(|err| {
                state.envelope_context.reject(Outcome::Filtered(err));
                ProcessingError::EventFiltered(err)
            })
        })
    }

    #[cfg(feature = "processing")]
    fn enforce_quotas(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let rate_limiter = match self.rate_limiter.as_ref() {
            Some(rate_limiter) => rate_limiter,
            None => return Ok(()),
        };

        let project_state = &state.project_state;
        let quotas = project_state.config.quotas.as_slice();
        if quotas.is_empty() {
            return Ok(());
        }

        let mut remove_event = false;
        let event_category = state.event_category();

        // When invoking the rate limiter, capture if the event item has been rate limited to also
        // remove it from the processing state eventually.
        let mut envelope_limiter = EnvelopeLimiter::new(|item_scope, quantity| {
            let limits = rate_limiter.is_rate_limited(quotas, item_scope, quantity)?;
            remove_event |= Some(item_scope.category) == event_category && limits.is_limited();
            Ok(limits)
        });

        // Tell the envelope limiter about the event, since it has been removed from the Envelope at
        // this stage in processing.
        if let Some(category) = event_category {
            envelope_limiter.assume_event(category);
        }

        let scoping = state.envelope_context.scoping();
        let (enforcement, limits) = metric!(timer(RelayTimers::EventProcessingRateLimiting), {
            envelope_limiter
                .enforce(&mut state.envelope, &scoping)
                .map_err(ProcessingError::QuotasFailed)?
        });

        if enforcement.extracted_transaction_metrics.is_active() {
            state.extracted_metrics.clear();
        }

        if limits.is_limited() {
            ProjectCache::from_registry()
                .do_send(UpdateRateLimits::new(scoping.project_key, limits));
        }

        enforcement.track_outcomes(&state.envelope, &state.envelope_context.scoping());

        if remove_event {
            state.remove_event();
            debug_assert!(state.envelope.is_empty());
        }

        Ok(())
    }

    /// Extract metrics for transaction events with breakdowns and measurements.
    fn extract_transaction_metrics(
        &self,
        state: &mut ProcessEnvelopeState,
    ) -> Result<(), ProcessingError> {
        if state.transaction_metrics_extracted {
            // Nothing to do here.
            return Ok(());
        }

        let config = match state.project_state.config().transaction_metrics {
            Some(ErrorBoundary::Ok(ref config)) => config,
            _ => return Ok(()),
        };

        if !config.is_enabled() {
            return Ok(());
        }

        let conditional_tagging_config = state
            .project_state
            .config
            .metric_conditional_tagging
            .as_slice();

        if let Some(event) = state.event.value() {
            let extracted_anything;
            metric!(
                timer(RelayTimers::TransactionMetricsExtraction),
                extracted_anything = &extracted_anything.to_string(),
                {
                    // Actual logic outsourced for unit tests
                    extracted_anything = extract_transaction_metrics(
                        config,
                        conditional_tagging_config,
                        event,
                        &mut state.extracted_metrics,
                    );
                }
            );
            state.transaction_metrics_extracted = true;

            if let Some(context) = state.envelope.sampling_context() {
                track_sampling_metrics(&state.project_state, context, event);
            }
        }
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
                process_value(event, &mut processor, ProcessingState::root())
                    .map_err(ProcessingError::ProcessingFailed)?;
            }
            let pii_config = config
                .datascrubbing_settings
                .pii_config()
                .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;
            if let Some(config) = pii_config {
                let mut processor = PiiProcessor::new(config.compiled());
                process_value(event, &mut processor, ProcessingState::root())
                    .map_err(ProcessingError::ProcessingFailed)?;
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
        let envelope = &mut state.envelope;
        if let Some(ref config) = state.project_state.config.pii_config {
            let minidump = envelope
                .get_item_by_mut(|item| item.attachment_type() == Some(AttachmentType::Minidump));

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
                        relay_log::warn!("failed to scrub minidump: {}", LogError(&scrub_error));
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
        event_item.set_metrics_extracted(state.transaction_metrics_extracted);

        // If there are sample rates, write them back to the envelope. In processing mode, sample
        // rates have been removed from the state and burnt into the event via `finalize_event`.
        if let Some(sample_rates) = state.sample_rates.take() {
            event_item.set_sample_rates(sample_rates);
        }

        state.envelope.add_item(event_item);

        Ok(())
    }

    /// Run dynamic sampling rules to see if we keep the envelope or remove it.
    fn sample_envelope(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let client_ip = state.envelope.meta().client_addr();

        match utils::should_keep_event(
            state.envelope.sampling_context(),
            state.event.value(),
            client_ip,
            &state.project_state,
            state.sampling_project_state.as_deref(),
            self.config.processing_enabled(),
        ) {
            SamplingResult::Drop(rule_id) => {
                state
                    .envelope_context
                    .reject(Outcome::FilteredSampling(rule_id));

                Err(ProcessingError::Sampled(rule_id))
            }
            SamplingResult::Keep => Ok(()),
        }
    }

    fn light_normalize_event(
        &self,
        state: &mut ProcessEnvelopeState,
    ) -> Result<(), ProcessingError> {
        let client_ipaddr = state.envelope.meta().client_addr().map(IpAddr::from);
        let config = LightNormalizationConfig {
            client_ip: client_ipaddr.as_ref(),
            user_agent: state.envelope.meta().user_agent(),
            received_at: Some(state.envelope_context.received_at()),
            max_secs_in_past: Some(self.config.max_secs_in_past()),
            max_secs_in_future: Some(self.config.max_secs_in_future()),
            measurements_config: state.project_state.config.measurements.as_ref(),
            breakdowns_config: state.project_state.config.breakdowns_v2.as_ref(),
            normalize_user_agent: Some(true),
            is_renormalize: false,
        };

        metric!(timer(RelayTimers::EventProcessingLightNormalization), {
            relay_general::store::light_normalize_event(&mut state.event, &config)
                .map_err(|_| ProcessingError::InvalidTransaction)?;
        });

        Ok(())
    }

    fn process_state(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        macro_rules! if_processing {
            ($if_true:block) => {
                #[cfg(feature = "processing")] {
                    if self.config.processing_enabled() $if_true
                }
            };
        }

        self.process_sessions(state);
        self.process_client_reports(state);
        self.process_user_reports(state);
        self.process_profiles(state);
        self.process_replays(state);

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
            self.filter_event(state)?;
            self.extract_transaction_metrics(state)?;
            self.sample_envelope(state)?;

            if_processing!({
                self.store_process_event(state)?;
            });
        }

        if_processing!({
            self.enforce_quotas(state)?;
        });

        if state.has_event() {
            self.scrub_event(state)?;
            self.serialize_event(state)?;
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
        let client = state.envelope.meta().client().map(str::to_owned);
        let user_agent = state.envelope.meta().user_agent().map(str::to_owned);
        let project_key = state.envelope.meta().public_key();

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
                        if !state.extracted_metrics.is_empty() {
                            let project_cache = ProjectCache::from_registry();
                            project_cache
                                .do_send(InsertMetrics::new(project_key, state.extracted_metrics));
                        }

                        // The envelope could be modified or even emptied during processing, which
                        // requires recomputation of the context.
                        state.envelope_context.update(&state.envelope);

                        let envelope_response = if state.envelope.is_empty() {
                            // Individual rate limits have already been issued
                            state.envelope_context.reject(Outcome::RateLimited(None));
                            None
                        } else {
                            Some((state.envelope, state.envelope_context))
                        };

                        Ok(ProcessEnvelopeResponse {
                            envelope: envelope_response,
                        })
                    }
                    Err(err) => {
                        if let Some(outcome) = err.to_outcome() {
                            state.envelope_context.reject(outcome);
                        }

                        if !state.extracted_metrics.is_empty() && err.should_keep_metrics() {
                            let project_cache = ProjectCache::from_registry();
                            project_cache
                                .do_send(InsertMetrics::new(project_key, state.extracted_metrics));
                        }

                        Err(err)
                    }
                }
            },
        )
    }

    fn handle_process_envelope(&self, message: ProcessEnvelope) {
        let project_key = message.envelope.meta().public_key();
        let wait_time = message.envelope_context.start_time().elapsed();
        metric!(timer(RelayTimers::EnvelopeWaitTime) = wait_time);

        let result = metric!(timer(RelayTimers::EnvelopeProcessingTime), {
            self.process(message)
        });

        match result {
            Ok(response) => {
                if let Some((envelope, envelope_context)) = response.envelope {
                    EnvelopeManager::from_registry().send(SubmitEnvelope {
                        envelope,
                        envelope_context,
                    })
                };
            }
            Err(error) => {
                // Errors are only logged for what we consider infrastructure or implementation
                // bugs. In other cases, we "expect" errors and log them as debug level.
                if error.is_internal() {
                    relay_log::with_scope(
                        |scope| scope.set_tag("project_key", project_key),
                        || relay_log::error!("error processing envelope: {}", LogError(&error)),
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

        let received = relay_common::instant_to_date_time(start_time);
        let received_timestamp = UnixTimestamp::from_secs(received.timestamp() as u64);

        let project_cache = ProjectCache::from_registry();
        let clock_drift_processor =
            ClockDriftProcessor::new(sent_at, received).at_least(MINIMUM_CLOCK_DRIFT);

        for item in items {
            let payload = item.payload();
            if item.ty() == &ItemType::Metrics {
                let mut timestamp = item.timestamp().unwrap_or(received_timestamp);
                clock_drift_processor.process_timestamp(&mut timestamp);

                let min_timestamp = max(
                    0,
                    received.timestamp() - self.config.max_session_secs_in_past(),
                ) as u64;
                let max_timestamp =
                    (received.timestamp() + self.config.max_secs_in_future()) as u64;
                if min_timestamp <= timestamp.as_secs() && timestamp.as_secs() <= max_timestamp {
                    let metrics =
                        Metric::parse_all(&payload, timestamp).filter_map(|result| result.ok());

                    relay_log::trace!("inserting metrics into project cache");
                    project_cache.do_send(InsertMetrics::new(public_key, metrics));
                }
            } else if item.ty() == &ItemType::MetricBuckets {
                match Bucket::parse_all(&payload) {
                    Ok(mut buckets) => {
                        for bucket in &mut buckets {
                            clock_drift_processor.process_timestamp(&mut bucket.timestamp);
                        }

                        relay_log::trace!("merging metric buckets into project cache");
                        project_cache.do_send(MergeBuckets::new(public_key, buckets));
                    }
                    Err(error) => {
                        relay_log::debug!("failed to parse metric bucket: {}", LogError(&error));
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
                let mut encoder = BrotliEncoder::new(Vec::new(), 5);
                encoder.write_all(body.as_ref())?;
                encoder.finish()?
            }
        };
        Ok(envelope_body)
    }

    fn handle_encode_envelope(&self, message: EncodeEnvelope) {
        let mut request = message.request;
        match Self::encode_envelope_body(request.envelope_body, request.http_encoding) {
            Err(e) => {
                request.response_sender.map(|sender| {
                    sender
                        .send(Err(SendEnvelopeError::BodyEncodingFailed(e)))
                        .ok()
                });
            }
            Ok(envelope_body) => {
                request.envelope_body = envelope_body;
                UpstreamRelay::from_registry().do_send(SendRequest(request));
            }
        }
    }

    fn handle_message(&self, message: EnvelopeProcessor) {
        match message {
            EnvelopeProcessor::ProcessEnvelope(message) => self.handle_process_envelope(*message),
            EnvelopeProcessor::ProcessMetrics(message) => self.handle_process_metrics(*message),
            EnvelopeProcessor::EncodeEnvelope(message) => self.handle_encode_envelope(*message),
        }
    }
}

impl Service for EnvelopeProcessorService {
    type Interface = EnvelopeProcessor;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let thread_count = self.config.cpu_concurrency();
        relay_log::info!("starting {} envelope processing workers", thread_count);

        tokio::spawn(async move {
            let service = Arc::new(self);
            let semaphore = Arc::new(Semaphore::new(thread_count));

            while let (Some(message), Ok(permit)) =
                tokio::join!(rx.recv(), semaphore.clone().acquire_owned())
            {
                let service = service.clone();
                tokio::task::spawn_blocking(move || {
                    service.handle_message(message);
                    drop(permit);
                });
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use relay_general::pii::{DataScrubbingConfig, PiiConfig};

    use crate::{actors::project::ProjectConfig, extractors::RequestMeta};

    use super::*;

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
        let d1 = Utc.ymd(2019, 10, 10).and_hms(12, 10, 10);
        let d2 = Utc.ymd(2019, 10, 11).and_hms(12, 10, 10);

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
        let d1 = Utc.ymd(2019, 10, 10).and_hms(12, 10, 10);
        let d2 = Utc.ymd(2019, 10, 11).and_hms(12, 10, 10);

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
        EnvelopeProcessorService {
            config: Arc::new(config),
            #[cfg(feature = "processing")]
            rate_limiter: None,
            #[cfg(feature = "processing")]
            geoip_lookup: None,
        }
    }

    #[test]
    #[ignore = "The current Register panics if the Addr of an Actor (that is not yet started) is
    queried, hence this test fails. The old Register returned dummy Addr's hence this did not fail."]
    fn test_user_report_invalid() {
        let processor = create_test_processor(Default::default());
        let event_id = protocol::EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::UserReport);
            item.set_payload(ContentType::Json, r###"{"foo": "bar"}"###);
            item
        });

        envelope.add_item({
            let mut item = Item::new(ItemType::Event);
            item.set_payload(ContentType::Json, "{}");
            item
        });

        let new_envelope = relay_test::with_system(move || {
            let envelope_response = processor
                .process(ProcessEnvelope {
                    envelope_context: EnvelopeContext::standalone(&envelope),
                    envelope,
                    project_state: Arc::new(ProjectState::allowed()),
                    sampling_project_state: None,
                })
                .unwrap();

            envelope_response.envelope.unwrap().0
        });

        assert_eq!(new_envelope.len(), 1);
        assert_eq!(new_envelope.items().next().unwrap().ty(), &ItemType::Event);
    }

    #[test]
    #[ignore = "The current Register panics if the Addr of an Actor (that is not yet started) is
    queried, hence this test fails. The old Register returned dummy Addr's hence this did not fail."]
    fn test_browser_version_extraction_with_pii_like_data() {
        let processor = create_test_processor(Default::default());
        let event_id = protocol::EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::Event);
            item.set_payload(
                ContentType::Json,
                r###"
                    {
                        "request": {
                            "headers": [
                                ["User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"]
                            ]
                        }
                    }
                "###,
            );
            item
        });

        let new_envelope = relay_test::with_system(move || {
            let mut datascrubbing_settings = DataScrubbingConfig::default();
            // enable all the default scrubbing
            datascrubbing_settings.scrub_data = true;
            datascrubbing_settings.scrub_defaults = true;
            datascrubbing_settings.scrub_ip_addresses = true;

            // Make sure to mask any IP-like looking data
            let pii_config = PiiConfig::from_json(
                r##"
                {
                    "applications": {
                        "**": ["@ip:mask"]
                    }
                }
                "##,
            )
            .unwrap();

            let config = ProjectConfig {
                datascrubbing_settings,
                pii_config: Some(pii_config),
                ..Default::default()
            };

            let mut project_state = ProjectState::allowed();
            project_state.config = config;
            let envelope_response = processor
                .process(ProcessEnvelope {
                    envelope_context: EnvelopeContext::standalone(&envelope),
                    envelope,
                    project_state: Arc::new(project_state),
                    sampling_project_state: None,
                })
                .unwrap();
            envelope_response.envelope.unwrap().0
        });

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
        let browser = contexts.get("browser").unwrap();
        assert_eq!(
            r#"{"name":"Chrome","version":"103.0.0","type":"browser"}"#,
            browser.to_json().unwrap()
        );
    }

    #[test]
    #[ignore = "The current Register panics if the Addr of an Actor (that is not yet started) is
    queried, hence this test fails. The old Register returned dummy Addr's hence this did not fail."]
    fn test_client_report_removal() {
        relay_test::setup();

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
                r###"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "###,
            );
            item
        });

        let envelope_response = relay_test::with_system(move || {
            processor
                .process(ProcessEnvelope {
                    envelope_context: EnvelopeContext::standalone(&envelope),
                    envelope,
                    project_state: Arc::new(ProjectState::allowed()),
                    sampling_project_state: None,
                })
                .unwrap()
        });

        assert!(envelope_response.envelope.is_none());
    }

    #[test]
    fn test_client_report_forwarding() {
        relay_test::setup();

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
                r###"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "###,
            );
            item
        });

        let envelope_response = relay_test::with_system(move || {
            processor
                .process(ProcessEnvelope {
                    envelope_context: EnvelopeContext::standalone(&envelope),
                    envelope,
                    project_state: Arc::new(ProjectState::allowed()),
                    sampling_project_state: None,
                })
                .unwrap()
        });

        let (envelope, ctx) = envelope_response.envelope.unwrap();
        let item = envelope.items().next().unwrap();
        assert_eq!(item.ty(), &ItemType::ClientReport);

        ctx.accept(); // do not try to capture or emit outcomes
    }

    #[test]
    #[cfg(feature = "processing")]
    #[ignore = "requires registry for the TestStore"]
    fn test_client_report_removal_in_processing() {
        relay_test::setup();

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
                r###"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "###,
            );
            item
        });

        let envelope_response = relay_test::with_system(move || {
            processor
                .process(ProcessEnvelope {
                    envelope_context: EnvelopeContext::standalone(&envelope),
                    envelope,
                    project_state: Arc::new(ProjectState::allowed()),
                    sampling_project_state: None,
                })
                .unwrap()
        });

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
        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "adsf"),
            Err(_)
        ));
        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:"),
            Err(_)
        ));
        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:foo"),
            Err(_)
        ));
        assert!(matches!(
            outcome_from_parts(ClientReportField::FilteredSampling, "Sampled:123"),
            Ok(Outcome::FilteredSampling(RuleId(123)))
        ));
    }

    #[test]
    fn test_from_outcome_type_filtered() {
        assert!(matches!(
            outcome_from_parts(ClientReportField::Filtered, "error-message"),
            Ok(Outcome::Filtered(FilterStatKey::ErrorMessage))
        ));
        assert!(matches!(
            outcome_from_parts(ClientReportField::Filtered, "adsf"),
            Err(_)
        ));
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
}
