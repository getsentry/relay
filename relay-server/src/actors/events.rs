use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::prelude::*;
use chrono::{DateTime, Duration as SignedDuration, Utc};
use failure::Fail;
use futures::prelude::*;
use serde_json::Value as SerdeValue;

use relay_common::{clone, metric, ProjectId};
use relay_config::{Config, RelayMode};
use relay_general::pii::{PiiAttachmentsProcessor, PiiProcessor};
use relay_general::processor::{process_value, ProcessingState};
use relay_general::protocol::{
    Breadcrumb, Csp, Event, EventId, EventType, ExpectCt, ExpectStaple, Hpkp, IpAddr,
    LenientString, Metrics, SecurityReportType, SessionUpdate, Timestamp, UserReport, Values,
};
use relay_general::store::ClockDriftProcessor;
use relay_general::types::{Annotated, Array, Object, ProcessingAction, Value};
use relay_log::LogError;
use relay_quotas::RateLimits;
use relay_redis::RedisPool;

use crate::actors::outcome::{DiscardReason, Outcome, OutcomeProducer, TrackOutcome};
use crate::actors::project::{
    CheckEnvelope, GetProjectState, Project, ProjectState, UpdateRateLimits,
};
use crate::actors::project_cache::ProjectError;
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequestError};
use crate::envelope::{self, AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::http::{HttpError, RequestBuilder};
use crate::metrics::{RelayCounters, RelayHistograms, RelaySets, RelayTimers};
use crate::service::ServerError;
use crate::utils::{self, ChunkedFormDataAggregator, FormDataIter, FutureExt};

#[cfg(feature = "processing")]
use {
    crate::actors::store::{StoreEnvelope, StoreError, StoreForwarder},
    crate::service::ServerErrorKind,
    crate::utils::EnvelopeLimiter,
    failure::ResultExt,
    relay_filter::FilterStatKey,
    relay_general::store::{GeoIpLookup, StoreConfig, StoreProcessor},
    relay_quotas::{DataCategory, RateLimitingError, RedisRateLimiter},
};

/// The minimum clock drift for correction to apply.
const MINIMUM_CLOCK_DRIFT: Duration = Duration::from_secs(55 * 60);

#[derive(Debug, Fail)]
pub enum QueueEnvelopeError {
    #[fail(display = "Too many events (event_buffer_size reached)")]
    TooManyEvents,
}

#[derive(Debug, Fail)]
enum ProcessingError {
    #[fail(display = "invalid json in event")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "invalid message pack event payload")]
    InvalidMsgpack(#[cause] rmp_serde::decode::Error),

    #[cfg(feature = "processing")]
    #[fail(display = "invalid unreal crash report")]
    InvalidUnrealReport(#[cause] symbolic::unreal::Unreal4Error),

    #[fail(display = "event payload too large")]
    PayloadTooLarge,

    #[fail(display = "invalid transaction event")]
    InvalidTransaction,

    #[fail(display = "event processor failed")]
    ProcessingFailed(#[cause] ProcessingAction),

    #[fail(display = "duplicate {} in event", _0)]
    DuplicateItem(ItemType),

    #[fail(display = "failed to extract event payload")]
    NoEventPayload,

    #[fail(display = "could not schedule project fetch")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to resolve project information")]
    ProjectFailed(#[cause] ProjectError),

    #[fail(display = "missing project id in DSN")]
    MissingProjectId,

    #[fail(display = "invalid security report type")]
    InvalidSecurityType,

    #[fail(display = "invalid security report")]
    InvalidSecurityReport(#[cause] serde_json::Error),

    #[fail(display = "event submission rejected with reason: {:?}", _0)]
    EventRejected(DiscardReason),

    #[cfg(feature = "processing")]
    #[fail(display = "event filtered with reason: {:?}", _0)]
    EventFiltered(FilterStatKey),

    #[fail(display = "could not serialize event payload")]
    SerializeFailed(#[cause] serde_json::Error),

    #[fail(display = "could not send event to upstream")]
    SendFailed(#[cause] UpstreamRequestError),

    #[cfg(feature = "processing")]
    #[fail(display = "could not store event")]
    StoreFailed(#[cause] StoreError),

    #[fail(display = "event rate limited")]
    RateLimited(RateLimits),

    #[cfg(feature = "processing")]
    #[fail(display = "failed to apply quotas")]
    QuotasFailed(#[cause] RateLimitingError),

    #[fail(display = "event exceeded its configured lifetime")]
    Timeout,

    #[fail(display = "envelope empty, transaction removed by sampling")]
    TransactionSampled,

    #[fail(display = "envelope empty, event removed by sampling")]
    EventSampled,
}

impl ProcessingError {
    fn to_outcome(&self) -> Option<Outcome> {
        match *self {
            // General outcomes for invalid events
            Self::PayloadTooLarge => Some(Outcome::Invalid(DiscardReason::TooLarge)),
            Self::InvalidJson(_) => Some(Outcome::Invalid(DiscardReason::InvalidJson)),
            Self::InvalidMsgpack(_) => Some(Outcome::Invalid(DiscardReason::InvalidMsgpack)),
            Self::EventRejected(reason) => Some(Outcome::Invalid(reason)),
            Self::InvalidSecurityType => Some(Outcome::Invalid(DiscardReason::SecurityReportType)),
            Self::InvalidSecurityReport(_) => Some(Outcome::Invalid(DiscardReason::SecurityReport)),
            Self::InvalidTransaction => Some(Outcome::Invalid(DiscardReason::InvalidTransaction)),
            Self::DuplicateItem(_) => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::NoEventPayload => Some(Outcome::Invalid(DiscardReason::NoEventPayload)),
            Self::RateLimited(ref rate_limits) => rate_limits
                .longest_error()
                .map(|r| Outcome::RateLimited(r.reason_code.clone())),

            // Processing-only outcomes (Sentry-internal Relays)
            #[cfg(feature = "processing")]
            Self::InvalidUnrealReport(_) => Some(Outcome::Invalid(DiscardReason::ProcessUnreal)),
            #[cfg(feature = "processing")]
            Self::EventFiltered(ref filter_stat_key) => Some(Outcome::Filtered(*filter_stat_key)),

            // Internal errors
            Self::SerializeFailed(_)
            | Self::ScheduleFailed(_)
            | Self::ProjectFailed(_)
            | Self::Timeout
            | Self::ProcessingFailed(_)
            | Self::MissingProjectId => Some(Outcome::Invalid(DiscardReason::Internal)),
            #[cfg(feature = "processing")]
            Self::StoreFailed(_) | Self::QuotasFailed(_) => {
                Some(Outcome::Invalid(DiscardReason::Internal))
            }

            // Dynamic sampling (not an error, just discarding messages that were removed by sampling)
            Self::TransactionSampled => Some(Outcome::Invalid(DiscardReason::TransactionSampled)),
            Self::EventSampled => Some(Outcome::Invalid(DiscardReason::EventSampled)),

            // If we send to an upstream, we don't emit outcomes.
            Self::SendFailed(_) => None,
        }
    }
}

type ExtractedEvent = (Annotated<Event>, usize);

/// A state container for envelope processing.
#[derive(Debug)]
struct ProcessEnvelopeState {
    /// The envelope.
    ///
    /// The pipeline can mutate the envelope and remove or add items. In particular, event items are
    /// removed at the beginning of event processing and re-added in the end.
    envelope: Envelope,

    /// The extracted event payload.
    ///
    /// For Envelopes without event payloads, this contains `Annotated::empty`. If a single item has
    /// `creates_event`, the event is required and the pipeline errors if no payload can be
    /// extracted.
    event: Annotated<Event>,

    /// Partial metrics of the Event during construction.
    ///
    /// The pipeline stages can add to this metrics objects. In `finalize_event`, the metrics are
    /// persisted into the Event. All modifications afterwards will have no effect.
    metrics: Metrics,

    /// Rate limits returned in processing mode.
    ///
    /// The rate limiter is invoked in processing mode, after which the resulting limits are stored
    /// in this field. Note that there can be rate limits even if the envelope still carries items.
    ///
    /// These are always empty in non-processing mode, since the rate limiter is not invoked.
    rate_limits: RateLimits,

    /// The state of the project that this envelope belongs to.
    project_state: Arc<ProjectState>,

    /// The id of the project that this envelope is ingested into.
    ///
    /// This identifier can differ from the one stated in the Envelope's DSN if the key was moved to
    /// a new project or on the legacy endpoint. In that case, normalization will update the project
    /// ID.
    project_id: ProjectId,

    /// UTC date time converted from the `start_time` instant.
    received_at: DateTime<Utc>,
}

impl ProcessEnvelopeState {
    /// Returns whether any item in the envelope creates an event.
    ///
    /// This is used to branch into the event processing pipeline. If this function returns false,
    /// only rate limits are executed.
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
    #[cfg(feature = "processing")]
    fn event_category(&self) -> Option<DataCategory> {
        self.event_type().map(DataCategory::from)
    }

    /// Removes the event payload from this processing state.
    #[cfg(feature = "processing")]
    fn remove_event(&mut self) {
        self.event = Annotated::empty();
    }
}

/// Synchronous service for processing envelopes.
pub struct EventProcessor {
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    rate_limiter: Option<RedisRateLimiter>,
    #[cfg(feature = "processing")]
    geoip_lookup: Option<Arc<GeoIpLookup>>,
}

impl EventProcessor {
    #[inline]
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            #[cfg(feature = "processing")]
            rate_limiter: None,
            #[cfg(feature = "processing")]
            geoip_lookup: None,
        }
    }

    #[cfg(feature = "processing")]
    #[inline]
    pub fn with_rate_limiter(mut self, rate_limiter: Option<RedisRateLimiter>) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    #[cfg(feature = "processing")]
    #[inline]
    pub fn with_geoip_lookup(mut self, geoip_lookup: Option<Arc<GeoIpLookup>>) -> Self {
        self.geoip_lookup = geoip_lookup;
        self
    }

    /// Validates all sessions in the envelope, if any.
    ///
    /// Sessions are removed from the envelope if they contain invalid JSON or if their timestamps
    /// are out of range after clock drift correction.
    fn process_sessions(&self, state: &mut ProcessEnvelopeState) {
        let envelope = &mut state.envelope;
        let received = state.received_at;
        let client_addr = envelope.meta().client_addr();

        let clock_drift_processor =
            ClockDriftProcessor::new(envelope.sent_at(), received).at_least(MINIMUM_CLOCK_DRIFT);

        envelope.retain_items(|item| {
            if item.ty() != ItemType::Session {
                return true;
            }

            let mut changed = false;
            let payload = item.payload();

            let mut session = match SessionUpdate::parse(&payload) {
                Ok(session) => session,
                Err(error) => {
                    return relay_log::with_scope(
                        |s| s.set_extra("session", String::from_utf8_lossy(&payload).into()),
                        || {
                            // Skip gracefully here to allow sending other sessions.
                            relay_log::error!("failed to store session: {}", LogError(&error));
                            false
                        },
                    );
                }
            };

            if session.sequence == u64::max_value() {
                relay_log::trace!("skipping session due to sequence overflow");
                return false;
            }

            if clock_drift_processor.is_drifted() {
                relay_log::trace!("applying clock drift correction to session");
                clock_drift_processor.process_session(&mut session);
                changed = true;
            }

            if session.timestamp < session.started {
                relay_log::trace!("fixing session timestamp to {}", session.timestamp);
                session.timestamp = session.started;
                changed = true;
            }

            let max_age = SignedDuration::seconds(self.config.max_session_secs_in_past());
            if (received - session.started) > max_age || (received - session.timestamp) > max_age {
                relay_log::trace!("skipping session older than {} days", max_age.num_days());
                return false;
            }

            let max_future = SignedDuration::seconds(self.config.max_secs_in_future());
            if (session.started - received) > max_age || (session.timestamp - received) > max_age {
                relay_log::trace!(
                    "skipping session more than {}s in the future",
                    max_future.num_seconds()
                );
                return false;
            }

            if let Some(ref ip_address) = session.attributes.ip_address {
                if ip_address.is_auto() {
                    session.attributes.ip_address = client_addr.map(IpAddr::from);
                    changed = true;
                }
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
        });
    }

    /// Validates and normalizes all user report items in the envelope.
    ///
    /// User feedback items are removed from the envelope if they contain invalid JSON or if the
    /// JSON violates the schema (basic type validation). Otherwise, their normalized representation
    /// is written back into the item.
    fn process_user_reports(&self, state: &mut ProcessEnvelopeState) {
        state.envelope.retain_items(|item| {
            if item.ty() != ItemType::UserReport {
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

    /// Creates and initializes the processing state.
    ///
    /// This applies defaults to the envelope and initializes empty rate limits.
    fn prepare_state(
        &self,
        message: ProcessEnvelope,
    ) -> Result<ProcessEnvelopeState, ProcessingError> {
        let ProcessEnvelope {
            mut envelope,
            project_state,
            start_time,
        } = message;

        // Set the event retention. Effectively, this value will only be available in processing
        // mode when the full project config is queried from the upstream.
        if let Some(retention) = project_state.config.event_retention {
            envelope.set_retention(retention);
        }

        // Prefer the project's project ID, and fall back to the stated project id from the
        // envelope. The project ID is available in all modes, other than in proxy mode, where
        // events for unknown projects are forwarded blindly.
        //
        // Neither ID can be available in proxy mode on the /store/ endpoint. This is not supported,
        // since we cannot process an event without project ID, so drop it.
        let project_id = project_state
            .project_id
            .or_else(|| envelope.meta().project_id())
            .ok_or(ProcessingError::MissingProjectId)?;

        // Ensure the project ID is updated to the stored instance for this project cache. This can
        // differ in two cases:
        //  1. The envelope was sent to the legacy `/store/` endpoint without a project ID.
        //  2. The DSN was moved and the envelope sent to the old project ID.
        envelope.meta_mut().set_project_id(project_id);

        Ok(ProcessEnvelopeState {
            envelope,
            event: Annotated::empty(),
            metrics: Metrics::default(),
            rate_limits: RateLimits::new(),
            project_state,
            project_id,
            received_at: relay_common::instant_to_date_time(start_time),
        })
    }

    /// Expands Unreal 4 items inside an envelope.
    ///
    /// If the envelope does NOT contain an `UnrealReport` item, it doesn't do anything. If the envelope
    /// contains an `UnrealReport` item, it removes it from the envelope and inserts new items for each
    /// of its contents.
    ///
    /// After this, the `EventProcessor` should be able to process the envelope the same way it
    /// processes any other envelopes.
    #[cfg(feature = "processing")]
    fn expand_unreal(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let envelope = &mut state.envelope;

        if let Some(item) = envelope.take_item_by(|item| item.ty() == ItemType::UnrealReport) {
            utils::expand_unreal_envelope(item, envelope)
                .map_err(ProcessingError::InvalidUnrealReport)?;
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
            // logged at call site of extract_event
            relay_log::configure_scope(|scope| {
                scope.set_extra("payload", String::from_utf8_lossy(&data).into());
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
            SecurityReportType::ExpectCt => EventType::ExpectCT,
            SecurityReportType::ExpectStaple => EventType::ExpectStaple,
            SecurityReportType::Hpkp => EventType::Hpkp,
        });

        Ok((Annotated::new(event), len))
    }

    fn merge_formdata(&self, target: &mut SerdeValue, item: Item) {
        let payload = item.payload();
        let mut aggregator = ChunkedFormDataAggregator::new();

        for entry in FormDataIter::new(&payload) {
            if entry.key() == "sentry" {
                // Custom clients can submit longer payloads and should JSON encode event data into
                // the optional `sentry` field.
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

            // session data is never considered as part of deduplication
            ItemType::Session => false,
            ItemType::Sessions => false,
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
        let event_item = envelope.take_item_by(|item| item.ty() == ItemType::Event);
        let transaction_item = envelope.take_item_by(|item| item.ty() == ItemType::Transaction);
        let security_item = envelope.take_item_by(|item| item.ty() == ItemType::Security);
        let raw_security_item = envelope.take_item_by(|item| item.ty() == ItemType::RawSecurity);
        let form_item = envelope.take_item_by(|item| item.ty() == ItemType::FormData);
        let attachment_item = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::EventPayload));
        let breadcrumbs1 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));
        let breadcrumbs2 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));

        // Event items can never occur twice in an envelope.
        if let Some(duplicate) = envelope.get_item_by(|item| self.is_duplicate(item)) {
            return Err(ProcessingError::DuplicateItem(duplicate.ty()));
        }

        let (event, event_len) = if let Some(item) = event_item.or(security_item) {
            relay_log::trace!("processing json event");
            metric!(timer(RelayTimers::EventProcessingDeserialize), {
                // Event items can never include transactions, so retain the event type and let
                // inference deal with this during store normalization.
                self.event_from_json_payload(item, None)?
            })
        } else if let Some(item) = transaction_item {
            relay_log::trace!("processing json transaction");
            metric!(timer(RelayTimers::EventProcessingDeserialize), {
                // Transaction items can only contain transaction events. Force the event type to
                // hint to normalization that we're dealing with a transaction now.
                self.event_from_json_payload(item, Some(EventType::Transaction))?
            })
        } else if let Some(item) = raw_security_item {
            relay_log::trace!("processing security report");
            self.event_from_security_report(item)?
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
            let attachment_size = envelope
                .items()
                .filter(|item| item.attachment_type() == Some(AttachmentType::Attachment))
                .map(|item| item.len() as u64)
                .sum::<u64>();

            if attachment_size > 0 {
                state.metrics.bytes_ingested_event_attachment = Annotated::new(attachment_size);
            }

            event._metrics = Annotated::new(std::mem::take(&mut state.metrics));
        }

        // TODO: Temporary workaround before processing. Experimental SDKs relied on a buggy
        // clock drift correction that assumes the event timestamp is the sent_at time. This
        // should be removed as soon as legacy ingestion has been removed.
        let sent_at = match envelope.sent_at() {
            Some(sent_at) => Some(sent_at),
            None if is_transaction => event.timestamp.value().copied().map(Timestamp::into_inner),
            None => None,
        };

        let mut processor =
            ClockDriftProcessor::new(sent_at, state.received_at).at_least(MINIMUM_CLOCK_DRIFT);
        process_value(&mut state.event, &mut processor, ProcessingState::root())
            .map_err(|_| ProcessingError::InvalidTransaction)?;

        Ok(())
    }

    #[cfg(feature = "processing")]
    fn store_process_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let ProcessEnvelopeState {
            ref envelope,
            ref mut event,
            ref project_state,
            received_at,
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
            received_at: Some(received_at),
        };

        let mut store_processor = StoreProcessor::new(store_config, self.geoip_lookup.as_deref());
        metric!(timer(RelayTimers::EventProcessingProcess), {
            process_value(event, &mut store_processor, ProcessingState::root())
                .map_err(|_| ProcessingError::InvalidTransaction)?;
        });

        Ok(())
    }

    #[cfg(feature = "processing")]
    fn filter_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let event = match state.event.value_mut() {
            Some(event) => event,
            None => return Err(ProcessingError::NoEventPayload),
        };

        let client_ip = state.envelope.meta().client_addr();
        let filter_settings = &state.project_state.config.filter_settings;

        metric!(timer(RelayTimers::EventProcessingFiltering), {
            relay_filter::should_filter(event, client_ip, filter_settings)
                .map_err(ProcessingError::EventFiltered)
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
        let category = state.event_category();

        // When invoking the rate limiter, capture if the event item has been rate limited to also
        // remove it from the processing state eventually.
        let mut envelope_limiter = EnvelopeLimiter::new(|item_scope, quantity| {
            let limits = rate_limiter.is_rate_limited(quotas, item_scope, quantity)?;
            remove_event |= Some(item_scope.category) == category && limits.is_limited();
            Ok(limits)
        });

        // Tell the envelope limiter about the event, since it has been removed from the Envelope at
        // this stage in processing.
        if let Some(category) = category {
            envelope_limiter.assume_event(category);
        }

        // Fetch scoping again from the project state. This is a rather cheap operation at this
        // point and it is easier than passing scoping through all layers of `process_envelope`.
        let scoping = project_state.get_scoping(state.envelope.meta());

        state.rate_limits = metric!(timer(RelayTimers::EventProcessingRateLimiting), {
            envelope_limiter
                .enforce(&mut state.envelope, &scoping)
                .map_err(ProcessingError::QuotasFailed)?
        });

        if remove_event {
            state.remove_event();
            debug_assert!(state.envelope.is_empty());
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
                let compiled = config.compiled();
                let mut processor = PiiProcessor::new(&compiled);
                process_value(event, &mut processor, ProcessingState::root())
                    .map_err(ProcessingError::ProcessingFailed)?;
            }
            if let Some(ref config) = *config.datascrubbing_settings.pii_config() {
                let compiled = config.compiled();
                let mut processor = PiiProcessor::new(&compiled);
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

                let compiled = config.compiled();
                let processor = PiiAttachmentsProcessor::new(&compiled);

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
        state.envelope.add_item(event_item);

        Ok(())
    }

    /// Run dynamic sampling rules to see if we keep the event or remove it.
    fn sample_event(&self, state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
        let event = match &state.event.0 {
            None => return Ok(()), // can't process without an event
            Some(event) => event,
        };

        let project_id = state.project_id;
        match utils::should_keep_event(event, &state.project_state, project_id) {
            Some(false) => Err(ProcessingError::EventSampled),
            Some(true) => Ok(()),
            None => Ok(()), // Not enough info to make a definite evaluation, keep the event
        }
    }

    fn process_state(
        &self,
        mut state: ProcessEnvelopeState,
    ) -> Result<ProcessEnvelopeResponse, ProcessingError> {
        macro_rules! if_processing {
            ($if_true:block $(, $if_not:block)?) => {
                #[cfg(feature = "processing")] {
                    if self.config.processing_enabled() $if_true
                }
            };
        }

        self.process_sessions(&mut state);
        self.process_user_reports(&mut state);

        if state.creates_event() {
            if_processing!({
                self.expand_unreal(&mut state)?;
            });

            self.extract_event(&mut state).map_err(|error| {
                relay_log::error!("failed to extract event: {}", LogError(&error));
                error
            })?;

            if_processing!({
                self.process_unreal(&mut state)?;
                self.create_placeholders(&mut state);
            });

            self.finalize_event(&mut state)?;

            self.sample_event(&mut state)?;

            if_processing!({
                self.store_process_event(&mut state)?;
                self.filter_event(&mut state)?;
            });
        }

        if_processing!({
            self.enforce_quotas(&mut state)?;
        });

        if state.has_event() {
            self.scrub_event(&mut state)?;
            self.serialize_event(&mut state)?;
        }

        self.scrub_attachments(&mut state);

        Ok(ProcessEnvelopeResponse::from(state))
    }

    fn process(
        &self,
        message: ProcessEnvelope,
    ) -> Result<ProcessEnvelopeResponse, ProcessingError> {
        let state = self.prepare_state(message)?;

        let project_id = state.project_id;
        let client = state.envelope.meta().client().map(str::to_owned);
        let user_agent = state.envelope.meta().user_agent().map(str::to_owned);

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
            || self.process_state(state),
        )
    }
}

impl Actor for EventProcessor {
    type Context = SyncContext<Self>;
}

struct ProcessEnvelope {
    pub envelope: Envelope,
    pub project_state: Arc<ProjectState>,
    pub start_time: Instant,
}

#[cfg_attr(not(feature = "processing"), allow(dead_code))]
struct ProcessEnvelopeResponse {
    envelope: Option<Envelope>,
    rate_limits: RateLimits,
}

impl From<ProcessEnvelopeState> for ProcessEnvelopeResponse {
    fn from(state: ProcessEnvelopeState) -> Self {
        Self {
            envelope: Some(state.envelope).filter(|e| !e.is_empty()),
            rate_limits: state.rate_limits,
        }
    }
}

impl Message for ProcessEnvelope {
    type Result = Result<ProcessEnvelopeResponse, ProcessingError>;
}

impl Handler<ProcessEnvelope> for EventProcessor {
    type Result = Result<ProcessEnvelopeResponse, ProcessingError>;

    fn handle(&mut self, message: ProcessEnvelope, _context: &mut Self::Context) -> Self::Result {
        metric!(timer(RelayTimers::EnvelopeWaitTime) = message.start_time.elapsed());
        metric!(timer(RelayTimers::EnvelopeProcessingTime), {
            self.process(message)
        })
    }
}

/// Either a captured envelope or an error that occured during processing.
pub type CapturedEnvelope = Result<Envelope, String>;

pub struct EventManager {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    processor: Addr<EventProcessor>,
    current_active_events: u32,
    outcome_producer: Addr<OutcomeProducer>,
    captures: BTreeMap<EventId, CapturedEnvelope>,

    #[cfg(feature = "processing")]
    store_forwarder: Option<Addr<StoreForwarder>>,
}

impl EventManager {
    pub fn create(
        config: Arc<Config>,
        upstream: Addr<UpstreamRelay>,
        outcome_producer: Addr<OutcomeProducer>,
        redis_pool: Option<RedisPool>,
    ) -> Result<Self, ServerError> {
        let thread_count = config.cpu_concurrency();
        relay_log::info!("starting {} event processing workers", thread_count);

        #[cfg(not(feature = "processing"))]
        let _ = redis_pool;

        #[cfg(feature = "processing")]
        let processor = {
            let geoip_lookup = match config.geoip_path() {
                Some(p) => Some(Arc::new(
                    GeoIpLookup::open(p).context(ServerErrorKind::GeoIpError)?,
                )),
                None => None,
            };

            let rate_limiter = redis_pool
                .map(|pool| RedisRateLimiter::new(pool).max_limit(config.max_rate_limit()));

            SyncArbiter::start(
                thread_count,
                clone!(config, || EventProcessor::new(config.clone())
                    .with_rate_limiter(rate_limiter.clone())
                    .with_geoip_lookup(geoip_lookup.clone())),
            )
        };

        #[cfg(not(feature = "processing"))]
        let processor = SyncArbiter::start(
            thread_count,
            clone!(config, || EventProcessor::new(config.clone())),
        );

        #[cfg(feature = "processing")]
        let store_forwarder = if config.processing_enabled() {
            let actor = StoreForwarder::create(config.clone())?;
            Some(Arbiter::start(move |_| actor))
        } else {
            None
        };

        Ok(EventManager {
            config,
            upstream,
            processor,
            current_active_events: 0,
            captures: BTreeMap::new(),

            #[cfg(feature = "processing")]
            store_forwarder,

            outcome_producer,
        })
    }
}

impl Actor for EventManager {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Set the mailbox size to the size of the event buffer. This is a rough estimate but
        // should ensure that we're not dropping events unintentionally after we've accepted them.
        let mailbox_size = self.config.event_buffer_size() as usize;
        context.set_mailbox_capacity(mailbox_size);
        relay_log::info!("event manager started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("event manager stopped");
    }
}

pub struct QueueEnvelope {
    pub envelope: Envelope,
    pub project: Addr<Project>,
    pub sampling_project: Option<Addr<Project>>,
    pub start_time: Instant,
}

impl Message for QueueEnvelope {
    type Result = Result<Option<EventId>, QueueEnvelopeError>;
}

impl Handler<QueueEnvelope> for EventManager {
    type Result = Result<Option<EventId>, QueueEnvelopeError>;

    fn handle(&mut self, mut message: QueueEnvelope, context: &mut Self::Context) -> Self::Result {
        metric!(
            histogram(RelayHistograms::EnvelopeQueueSize) = u64::from(self.current_active_events)
        );

        metric!(
            histogram(RelayHistograms::EnvelopeQueueSizePct) = {
                let queue_size_pct = self.current_active_events as f32 * 100.0
                    / self.config.event_buffer_size() as f32;
                queue_size_pct.floor() as u64
            }
        );

        if self.config.event_buffer_size() <= self.current_active_events {
            return Err(QueueEnvelopeError::TooManyEvents);
        }

        let event_id = message.envelope.event_id();

        // Split the envelope into event-related items and other items. This allows to fast-track:
        //  1. Envelopes with only session items. They only require rate limiting.
        //  2. Event envelope processing can bail out if the event is filtered or rate limited,
        //     since all items depend on this event.
        if let Some(event_envelope) = message.envelope.split_by(Item::requires_event) {
            self.current_active_events += 1;
            context.notify(HandleEnvelope {
                envelope: event_envelope,
                sampling_project: message.sampling_project.clone(),
                project: message.project.clone(),
                start_time: message.start_time,
            });
        }

        self.current_active_events += 1;
        context.notify(HandleEnvelope {
            envelope: message.envelope,
            sampling_project: message.sampling_project,
            project: message.project,
            start_time: message.start_time,
        });

        // Actual event handling is performed asynchronously in a separate future. The lifetime of
        // that future will be tied to the EventManager's context. This allows to keep the Project
        // actor alive even if it is cleaned up in the ProjectManager.

        relay_log::trace!("queued event");
        Ok(event_id)
    }
}

struct HandleEnvelope {
    pub envelope: Envelope,
    pub project: Addr<Project>,
    pub sampling_project: Option<Addr<Project>>,
    pub start_time: Instant,
}

impl Message for HandleEnvelope {
    type Result = Result<(), ()>;
}

impl Handler<HandleEnvelope> for EventManager {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, message: HandleEnvelope, _ctx: &mut Self::Context) -> Self::Result {
        // We measure three timers while handling events, once they have been initially accepted:
        //
        // 1. `event.wait_time`: The time we take to get all dependencies for events before
        //    they actually start processing. This includes scheduling overheads, project config
        //    fetching, batched requests and congestions in the sync processor arbiter. This does
        //    not include delays in the incoming request (body upload) and skips all events that are
        //    fast-rejected.
        //
        // 2. `event.processing_time`: The time the sync processor takes to parse the event payload,
        //    apply normalizations, strip PII and finally re-serialize it into a byte stream. This
        //    is recorded directly in the EventProcessor.
        //
        // 3. `event.total_time`: The full time an event takes from being initially accepted up to
        //    being sent to the upstream (including delays in the upstream). This can be regarded
        //    the total time an event spent in this relay, corrected by incoming network delays.

        let upstream = self.upstream.clone();
        let processor = self.processor.clone();
        let outcome_producer = self.outcome_producer.clone();
        let capture = self.config.relay_mode() == RelayMode::Capture;
        let http_encoding = self.config.http_encoding();

        #[cfg(feature = "processing")]
        let store_forwarder = self.store_forwarder.clone();

        let HandleEnvelope {
            envelope,
            project,
            start_time,
            sampling_project,
        } = message;

        let event_id = envelope.event_id();
        let remote_addr = envelope.meta().client_addr();

        // Compute whether this envelope contains an event. This is used in error handling to
        // appropriately emit an outcome. Envelopes not containing events (such as standalone
        // attachment uploads or user reports) should never create outcomes.
        let is_event = envelope.items().any(Item::creates_event);

        let scoping = Rc::new(RefCell::new(envelope.meta().get_partial_scoping()));
        let is_received = Rc::new(AtomicBool::from(false));

        let future = project
            .send(CheckEnvelope::fetched(envelope))
            .map_err(ProcessingError::ScheduleFailed)
            .and_then(|result| result.map_err(ProcessingError::ProjectFailed))
            .and_then(clone!(scoping, |response| {
                // Use the project id from the loaded project state to account for redirects.
                let project_id = response.scoping.project_id.value();
                metric!(set(RelaySets::UniqueProjects) = project_id as i64);

                scoping.replace(response.scoping);

                let checked = response.result.map_err(ProcessingError::EventRejected)?;
                match checked.envelope {
                    Some(envelope) => Ok(envelope),
                    None => Err(ProcessingError::RateLimited(checked.rate_limits)),
                }
            }))
            .and_then(|envelope| {
                utils::sample_transaction(envelope, sampling_project, false)
                    .map_err(|()| (ProcessingError::TransactionSampled))
            })
            .and_then(|envelope| {
                if envelope.is_empty() {
                    Err(ProcessingError::TransactionSampled)
                } else {
                    Ok(envelope)
                }
            })
            .and_then(clone!(project, |envelope| {
                // get the state for the current project
                project
                    .send(GetProjectState)
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(|result| result.map_err(ProcessingError::ProjectFailed))
                    .map(|state| (envelope, state))
            }))
            .and_then(move |(envelope, project_state)| {
                processor
                    .send(ProcessEnvelope {
                        envelope,
                        project_state,
                        start_time,
                    })
                    .map_err(ProcessingError::ScheduleFailed)
                    .flatten()
            })
            .and_then(clone!(project, |processed| {
                let rate_limits = processed.rate_limits;

                // Processing returned new rate limits. Cache them on the project to avoid expensive
                // processing while the limit is active.
                if rate_limits.is_limited() {
                    project.do_send(UpdateRateLimits(rate_limits.clone()));
                }

                match processed.envelope {
                    Some(envelope) => Ok(envelope),
                    None => Err(ProcessingError::RateLimited(rate_limits)),
                }
            }))
            .into_actor(self)
            .and_then(clone!(scoping, is_received, |mut envelope, slf, _| {
                #[cfg(feature = "processing")]
                {
                    if let Some(store_forwarder) = store_forwarder {
                        relay_log::trace!("sending envelope to kafka");
                        let future = store_forwarder
                            .send(StoreEnvelope {
                                envelope,
                                start_time,
                                scoping: *scoping.borrow(),
                            })
                            .map_err(ProcessingError::ScheduleFailed)
                            .and_then(move |result| result.map_err(ProcessingError::StoreFailed))
                            .into_actor(slf);

                        return Box::new(future) as ResponseActFuture<_, _, _>;
                    }
                }

                // if we are in capture mode, we stash away the event instead of
                // forwarding it.
                if capture {
                    // XXX: this is wrong because captured_events does not take envelopes without
                    // event_id into account.
                    if let Some(event_id) = event_id {
                        relay_log::debug!("capturing envelope");
                        slf.captures.insert(event_id, Ok(envelope));
                    } else {
                        relay_log::debug!("dropping non event envelope");
                    }
                    return Box::new(fut::ok(())) as ResponseActFuture<_, _, _>;
                }

                relay_log::trace!("sending event to sentry endpoint");
                let project_id = scoping.borrow().project_id;
                let request = SendRequest::post(format!("/api/{}/envelope/", project_id)).build(
                    move |mut builder: RequestBuilder| {
                        // Override the `sent_at` timestamp. Since the event went through basic
                        // normalization, all timestamps have been corrected. We propagate the new
                        // `sent_at` to allow the next Relay to double-check this timestamp and
                        // potentially apply correction again. This is done as close to sending as
                        // possible so that we avoid internal delays.
                        envelope.set_sent_at(Utc::now());

                        let meta = envelope.meta();

                        if let Some(origin) = meta.origin() {
                            builder.header("Origin", origin.as_str());
                        }

                        if let Some(user_agent) = meta.user_agent() {
                            builder.header("User-Agent", user_agent);
                        }

                        builder
                            .content_encoding(http_encoding)
                            .header("X-Sentry-Auth", meta.auth_header())
                            .header("X-Forwarded-For", meta.forwarded_for())
                            .header("Content-Type", envelope::CONTENT_TYPE);

                        builder
                            .body(
                                envelope
                                    .to_vec()
                                    // XXX: upstream actor should allow for custom error type,
                                    // right now we are forced to shoehorn our envelope errors into
                                    // UpstreamRequestError
                                    .map_err(failure::Error::from)
                                    .map_err(actix_web::Error::from)
                                    .map_err(HttpError::Actix)
                                    .map_err(UpstreamRequestError::Http)?
                                    .into(),
                            )
                            .map_err(UpstreamRequestError::Http)
                    },
                );

                let future = upstream
                    .send(request)
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(move |result| {
                        let received = match result {
                            Ok(_) => true,
                            Err(ref e) => e.is_received(),
                        };

                        // Flag that upstream has received the request, which will skip outcome
                        // generation below.
                        is_received.store(received, Ordering::SeqCst);

                        result.map_err(move |error| match error {
                            UpstreamRequestError::RateLimited(upstream_limits) => {
                                let limits = upstream_limits.scope(&scoping.borrow());
                                project.do_send(UpdateRateLimits(limits.clone()));
                                ProcessingError::RateLimited(limits)
                            }
                            other => ProcessingError::SendFailed(other),
                        })
                    })
                    .into_actor(slf);

                Box::new(future) as ResponseActFuture<_, _, _>
            }))
            .timeout(self.config.event_buffer_expiry(), ProcessingError::Timeout)
            .map(|_, _, _| metric!(counter(RelayCounters::EnvelopeAccepted) += 1))
            .map_err(move |error, slf, _| {
                metric!(counter(RelayCounters::EnvelopeRejected) += 1);

                // if we are in capture mode, we stash away the event instead of
                // forwarding it.
                if capture {
                    // XXX: does not work with envelopes without event_id
                    if let Some(event_id) = event_id {
                        relay_log::debug!("capturing failed event {}", event_id);
                        let msg = LogError(&error).to_string();
                        slf.captures.insert(event_id, Err(msg));
                    } else {
                        relay_log::debug!("dropping failed envelope without event");
                    }
                }

                // Do not track outcomes or capture events for non-event envelopes (such as
                // individual attachments)
                if !is_event {
                    return;
                }

                let outcome = error.to_outcome();
                if let Some(Outcome::Invalid(DiscardReason::Internal)) = outcome {
                    // Errors are only logged for what we consider an internal discard reason. These
                    // indicate errors in the infrastructure or implementation bugs. In other cases,
                    // we "expect" errors and log them as debug level.
                    relay_log::error!("error processing event: {}", LogError(&error));
                } else {
                    relay_log::debug!("dropped event: {}", LogError(&error));
                }

                // Do not emit outcomes for requests that have been accepted by the upstream. In
                // such a case, the upstream assumes ownership and logs the outcome, instead. This
                // is irrelevant to processing Relays, since they never send to the upstream.
                if is_received.load(Ordering::SeqCst) {
                    return;
                }

                if let Some(outcome) = outcome {
                    outcome_producer.do_send(TrackOutcome {
                        timestamp: Instant::now(),
                        scoping: *scoping.borrow(),
                        outcome,
                        event_id,
                        remote_addr,
                    })
                }
            })
            .then(move |x, slf, _| {
                metric!(timer(RelayTimers::EnvelopeTotalTime) = start_time.elapsed());
                slf.current_active_events -= 1;
                fut::result(x)
            })
            .drop_guard("process_event");

        Box::new(future)
    }
}

/// Resolves a [`CapturedEnvelope`] by the given `event_id`.
pub struct GetCapturedEnvelope {
    pub event_id: EventId,
}

impl Message for GetCapturedEnvelope {
    type Result = Option<CapturedEnvelope>;
}

impl Handler<GetCapturedEnvelope> for EventManager {
    type Result = Option<CapturedEnvelope>;

    fn handle(
        &mut self,
        message: GetCapturedEnvelope,
        _context: &mut Self::Context,
    ) -> Self::Result {
        self.captures.get(&message.event_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::extractors::RequestMeta;

    use chrono::{DateTime, TimeZone, Utc};

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
        let result =
            EventProcessor::event_from_attachments(&Config::default(), None, Some(item), None);

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
        let result =
            EventProcessor::event_from_attachments(&Config::default(), None, None, Some(item));

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

        let result = EventProcessor::event_from_attachments(
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

        let result = EventProcessor::event_from_attachments(
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

        let result = EventProcessor::event_from_attachments(
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

        let result = EventProcessor::event_from_attachments(
            &Config::default(),
            Some(item1),
            Some(item2),
            Some(item3),
        );

        // regression test to ensure we don't fail parsing an empty file
        result.expect("event_from_attachments");
    }

    #[test]
    fn test_user_report_invalid() {
        let processor = EventProcessor::new(Arc::new(Default::default()));
        let event_id = EventId::new();

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

        let envelope_response = processor
            .process(ProcessEnvelope {
                envelope,
                project_state: Arc::new(ProjectState::allowed()),
                start_time: Instant::now(),
            })
            .unwrap();

        let new_envelope = envelope_response.envelope.unwrap();

        assert_eq!(new_envelope.len(), 1);
        assert_eq!(new_envelope.items().next().unwrap().ty(), ItemType::Event);
    }
}
