//! This module contains the actor that tracks outcomes.
//!
//! Outcomes describe the final "fate" of an envelope item. As such, for every item exactly one
//! outcome must be emitted in the entire ingestion pipeline. Since Relay is only one part in this
//! pipeline, outcomes may not be emitted if the item is accepted.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;
use std::mem;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::SystemService;
use actix_web::http::Method;
use chrono::{DateTime, SecondsFormat, Utc};
#[cfg(feature = "processing")]
use failure::Fail;
#[cfg(feature = "processing")]
use rdkafka::producer::BaseRecord;
use relay_system::{Interface, NoResponse};
use serde::{Deserialize, Serialize};

use relay_common::{DataCategory, ProjectId, UnixTimestamp};
#[cfg(feature = "processing")]
use relay_config::KafkaTopic;
use relay_config::{Config, EmitOutcomes};
use relay_filter::FilterStatKey;
use relay_general::protocol::{ClientReport, DiscardedEvent, EventId};
use relay_log::LogError;
use relay_quotas::{ReasonCode, Scoping};
use relay_sampling::RuleId;
use relay_statsd::metric;
use relay_system::{compat, Addr, FromMessage, Service};

use crate::actors::envelopes::{EnvelopeManager, SendClientReports};
#[cfg(feature = "processing")]
use crate::actors::store::{self, Producer};
use crate::actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay};
use crate::service::REGISTRY;
use crate::statsd::RelayCounters;
use crate::utils::SleepHandle;
use crate::ServerError;

/// Defines the structure of the HTTP outcomes requests
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SendOutcomes {
    #[serde(default)]
    pub outcomes: Vec<TrackRawOutcome>,
}

impl UpstreamQuery for SendOutcomes {
    type Response = SendOutcomesResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/outcomes/")
    }

    fn retry() -> bool {
        true
    }
}

/// Defines the structure of the HTTP outcomes responses for successful requests
#[derive(Debug, Deserialize, Serialize)]
pub struct SendOutcomesResponse {
    // nothing yet, future features will go here
}

/// The numerical identifier of the outcome category (Accepted, Filtered, ...)
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub struct OutcomeId(u8);

impl OutcomeId {
    // This is not an enum because we still want to forward unknown outcome IDs transparently
    const ACCEPTED: OutcomeId = OutcomeId(0);
    const FILTERED: OutcomeId = OutcomeId(1);
    const RATE_LIMITED: OutcomeId = OutcomeId(2);
    const INVALID: OutcomeId = OutcomeId(3);
    const ABUSE: OutcomeId = OutcomeId(4);
    const CLIENT_DISCARD: OutcomeId = OutcomeId(5);
}

trait TrackOutcomeLike {
    /// TODO: Doc
    fn reason(&self) -> Option<Cow<str>>;

    /// TODO: Doc
    fn outcome_id(&self) -> OutcomeId;

    /// TODO: Doc
    fn tag_name(&self) -> &'static str {
        match self.outcome_id() {
            OutcomeId::ACCEPTED => "accepted",
            OutcomeId::FILTERED => "filtered",
            OutcomeId::RATE_LIMITED => "rate_limited",
            OutcomeId::INVALID => "invalid",
            OutcomeId::ABUSE => "abuse",
            OutcomeId::CLIENT_DISCARD => "client_discard",
            _ => "<unknown>",
        }
    }
}

/// Tracks an [`Outcome`] of an Envelope item.
///
/// See the module level documentation for more information.
#[derive(Clone, Debug, Hash)]
pub struct TrackOutcome {
    /// The timespan of the event outcome.
    pub timestamp: DateTime<Utc>,
    /// Scoping of the request.
    pub scoping: Scoping,
    /// The outcome.
    pub outcome: Outcome,
    /// The event id.
    pub event_id: Option<EventId>,
    /// The client ip address.
    pub remote_addr: Option<IpAddr>,
    /// The event's data category.
    pub category: DataCategory,
    /// The number of events or total attachment size in bytes.
    pub quantity: u32,
}

impl TrackOutcomeLike for TrackOutcome {
    fn reason(&self) -> Option<Cow<str>> {
        self.outcome.to_reason()
    }

    fn outcome_id(&self) -> OutcomeId {
        self.outcome.to_outcome_id()
    }
}

impl Interface for TrackOutcome {}

impl FromMessage<Self> for TrackOutcome {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

/// Defines the possible outcomes from processing an event.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    // /// The event has been accepted and handled completely.
    // ///
    // /// This is never emitted by Relay as the event may be discarded by the processing pipeline
    // /// after Relay. Only the `save_event` task in Sentry finally accepts an event.
    // #[allow(dead_code)]
    // Accepted,
    /// The event has been filtered due to a configured filter.
    Filtered(FilterStatKey),

    /// The event has been filtered by a Sampling Rule
    FilteredSampling(RuleId),

    /// The event has been rate limited.
    RateLimited(Option<ReasonCode>),

    /// The event has been discarded because of invalid data.
    Invalid(DiscardReason),

    /// Reserved but unused in Relay.
    #[allow(dead_code)]
    Abuse,

    /// The event has already been discarded on the client side.
    ClientDiscard(String),
}

impl Outcome {
    /// Returns the raw numeric value of this outcome for the JSON and Kafka schema.
    fn to_outcome_id(&self) -> OutcomeId {
        match self {
            Outcome::Filtered(_) | Outcome::FilteredSampling(_) => OutcomeId::FILTERED,
            Outcome::RateLimited(_) => OutcomeId::RATE_LIMITED,
            Outcome::Invalid(_) => OutcomeId::INVALID,
            Outcome::Abuse => OutcomeId::ABUSE,
            Outcome::ClientDiscard(_) => OutcomeId::CLIENT_DISCARD,
        }
    }

    /// Returns the `reason` code field of this outcome.
    fn to_reason(&self) -> Option<Cow<str>> {
        match self {
            Outcome::Invalid(discard_reason) => Some(Cow::Borrowed(discard_reason.name())),
            Outcome::Filtered(filter_key) => Some(Cow::Borrowed(filter_key.name())),
            Outcome::FilteredSampling(rule_id) => Some(Cow::Owned(format!("Sampled:{}", rule_id))),
            //TODO can we do better ? (not re copying the string )
            Outcome::RateLimited(code_opt) => code_opt
                .as_ref()
                .map(|code| Cow::Owned(code.as_str().into())),
            Outcome::ClientDiscard(ref discard_reason) => Some(Cow::Borrowed(discard_reason)),
            Outcome::Abuse => None,
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Filtered(key) => write!(f, "filtered by {}", key),
            Outcome::FilteredSampling(rule) => write!(f, "sampling rule {}", rule),
            Outcome::RateLimited(None) => write!(f, "rate limited"),
            Outcome::RateLimited(Some(reason)) => write!(f, "rate limited with reason {}", reason),
            Outcome::Invalid(DiscardReason::Internal) => write!(f, "internal error"),
            Outcome::Invalid(reason) => write!(f, "invalid data ({})", reason),
            Outcome::Abuse => write!(f, "abuse limit reached"),
            Outcome::ClientDiscard(reason) => write!(f, "discarded by client ({})", reason),
        }
    }
}

/// Reason for a discarded invalid event.
///
/// Used in `Outcome::Invalid`. Synchronize overlap with Sentry.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub enum DiscardReason {
    /// (Post Processing) An event with the same id has already been processed for this project.
    /// Sentry does not allow duplicate events and only stores the first one.
    Duplicate,

    /// (Relay) There was no valid project id in the request or the required project does not exist.
    ProjectId,

    /// (Relay) The protocol version sent by the SDK is not supported and parts of the payload may
    /// be invalid.
    AuthVersion,

    /// (Legacy) The SDK did not send a client identifier.
    ///
    /// In Relay, this is no longer required.
    AuthClient,

    /// (Relay) The store request was missing an event payload.
    NoData,

    /// (Relay) The envelope contains no items.
    EmptyEnvelope,

    /// (Relay) The event payload exceeds the maximum size limit for the respective endpoint.
    TooLarge,

    /// (Legacy) A store request was received with an invalid method.
    ///
    /// This outcome is no longer emitted by Relay, as HTTP method validation occurs before an event
    /// id or project id are extracted for a request.
    DisallowedMethod,

    /// (Relay) The content type for a specific endpoint was not allowed.
    ///
    /// While the standard store endpoint allows all content types, other endpoints may have
    /// stricter requirements.
    ContentType,

    /// (Legacy) The project id in the URL does not match the one specified for the public key.
    ///
    /// This outcome is no longer emitted by Relay. Instead, Relay will emit a standard `ProjectId`
    /// since it resolves the project first, and then checks for the valid project key.
    MultiProjectId,

    /// (Relay) A minidump file was missing for the minidump endpoint.
    MissingMinidumpUpload,

    /// (Relay) The file submitted as minidump is not a valid minidump file.
    InvalidMinidump,

    /// (Relay) The security report was not recognized due to missing data.
    SecurityReportType,

    /// (Relay) The security report did not pass schema validation.
    SecurityReport,

    /// (Relay) The request origin is not allowed for the project.
    Cors,

    /// (Relay) Reading or decoding the payload from the socket failed for any reason.
    Payload,

    /// (Relay) Parsing the event JSON payload failed due to a syntax error.
    InvalidJson,

    /// (Relay) Parsing the event msgpack payload failed due to a syntax error.
    InvalidMsgpack,

    /// (Relay) Parsing a multipart form-data request failed.
    InvalidMultipart,

    /// (Relay) The event is parseable but semantically invalid. This should only happen with
    /// transaction events.
    InvalidTransaction,

    /// (Relay) Parsing an event envelope failed (likely missing a required header).
    InvalidEnvelope,

    /// (Relay) The payload had an invalid compression stream.
    InvalidCompression,

    /// (Relay) A project state returned by the upstream could not be parsed.
    ProjectState,

    /// (Relay) An envelope was submitted with two items that need to be unique.
    DuplicateItem,

    /// (Relay) An event envelope was submitted but no payload could be extracted.
    NoEventPayload,

    /// (All) An error in Relay caused event ingestion to fail. This is the catch-all and usually
    /// indicates bugs in Relay, rather than an expected failure.
    Internal,

    /// (Relay) Symbolic failed to extract an Unreal Crash report from a request sent to the
    /// Unreal endpoint
    ProcessUnreal,

    /// (Relay) The envelope, which contained only a transaction, was discarded by the
    /// dynamic sampling rules.
    TransactionSampled,

    /// (Relay) We failed to parse the profile so we discard the profile.
    ProcessProfile,

    /// (Relay) The profile is parseable but semantically invalid. This could happen if
    /// profiles lack sufficient samples.
    InvalidProfile,

    // (Relay) We failed to parse the replay so we discard it.
    InvalidReplayEvent,
}

impl DiscardReason {
    pub fn name(self) -> &'static str {
        match self {
            DiscardReason::Duplicate => "duplicate",
            DiscardReason::ProjectId => "project_id",
            DiscardReason::AuthVersion => "auth_version",
            DiscardReason::AuthClient => "auth_client",
            DiscardReason::NoData => "no_data",
            DiscardReason::TooLarge => "too_large",
            DiscardReason::DisallowedMethod => "disallowed_method",
            DiscardReason::ContentType => "content_type",
            DiscardReason::MultiProjectId => "multi_project_id",
            DiscardReason::MissingMinidumpUpload => "missing_minidump_upload",
            DiscardReason::InvalidMinidump => "invalid_minidump",
            DiscardReason::SecurityReportType => "security_report_type",
            DiscardReason::SecurityReport => "security_report",
            DiscardReason::Cors => "cors",
            DiscardReason::ProcessUnreal => "process_unreal",
            DiscardReason::ProcessProfile => "process_profile",

            // Relay specific reasons (not present in Sentry)
            DiscardReason::Payload => "payload",
            DiscardReason::InvalidJson => "invalid_json",
            DiscardReason::InvalidMultipart => "invalid_multipart",
            DiscardReason::InvalidMsgpack => "invalid_msgpack",
            DiscardReason::InvalidTransaction => "invalid_transaction",
            DiscardReason::InvalidEnvelope => "invalid_envelope",
            DiscardReason::InvalidCompression => "invalid_compression",
            DiscardReason::ProjectState => "project_state",
            DiscardReason::DuplicateItem => "duplicate_item",
            DiscardReason::NoEventPayload => "no_event_payload",
            DiscardReason::Internal => "internal",
            DiscardReason::TransactionSampled => "transaction_sampled",
            DiscardReason::EmptyEnvelope => "empty_envelope",
            DiscardReason::InvalidProfile => "invalid_profile",
            DiscardReason::InvalidReplayEvent => "invalid_replay",
        }
    }
}

impl fmt::Display for DiscardReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Raw representation of an outcome for serialization.
///
/// The JSON serialization of this structure is placed on the Kafka topic and used in the HTTP
/// endpoints. To create a new outcome, use [`TrackOutcome`], instead.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackRawOutcome {
    /// The timespan of the event outcome.
    timestamp: String,
    /// Organization id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    org_id: Option<u64>,
    /// Project id.
    project_id: ProjectId,
    /// The DSN project key id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_id: Option<u64>,
    /// The outcome.
    outcome: OutcomeId,
    /// Reason for the outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// The event id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_id: Option<EventId>,
    /// The client ip address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remote_addr: Option<String>,
    /// The source of the outcome (which Relay sent it)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    /// The event's data category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<u8>,
    /// The number of events or total attachment size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity: Option<u32>,
}

impl TrackRawOutcome {
    fn from_outcome(msg: TrackOutcome, config: &Config) -> Self {
        let reason = msg.outcome.to_reason().map(|reason| reason.to_string());

        // convert to a RFC 3339 formatted date with the shape YYYY-MM-DDTHH:MM:SS.mmmmmmZ
        // e.g. something like: "2019-09-29T09:46:40.123456Z"
        let timestamp = msg.timestamp.to_rfc3339_opts(SecondsFormat::Micros, true);

        let org_id = match msg.scoping.organization_id {
            0 => None,
            id => Some(id),
        };

        // since TrackOutcome objects come only from this Relay (and not any downstream
        // Relays), set the source to whatever our current outcome source is.
        let source = config.outcome_source().map(str::to_owned);

        TrackRawOutcome {
            timestamp,
            org_id,
            project_id: msg.scoping.project_id,
            key_id: msg.scoping.key_id,
            outcome: msg.outcome.to_outcome_id(),
            reason,
            event_id: msg.event_id,
            remote_addr: msg.remote_addr.map(|addr| addr.to_string()),
            source,
            category: msg.category.value(),
            quantity: Some(msg.quantity),
        }
    }

    #[cfg(feature = "processing")]
    fn is_billing(&self) -> bool {
        matches!(self.outcome, OutcomeId::ACCEPTED | OutcomeId::RATE_LIMITED)
    }
}

impl TrackOutcomeLike for TrackRawOutcome {
    fn reason(&self) -> Option<Cow<str>> {
        self.reason.as_ref().map(|s| s.into())
    }

    fn outcome_id(&self) -> OutcomeId {
        self.outcome
    }
}

impl Interface for TrackRawOutcome {}

impl FromMessage<Self> for TrackRawOutcome {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "processing", derive(Fail))]
pub enum OutcomeError {
    #[fail(display = "failed to send kafka message")]
    #[cfg(feature = "processing")]
    SendFailed(rdkafka::error::KafkaError),
    #[fail(display = "json serialization error")]
    #[cfg(feature = "processing")]
    SerializationError(serde_json::Error),
}

/// Outcome producer backend via HTTP as [`TrackRawOutcome`].
struct HttpOutcomeProducer {
    config: Arc<Config>,
    unsent_outcomes: Vec<TrackRawOutcome>,
    pending_flush_handle: SleepHandle,
}

impl HttpOutcomeProducer {
    pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
        Ok(Self {
            config,
            unsent_outcomes: Vec::new(),
            pending_flush_handle: SleepHandle::idle(),
        })
    }

    fn send_batch(&mut self) {
        self.pending_flush_handle.reset();

        if self.unsent_outcomes.is_empty() {
            relay_log::warn!("unexpected send_batch scheduled with no outcomes to send.");
            return;
        } else {
            relay_log::trace!(
                "sending outcome batch of size:{}",
                self.unsent_outcomes.len()
            );
        }

        let request = SendOutcomes {
            outcomes: mem::take(&mut self.unsent_outcomes),
        };

        tokio::spawn(async move {
            match compat::send(UpstreamRelay::from_registry(), SendQuery(request)).await {
                Ok(_) => relay_log::trace!("outcome batch sent."),
                Err(error) => {
                    relay_log::error!("outcome batch sending failed with: {}", LogError(&error))
                }
            }
        });
    }

    fn handle_message(&mut self, message: TrackRawOutcome) {
        relay_log::trace!("Batching outcome");
        self.unsent_outcomes.push(message);

        if self.unsent_outcomes.len() >= self.config.outcome_batch_size() {
            self.send_batch();
        } else if self.pending_flush_handle.is_idle() {
            self.pending_flush_handle
                .set(self.config.outcome_batch_interval());
        }
    }
}

impl Service for HttpOutcomeProducer {
    type Interface = TrackRawOutcome;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = rx.recv() => self.handle_message(message),
                    () = &mut self.pending_flush_handle => self.send_batch(),
                    else => break,
                }
            }
        });
    }
}

/// Outcome producer backend via HTTP as [`ClientReport`].
struct ClientReportOutcomeProducer {
    flush_interval: Duration,
    unsent_reports: BTreeMap<Scoping, Vec<ClientReport>>,
    flush_handle: SleepHandle,
}

impl ClientReportOutcomeProducer {
    fn create(config: &Config) -> Self {
        Self {
            // Use same batch interval as outcome aggregator
            flush_interval: Duration::from_secs(config.outcome_aggregator().flush_interval),
            unsent_reports: BTreeMap::new(),
            flush_handle: SleepHandle::idle(),
        }
    }

    fn flush(&mut self) {
        relay_log::trace!("Flushing client reports");
        self.flush_handle.reset();

        let unsent_reports = mem::take(&mut self.unsent_reports);
        let envelope_manager = EnvelopeManager::from_registry();
        for (scoping, client_reports) in unsent_reports.into_iter() {
            envelope_manager.do_send(SendClientReports {
                client_reports,
                scoping,
            });
        }
    }

    fn handle_message(&mut self, msg: TrackOutcome) {
        let mut client_report = ClientReport {
            timestamp: Some(UnixTimestamp::from_secs(
                msg.timestamp.timestamp().try_into().unwrap_or(0),
            )),
            ..Default::default()
        };

        // The outcome type determines what field to place the outcome in:
        let discarded_events = match msg.outcome {
            Outcome::Filtered(_) => &mut client_report.filtered_events,
            Outcome::FilteredSampling(_) => &mut client_report.filtered_sampling_events,
            Outcome::RateLimited(_) => &mut client_report.rate_limited_events,
            _ => {
                // Cannot convert this outcome to a client report.
                return;
            }
        };

        // Now that we know where to put it, let's create a DiscardedEvent
        let discarded_event = DiscardedEvent {
            reason: msg.outcome.to_reason().unwrap_or_default().to_string(),
            category: msg.category,
            quantity: msg.quantity,
        };
        discarded_events.push(discarded_event);

        self.unsent_reports
            .entry(msg.scoping)
            .or_default()
            .push(client_report);

        if self.flush_interval == Duration::ZERO {
            // Flush immediately. Useful for integration tests.
            self.flush();
        } else if self.flush_handle.is_idle() {
            self.flush_handle.set(self.flush_interval);
        }
    }
}

impl Service for ClientReportOutcomeProducer {
    type Interface = TrackOutcome;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = rx.recv() => self.handle_message(message),
                    () = &mut self.flush_handle => self.flush(),
                    else => break,
                }
            }
        });
    }
}

/// Outcomes producer backend for Kafka.
///
/// Internally, this type creates at least one Kafka producer for the cluster of the `outcomes`
/// topic assignment. If the `outcomes-billing` topic specifies a different cluster, it creates a
/// second producer.
///
/// Use `KafkaOutcomesProducer::billing` for outcomes that are critical to billing (see
/// `is_billing`), otherwise use `KafkaOutcomesProducer::default`. This will return the correct
/// producer instance internally.
#[cfg(feature = "processing")]
struct KafkaOutcomesProducer {
    default: Producer,
    billing: Producer,
}

#[cfg(feature = "processing")]
impl KafkaOutcomesProducer {
    /// Creates and connects the Kafka producers.
    ///
    /// If the given Kafka configuration parameters are invalid, or an error happens during
    /// connecting during the broker, an error is returned.
    pub fn create(config: &Config) -> Result<Self, ServerError> {
        let mut reused_producers = BTreeMap::new();
        let producers = KafkaOutcomesProducer {
            default: store::make_producer(config, &mut reused_producers, KafkaTopic::Outcomes)?,
            billing: store::make_producer(
                config,
                &mut reused_producers,
                KafkaTopic::OutcomesBilling,
            )?,
        };

        Ok(producers)
    }

    /// Returns the producer for default outcomes.
    pub fn default(&self) -> &Producer {
        &self.default
    }

    /// Returns the producer for billing outcomes.
    ///
    /// Note that this may return the same producer instance as [`default`](Self::default) depending
    /// on the configuration.
    pub fn billing(&self) -> &Producer {
        &self.billing
    }
}

enum ProducerInner {
    AsClientReports(Addr<TrackOutcome>),
    AsHttpOutcomes(Addr<TrackRawOutcome>),
    #[cfg(feature = "processing")]
    AsKafkaOutcomes(KafkaOutcomesProducer),
    Disabled,
}

/// Produces [`Outcome`]s to a configurable backend.
///
/// There are two variants based on the source of outcomes. When logging outcomes, [`TrackOutcome`]
/// should be heavily preferred. When processing outcomes from endpoints, [`TrackRawOutcome`] can be
/// used instead.
///
/// The backend is configured through the `outcomes` configuration object and can be:
///
///  1. Kafka in processing mode
///  2. Upstream Relay via batch HTTP request in point-of-presence configuration
///  3. Upstream Relay via client reports in external configuration
///  4. (default) Disabled
#[derive(Debug)]
pub enum OutcomeProducer {
    TrackOutcome(TrackOutcome),
    TrackRawOutcome(TrackRawOutcome),
}

impl Interface for OutcomeProducer {}

impl FromMessage<TrackOutcome> for OutcomeProducer {
    type Response = NoResponse;

    fn from_message(message: TrackOutcome, _: ()) -> Self {
        Self::TrackOutcome(message)
    }
}

impl FromMessage<TrackRawOutcome> for OutcomeProducer {
    type Response = NoResponse;

    fn from_message(message: TrackRawOutcome, _: ()) -> Self {
        Self::TrackRawOutcome(message)
    }
}

/// Service implementing the [`OutcomeProducer`] interface.
pub struct OutcomeProducerService {
    config: Arc<Config>,
    producer: ProducerInner,
}

impl OutcomeProducerService {
    pub fn from_registry() -> Addr<OutcomeProducer> {
        REGISTRY.get().unwrap().outcome_producer.clone()
    }

    pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
        let producer = match config.emit_outcomes() {
            EmitOutcomes::AsOutcomes => {
                // We emit outcomes as raw outcomes, and accept raw outcomes emitted by downstream
                // relays.
                if config.processing_enabled() {
                    #[cfg(feature = "processing")]
                    {
                        ProducerInner::AsKafkaOutcomes(KafkaOutcomesProducer::create(&config)?)
                    }

                    #[cfg(not(feature = "processing"))]
                    unreachable!("config parsing should have failed")
                } else {
                    relay_log::info!("Configured to emit outcomes via http");
                    ProducerInner::AsHttpOutcomes(
                        HttpOutcomeProducer::create(Arc::clone(&config))?.start(),
                    )
                }
            }
            EmitOutcomes::AsClientReports => {
                // We emit outcomes as client reports, and we do not
                // accept any raw outcomes
                relay_log::info!("Configured to emit outcomes as client reports");
                ProducerInner::AsClientReports(ClientReportOutcomeProducer::create(&config).start())
            }
            EmitOutcomes::None => {
                relay_log::info!("Configured to drop all outcomes");
                ProducerInner::Disabled
            }
        };

        Ok(Self { config, producer })
    }

    fn handle_message(&mut self, message: OutcomeProducer) {
        match message {
            OutcomeProducer::TrackOutcome(msg) => self.handle_track_outcome(msg),
            OutcomeProducer::TrackRawOutcome(msg) => self.handle_track_raw_outcome(msg),
        }
    }

    #[cfg(feature = "processing")]
    fn send_kafka_message(
        &self,
        producer: &KafkaOutcomesProducer,
        organization_id: u64,
        message: TrackRawOutcome,
    ) -> Result<(), OutcomeError> {
        relay_log::trace!("Tracking kafka outcome: {:?}", message);

        let payload = serde_json::to_string(&message).map_err(OutcomeError::SerializationError)?;

        // At the moment, we support outcomes with optional EventId.
        // Here we create a fake EventId, when we don't have the real one, so that we can
        // create a kafka message key that spreads the events nicely over all the
        // kafka consumer groups.
        let key = message.event_id.unwrap_or_else(EventId::new).0;

        // Dispatch to the correct topic and cluster based on the kind of outcome.
        let (_topic, producer) = if message.is_billing() {
            (KafkaTopic::OutcomesBilling, producer.billing())
        } else {
            (KafkaTopic::Outcomes, producer.default())
        };

        let result = match producer {
            Producer::Single {
                topic_name,
                producer,
            } => {
                let record = BaseRecord::to(topic_name)
                    .payload(&payload)
                    .key(key.as_bytes().as_ref());

                producer.send(record)
            }

            Producer::Sharded(sharded) => {
                let (topic_name, producer) = sharded.get_producer(organization_id);
                let record = BaseRecord::to(topic_name)
                    .payload(&payload)
                    .key(key.as_bytes().as_ref());
                producer.send(record)
            }
        };

        match result {
            Ok(_) => Ok(()),
            Err((kafka_error, _message)) => Err(OutcomeError::SendFailed(kafka_error)),
        }
    }

    fn send_outcome_metric(message: &impl TrackOutcomeLike, to: &'static str) {
        metric!(
            counter(RelayCounters::Outcomes) += 1,
            reason = message.reason().as_deref().unwrap_or(""),
            outcome = message.tag_name(),
            to = to,
        );
    }

    fn handle_track_outcome(&mut self, message: TrackOutcome) {
        match &self.producer {
            #[cfg(feature = "processing")]
            ProducerInner::AsKafkaOutcomes(ref kafka_producer) => {
                Self::send_outcome_metric(&message, "kafka");
                let organization_id = message.scoping.organization_id;
                let raw_message = TrackRawOutcome::from_outcome(message, &self.config);
                if let Err(error) =
                    self.send_kafka_message(kafka_producer, organization_id, raw_message)
                {
                    relay_log::error!("failed to produce outcome: {}", LogError(&error));
                }
            }
            ProducerInner::AsClientReports(ref producer) => {
                Self::send_outcome_metric(&message, "client_report");
                producer.send(message);
            }
            ProducerInner::AsHttpOutcomes(ref producer) => {
                Self::send_outcome_metric(&message, "http");
                producer.send(TrackRawOutcome::from_outcome(message, &self.config));
            }
            ProducerInner::Disabled => (),
        }
    }

    fn handle_track_raw_outcome(&mut self, message: TrackRawOutcome) {
        match &self.producer {
            #[cfg(feature = "processing")]
            ProducerInner::AsKafkaOutcomes(ref kafka_producer) => {
                Self::send_outcome_metric(&message, "kafka");
                let sharding_id = message.org_id.unwrap_or_else(|| message.project_id.value());
                if let Err(error) = self.send_kafka_message(kafka_producer, sharding_id, message) {
                    relay_log::error!("failed to produce outcome: {}", LogError(&error));
                }
            }
            ProducerInner::AsHttpOutcomes(ref producer) => {
                Self::send_outcome_metric(&message, "http");
                producer.send(message);
            }
            ProducerInner::AsClientReports(_) => (),
            ProducerInner::Disabled => (),
        }
    }
}

impl Service for OutcomeProducerService {
    type Interface = OutcomeProducer;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("OutcomeProducer started.");
            while let Some(message) = rx.recv().await {
                self.handle_message(message);
            }
            relay_log::info!("OutcomeProducer stopped.");
        });
    }
}
