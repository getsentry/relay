//! This module contains the actor that tracks event outcomes.
//!
//! Outcomes describe the final "fate" of an event. As such, for every event exactly one outcome
//! must be emitted in the entire ingestion pipeline. Since Relay is only one part in this pipeline,
//! outcomes may not be emitted if the event is accepted.

use std::borrow::Cow;
use std::mem;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::http::Method;
use chrono::SecondsFormat;
use futures::future::Future;
use serde::{Deserialize, Serialize};

use relay_common::ProjectId;
use relay_config::Config;
use relay_filter::FilterStatKey;
use relay_general::protocol::EventId;
use relay_log::LogError;
use relay_quotas::{ReasonCode, Scoping};

use crate::actors::upstream::SendQuery;
use crate::actors::upstream::{UpstreamQuery, UpstreamRelay};
use crate::ServerError;

// Choose the outcome module implementation (either processing or non-processing).
// Processing outcome implementation
#[cfg(feature = "processing")]
pub use self::processing::*;

#[cfg(feature = "processing")]
pub type OutcomeProducer = processing::ProcessingOutcomeProducer;
#[cfg(not(feature = "processing"))]
pub type OutcomeProducer = HttpOutcomeProducer;

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

/// Tracks an outcome of an event.
///
/// See the module level documentation for more information.
#[derive(Clone, Debug)]
pub struct TrackOutcome {
    /// The timespan of the event outcome.
    pub timestamp: Instant,
    /// Scoping of the request.
    pub scoping: Scoping,
    /// The outcome.
    pub outcome: Outcome,
    /// The event id.
    pub event_id: Option<EventId>,
    /// The client ip address.
    pub remote_addr: Option<IpAddr>,
}

impl Message for TrackOutcome {
    type Result = Result<(), OutcomeError>;
}

/// Defines the possible outcomes from processing an event.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// The event has been accepted.
    ///
    /// This is never emitted by Relay as the event may be discarded by the processing pipeline
    /// after Relay. Only the `save_event` task in Sentry finally accepts an event.
    #[allow(dead_code)]
    Accepted,

    /// The event has been filtered due to a configured filter.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    Filtered(FilterStatKey),

    /// The event has been rate limited.
    RateLimited(Option<ReasonCode>),

    /// The event has been discarded because of invalid data.
    Invalid(DiscardReason),

    /// Reserved but unused in Sentry.
    #[allow(dead_code)]
    Abuse,
}

impl Outcome {
    fn to_outcome_id(&self) -> u8 {
        match self {
            Outcome::Accepted => 0,
            Outcome::Filtered(_) => 1,
            Outcome::RateLimited(_) => 2,
            Outcome::Invalid(_) => 3,
            Outcome::Abuse => 4,
        }
    }

    fn to_reason(&self) -> Option<&str> {
        match self {
            Outcome::Accepted => None,
            Outcome::Invalid(discard_reason) => Some(discard_reason.name()),
            Outcome::Filtered(filter_key) => Some(filter_key.name()),
            Outcome::RateLimited(code_opt) => code_opt.as_ref().map(|code| code.as_str()),
            Outcome::Abuse => None,
        }
    }
}

/// Reason for a discarded invalid event.
///
/// Used in `Outcome::Invalid`. Synchronize overlap with Sentry.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub enum DiscardReason {
    /// [Post Processing] An event with the same id has already been processed for this project.
    /// Sentry does not allow duplicate events and only stores the first one.
    Duplicate,

    /// [Relay] There was no valid project id in the request or the required project does not exist.
    ProjectId,

    /// [Relay] The protocol version sent by the SDK is not supported and parts of the payload may
    /// be invalid.
    AuthVersion,

    /// [Legacy] The SDK did not send a client identifier.
    ///
    /// In Relay, this is no longer required.
    AuthClient,

    /// [Relay] The store request was missing an event payload.
    NoData,

    /// [Relay] The event payload exceeds the maximum size limit for the respective endpoint.
    TooLarge,

    /// [Legacy] A store request was received with an invalid method.
    ///
    /// This outcome is no longer emitted by Relay, as HTTP method validation occurs before an event
    /// id or project id are extracted for a request.
    DisallowedMethod,

    /// [Relay] The content type for a specific endpoint was not allowed.
    ///
    /// While the standard store endpoint allows all content types, other endpoints may have
    /// stricter requirements.
    ContentType,

    /// [Legacy] The project id in the URL does not match the one specified for the public key.
    ///
    /// This outcome is no longer emitted by Relay. Instead, Relay will emit a standard `ProjectId`
    /// since it resolves the project first, and then checks for the valid project key.
    MultiProjectId,

    /// [Relay] A minidump file was missing for the minidump endpoint.
    MissingMinidumpUpload,

    /// [Relay] The file submitted as minidump is not a valid minidump file.
    InvalidMinidump,

    /// [Relay] The security report was not recognized due to missing data.
    SecurityReportType,

    /// [Relay] The security report did not pass schema validation.
    SecurityReport,

    /// [Relay] The request origin is not allowed for the project.
    Cors,

    /// [Relay] Reading or decoding the payload from the socket failed for any reason.
    Payload,

    /// [Relay] Parsing the event JSON payload failed due to a syntax error.
    InvalidJson,

    /// [Relay] Parsing the event msgpack payload failed due to a syntax error.
    InvalidMsgpack,

    /// [Relay] Parsing a multipart form-data request failed.
    InvalidMultipart,

    /// [Relay] The event is parseable but semantically invalid. This should only happen with
    /// transaction events.
    InvalidTransaction,

    /// [Relay] Parsing an event envelope failed (likely missing a required header).
    InvalidEnvelope,

    /// [Relay] A project state returned by the upstream could not be parsed.
    ProjectState,

    /// [Relay] An envelope was submitted with two items that need to be unique.
    DuplicateItem,

    /// [Relay] An event envelope was submitted but no payload could be extracted.
    NoEventPayload,

    /// [All] An error in Relay caused event ingestion to fail. This is the catch-all and usually
    /// indicates bugs in Relay, rather than an expected failure.
    Internal,

    /// [Relay] Symbolic failed to extract an Unreal Crash report from a request sent to the
    /// Unreal endpoint
    ProcessUnreal,

    /// [Relay] The envelope, which contained only a transaction, was discarded by the
    /// dynamic sampling rules.
    TransactionSampled,

    /// [Relay] The envelope, which contained an event, was discarded by the
    /// dynamic sampling rules.
    EventSampled,
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

            // Relay specific reasons (not present in Sentry)
            DiscardReason::Payload => "payload",
            DiscardReason::InvalidJson => "invalid_json",
            DiscardReason::InvalidMultipart => "invalid_multipart",
            DiscardReason::InvalidMsgpack => "invalid_msgpack",
            DiscardReason::InvalidTransaction => "invalid_transaction",
            DiscardReason::InvalidEnvelope => "invalid_envelope",
            DiscardReason::ProjectState => "project_state",
            DiscardReason::DuplicateItem => "duplicate_item",
            DiscardReason::NoEventPayload => "no_event_payload",
            DiscardReason::Internal => "internal",
            DiscardReason::TransactionSampled => "transaction_sampled",
            DiscardReason::EventSampled => "event_sampled",
        }
    }
}

/// The outcome message is serialized as json and placed on the Kafka topic or in
/// the http using TrackRawOutcome
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
    outcome: u8,
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
}

impl TrackRawOutcome {
    fn from_outcome(msg: TrackOutcome, config: &Config) -> Self {
        let reason = match msg.outcome.to_reason() {
            None => None,
            Some(reason) => Some(reason.to_string()),
        };

        let date_time = relay_common::instant_to_date_time(msg.timestamp);

        // convert to a RFC 3339 formatted date with the shape YYYY-MM-DDTHH:MM:SS.mmmmmmZ
        // e.g. something like: "2019-09-29T09:46:40.123456Z"
        let timestamp = date_time.to_rfc3339_opts(SecondsFormat::Micros, true);

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
        }
    }
}

impl Message for TrackRawOutcome {
    type Result = Result<(), OutcomeError>;
}

/// This is the implementation that uses kafka queues and does stuff
#[cfg(feature = "processing")]
mod processing {
    use super::*;

    use failure::{Fail, ResultExt};
    use rdkafka::error::KafkaError;
    use rdkafka::producer::BaseRecord;
    use rdkafka::ClientConfig;
    use serde_json::Error as SerdeSerializationError;

    use relay_common::metric;
    use relay_config::KafkaTopic;

    use crate::metrics::RelayCounters;
    use crate::service::ServerErrorKind;
    use crate::utils::{CaptureErrorContext, ThreadedProducer};

    #[derive(Fail, Debug)]
    pub enum OutcomeError {
        #[fail(display = "failed to send kafka message")]
        SendFailed(KafkaError),
        #[fail(display = "json serialization error")]
        SerializationError(SerdeSerializationError),
    }

    impl TrackRawOutcome {
        fn tag_name(&self) -> &'static str {
            match self.outcome {
                0 => "accepted",
                1 => "filtered",
                2 => "rate_limited",
                3 => "invalid",
                4 => "abuse",
                _ => "<unknown>",
            }
        }
    }

    pub struct ProcessingOutcomeProducer {
        config: Arc<Config>,
        producer: Option<ThreadedProducer>,
        http_producer: Option<Addr<HttpOutcomeProducer>>,
    }

    impl ProcessingOutcomeProducer {
        pub fn create(
            config: Arc<Config>,
            upstream: Addr<UpstreamRelay>,
        ) -> Result<Self, ServerError> {
            let (future_producer, http_producer) = if config.processing_enabled() {
                let mut client_config = ClientConfig::new();
                for config_p in config.kafka_config() {
                    client_config.set(config_p.name.as_str(), config_p.value.as_str());
                }
                let future_producer = client_config
                    .create_with_context(CaptureErrorContext)
                    .context(ServerErrorKind::KafkaError)?;
                (Some(future_producer), None)
            } else {
                let http_producer = HttpOutcomeProducer::create(config.clone(), upstream)
                    .map(|producer| producer.start())
                    .map_err(|error| {
                        relay_log::error!("Failed to start http producer: {}", LogError(&error));
                        error
                    })
                    .ok();
                (None, http_producer)
            };

            Ok(Self {
                config,
                producer: future_producer,
                http_producer,
            })
        }

        fn send_kafka_message(&self, message: TrackRawOutcome) -> Result<(), OutcomeError> {
            relay_log::trace!("Tracking kafka outcome: {:?}", message);

            let producer = match self.producer {
                Some(ref producer) => producer,
                None => return Ok(()),
            };

            let payload =
                serde_json::to_string(&message).map_err(OutcomeError::SerializationError)?;

            metric!(
                counter(RelayCounters::Outcomes) += 1,
                reason = message.reason.as_deref().unwrap_or(""),
                outcome = message.tag_name(),
                to = "kafka",
            );

            // At the moment, we support outcomes with optional EventId.
            // Here we create a fake EventId, when we don't have the real one, so that we can
            // create a kafka message key that spreads the events nicely over all the
            // kafka consumer groups.
            let key = message.event_id.unwrap_or_else(EventId::new).0;

            let record = BaseRecord::to(self.config.kafka_topic_name(KafkaTopic::Outcomes))
                .payload(&payload)
                .key(key.as_bytes().as_ref());

            match producer.send(record) {
                Ok(_) => Ok(()),
                Err((kafka_error, _message)) => Err(OutcomeError::SendFailed(kafka_error)),
            }
        }

        fn send_http_message(&self, message: TrackRawOutcome) {
            relay_log::trace!("Tracking http outcome: {:?}", message);

            let producer = match self.http_producer {
                Some(ref producer) => producer,
                None => {
                    relay_log::error!("send_http_message called with invalid http_producer");
                    return;
                }
            };

            metric!(
                counter(RelayCounters::Outcomes) += 1,
                reason = message.reason.as_deref().unwrap_or(""),
                outcome = message.tag_name(),
                to = "http",
            );

            producer.do_send(message);
        }
    }

    impl Actor for ProcessingOutcomeProducer {
        type Context = Context<Self>;

        fn started(&mut self, context: &mut Self::Context) {
            // Set the mailbox size to the size of the event buffer. This is a rough estimate but
            // should ensure that we're not dropping outcomes unintentionally.
            let mailbox_size = self.config.event_buffer_size() as usize;
            context.set_mailbox_capacity(mailbox_size);

            relay_log::info!("OutcomeProducer started.");
        }

        fn stopped(&mut self, _ctx: &mut Self::Context) {
            relay_log::info!("OutcomeProducer stopped.");
        }
    }

    impl Handler<TrackOutcome> for ProcessingOutcomeProducer {
        type Result = Result<(), OutcomeError>;

        fn handle(&mut self, message: TrackOutcome, _ctx: &mut Self::Context) -> Self::Result {
            self.handle(TrackRawOutcome::from_outcome(message, &self.config), _ctx)
        }
    }

    impl Handler<TrackRawOutcome> for ProcessingOutcomeProducer {
        type Result = Result<(), OutcomeError>;
        fn handle(&mut self, message: TrackRawOutcome, _ctx: &mut Self::Context) -> Self::Result {
            relay_log::trace!("handling outcome");
            if self.config.processing_enabled() {
                self.send_kafka_message(message)
            } else if self.config.emit_outcomes() {
                self.send_http_message(message);
                Ok(())
            } else {
                Ok(()) // processing not enabled and emit_outcomes disabled
            }
        }
    }
}

/// This is the Outcome processing implementation for Relays that are compiled without processing
/// There is no access to kafka, we can only send outcomes to the upstream Relay
#[cfg(not(feature = "processing"))]
#[derive(Debug)]
pub enum OutcomeError {}

pub struct HttpOutcomeProducer {
    pub(super) config: Arc<Config>,
    pub(super) upstream: Addr<UpstreamRelay>,
    pub(super) unsent_outcomes: Vec<TrackRawOutcome>,
    pub(super) pending_flush_handle: Option<SpawnHandle>,
}

impl HttpOutcomeProducer {
    pub fn create(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Result<Self, ServerError> {
        Ok(Self {
            config,
            upstream,
            unsent_outcomes: Vec::new(),
            pending_flush_handle: None,
        })
    }
}

impl HttpOutcomeProducer {
    fn send_batch(&mut self, context: &mut Context<Self>) {
        //the future should be either canceled (if we are called with a full batch)
        // or already called (if we are called by a timeout)
        self.pending_flush_handle = None;

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

        self.upstream
            .send(SendQuery(request))
            .map(|_| relay_log::trace!("outcome batch sent."))
            .map_err(|error| {
                relay_log::error!("outcome batch sending failed with: {}", LogError(&error))
            })
            .into_actor(self)
            .spawn(context);
    }

    fn send_http_message(&mut self, message: TrackRawOutcome, context: &mut Context<Self>) {
        relay_log::trace!("Batching outcome");
        self.unsent_outcomes.push(message);

        if self.unsent_outcomes.len() >= self.config.outcome_batch_size() {
            if let Some(pending_flush_handle) = self.pending_flush_handle {
                context.cancel_future(pending_flush_handle);
            }

            self.send_batch(context)
        } else if self.pending_flush_handle.is_none() {
            self.pending_flush_handle =
                Some(context.run_later(self.config.outcome_batch_interval(), Self::send_batch));
        }
    }
}

impl Actor for HttpOutcomeProducer {
    type Context = Context<Self>;
}

impl Handler<TrackRawOutcome> for HttpOutcomeProducer {
    type Result = Result<(), OutcomeError>;
    fn handle(&mut self, message: TrackRawOutcome, ctx: &mut Self::Context) -> Self::Result {
        if self.config.emit_outcomes() {
            self.send_http_message(message, ctx);
        }

        Ok(())
    }
}

impl Handler<TrackOutcome> for HttpOutcomeProducer {
    type Result = Result<(), OutcomeError>;

    fn handle(&mut self, message: TrackOutcome, _ctx: &mut Self::Context) -> Self::Result {
        self.handle(TrackRawOutcome::from_outcome(message, &self.config), _ctx)
    }
}
