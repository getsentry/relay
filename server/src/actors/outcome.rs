//! This module contains the actor that tracks event outcomes.
//!
//! Outcomes describe the final "fate" of an event. As such, for every event exactly one outcome
//! must be emitted in the entire ingestion pipeline. Since Relay is only one part in this pipeline,
//! outcomes may not be emitted if the event is accepted.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;

use semaphore_common::Config;
use semaphore_general::filter::FilterStatKey;
use semaphore_general::protocol::EventId;

use crate::actors::project::RateLimit;
use crate::ServerError;

// Choose the outcome module implementation (either the real one or the fake, no-op one).
// Real outcome implementation
#[cfg(feature = "processing")]
pub use self::real_implementation::*;
// No-op outcome implementation
#[cfg(not(feature = "processing"))]
pub use self::no_op_implementation::*;

/// Tracks an outcome of an event.
///
/// See the module level documentation for more information.
#[derive(Clone, Debug)]
pub struct TrackOutcome {
    /// The timespan of the event outcome.
    pub timestamp: Instant,
    /// Organization id.
    pub org_id: Option<u64>,
    /// Project id.
    pub project_id: Option<u64>,
    /// The DSN project key id.
    pub key_id: Option<u64>,
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
    Filtered(FilterStatKey),

    /// The event has been rate limited.
    RateLimited(RateLimit),

    /// The event has been discarded because of invalid data.
    Invalid(DiscardReason),

    /// Reserved but unused in Sentry.
    #[allow(dead_code)]
    Abuse,
}

/// Reason for a discarded invalid event.
///
/// Used in `Outcome::Invalid`. Synchronize overlap with Sentry.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub enum DiscardReason {
    // Outcomes also defined in Sentry
    // -------------------------------
    Duplicate,
    ProjectId,
    AuthVersion,
    AuthClient,
    NoData,
    TooLarge,
    DisallowedMethod,
    ContentType,
    MultiProjectId,
    MissingMinidumpUpload,
    InvalidMinidump,
    SecurityReportType,
    SecurityReport,
    Cors,

    // Outcomes only emitted by Relay, not in Sentry
    // ---------------------------------------------
    ///  The event payload exceeds the maximum size limit for the respective endpoint.
    PayloadTooLarge,

    /// The payload length was not specified in headers.
    UnknownPayloadLength,

    /// Reading or decoding the payload from the socket failed for any reason.
    InvalidPayloadFormat,

    /// Parsing the event JSON payload failed due to a syntax error.
    InvalidPayloadJsonError,

    /// A project state returned by the upstream could not be parsed.
    ProjectState,

    /// Relay does not support the protocol version sent by the SDK and may not understand the event
    /// payload.
    UnsupportedProtocolVersion,

    /// An error in Relay caused event ingestion to fail. This is the catch-all and usually
    /// indicates bugs in Relay, rather than an expected failure.
    Internal,
}

/// This is the implementation that uses kafka queues and does stuff
#[cfg(feature = "processing")]
mod real_implementation {
    use super::*;

    use chrono::{DateTime, SecondsFormat, Utc};
    use failure::{Fail, ResultExt};
    use futures::Future;
    use rdkafka::{
        error::KafkaError,
        producer::{FutureProducer, FutureRecord},
        ClientConfig,
    };
    use serde::Serialize;
    use serde_json::Error as SerdeSerializationError;

    use semaphore_common::tryf;
    use semaphore_common::{metric, KafkaTopic, Uuid};

    use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
    use crate::service::ServerErrorKind;
    use crate::utils::{self, SyncFuture, SyncHandle};

    impl Outcome {
        /// Returns the name of the outcome as recognized by Sentry.
        fn name(&self) -> &'static str {
            match self {
                Outcome::Accepted => "accepted",
                Outcome::Filtered(_) => "filtered",
                Outcome::RateLimited(_) => "rate_limited",
                Outcome::Invalid(_) => "invalid",
                Outcome::Abuse => "abuse",
            }
        }

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
                Outcome::RateLimited(rate_limit) => rate_limit.reason_code(),
                Outcome::Abuse => None,
            }
        }
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

                // Relay specific reasons (not present in Sentry)
                DiscardReason::PayloadTooLarge => "payload_too_large",
                DiscardReason::UnknownPayloadLength => "unknown_payload_length",
                DiscardReason::InvalidPayloadFormat => "invalid_payload_format",
                DiscardReason::InvalidPayloadJsonError => "invalid_payload_json_error",
                DiscardReason::ProjectState => "project_state",
                DiscardReason::UnsupportedProtocolVersion => "unsupported_protocol_version",
                DiscardReason::Internal => "internal",
            }
        }
    }

    /// The outcome message is serialized as json and placed on the Kafka topic using OutcomePayload
    #[derive(Debug, Serialize, Clone)]
    struct OutcomePayload {
        /// The timespan of the event outcome.
        timestamp: String,
        /// Organization id.
        org_id: Option<u64>,
        /// Project id.
        project_id: Option<u64>,
        /// The DSN project key id.
        key_id: Option<u64>,
        /// The outcome.
        outcome: u8,
        /// Reason for the outcome.
        reason: Option<String>,
        /// The event id.
        event_id: Option<EventId>,
        /// The client ip address.
        remote_addr: Option<String>,
    }

    impl From<&TrackOutcome> for OutcomePayload {
        fn from(msg: &TrackOutcome) -> Self {
            let reason = match msg.outcome.to_reason() {
                None => None,
                Some(reason) => Some(reason.to_string()),
            };

            let start_time = utils::instant_to_system_time(msg.timestamp);
            let date_time: DateTime<Utc> = start_time.into();

            // convert to a RFC 3339 formatted date with the shape YYYY-MM-DDTHH:MM:SS.mmmmmmZ
            // e.g. something like: "2019-09-29T09:46:40.123456Z"
            let timestamp = date_time.to_rfc3339_opts(SecondsFormat::Micros, true);

            OutcomePayload {
                timestamp,
                org_id: msg.org_id,
                project_id: msg.project_id,
                key_id: msg.key_id,
                outcome: msg.outcome.to_outcome_id(),
                reason,
                event_id: msg.event_id,
                remote_addr: msg.remote_addr.map(|addr| addr.to_string()),
            }
        }
    }

    #[derive(Fail, Debug)]
    pub enum OutcomeError {
        #[fail(display = "kafka message send canceled")]
        Canceled,
        #[fail(display = "failed to send kafka message")]
        SendFailed(KafkaError),
        #[fail(display = "shutdown timer expired")]
        Shutdown,
        #[fail(display = "json serialization error")]
        SerializationError(SerdeSerializationError),
    }

    pub struct OutcomeProducer {
        config: Arc<Config>,
        shutdown: SyncHandle,
        producer: Option<FutureProducer>,
    }

    impl OutcomeProducer {
        pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
            let future_producer = if config.processing_enabled() {
                let mut client_config = ClientConfig::new();
                for config_p in config.kafka_config() {
                    client_config.set(config_p.name.as_str(), config_p.value.as_str());
                }
                let future_producer = client_config
                    .create()
                    .context(ServerErrorKind::KafkaError)?;
                Some(future_producer)
            } else {
                None
            };

            Ok(OutcomeProducer {
                config,
                shutdown: SyncHandle::new(),
                producer: future_producer,
            })
        }
    }

    impl Actor for OutcomeProducer {
        type Context = Context<Self>;

        fn started(&mut self, context: &mut Self::Context) {
            log::info!("OutcomeProducer started.");
            //register with the controller for shutdown notifications
            Controller::from_registry().do_send(Subscribe(context.address().recipient()));
        }

        fn stopped(&mut self, _ctx: &mut Self::Context) {
            log::info!("OutcomeProducer stopped.");
        }
    }

    impl Handler<TrackOutcome> for OutcomeProducer {
        type Result = ResponseFuture<(), OutcomeError>;

        fn handle(&mut self, message: TrackOutcome, _ctx: &mut Self::Context) -> Self::Result {
            log::trace!("Tracking outcome: {:?}", message);

            let producer = match self.producer {
                Some(ref producer) => producer,
                None => return Box::new(futures::future::ok(())),
            };

            let payload = tryf![serde_json::to_string(&OutcomePayload::from(&message))
                .map_err(OutcomeError::SerializationError)];

            metric!(counter("events.outcomes") +=1,
                    "reason"=>message.outcome.to_reason().unwrap_or(""),
                    "outcome"=>message.outcome.name());

            // At the moment, we support outcomes with optional EventId.
            // Here we create a fake EventId, when we don't have the real one, so that we can
            // create a kafka message key that spreads the events nicely over all the
            // kafka consumer groups.
            let key = message
                .event_id
                .unwrap_or_else(|| EventId(Uuid::new_v4()))
                .0;

            let record = FutureRecord::to(self.config.kafka_topic_name(KafkaTopic::Outcomes))
                .payload(&payload)
                .key(key.as_bytes().as_ref());

            let future = producer
                .send(record, 0)
                .then(|result| match result {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err((kafka_error, _message))) => Err(OutcomeError::SendFailed(kafka_error)),
                    Err(_) => Err(OutcomeError::Canceled),
                })
                .sync(&self.shutdown, OutcomeError::Shutdown);

            Box::new(future)
        }
    }

    impl Handler<Shutdown> for OutcomeProducer {
        type Result = ResponseActFuture<Self, (), TimeoutError>;

        fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
            let shutdown = match message.timeout {
                Some(timeout) => self.shutdown.timeout(timeout),
                None => self.shutdown.now(),
            };

            let future = shutdown.into_actor(self).and_then(move |_, slf, _ctx| {
                if let Some(ref producer) = slf.producer {
                    producer.flush(message.timeout);
                }
                actix::fut::ok(())
            });

            Box::new(future)
        }
    }
}

/// This is a shell implementation that doesn't do anything it is here to cleanly isolate the
/// conditional compilation of Semaphore  with / without processing.
/// When compiling with processing this module will NOT be included in compilation.
/// When compiling without processing this module will be included and will serve as a 'no op'
/// actor that just returns a success future whenever a message is sent to it.
#[cfg(not(feature = "processing"))]
mod no_op_implementation {
    use super::*;

    #[derive(Debug)]
    pub enum OutcomeError {}

    pub struct OutcomeProducer {}

    impl OutcomeProducer {
        pub fn create(_config: Arc<Config>) -> Result<Self, ServerError> {
            Ok(OutcomeProducer {})
        }
    }

    impl Actor for OutcomeProducer {
        type Context = Context<Self>;

        fn started(&mut self, _ctx: &mut Self::Context) {
            log::info!("Fake OutcomeProducer started.");
        }

        fn stopped(&mut self, _ctx: &mut Self::Context) {
            log::info!("Fake OutcomeProducer stopped.");
        }
    }

    impl Handler<TrackOutcome> for OutcomeProducer {
        type Result = ResponseFuture<(), OutcomeError>;

        fn handle(&mut self, message: TrackOutcome, _ctx: &mut Self::Context) -> Self::Result {
            log::trace!("Tracking outcome (no_op): {:?}", message);
            // nothing to do here
            Box::new(futures::future::ok(()))
        }
    }
}
