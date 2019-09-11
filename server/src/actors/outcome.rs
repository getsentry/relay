//! This module contains the actor that forwards Outcomes to a kafka topic
//!

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Serialize, Serializer};

use semaphore_common::Config;
use semaphore_general::protocol::EventId;
use semaphore_general::reason::OutcomeReason;

use crate::utils::instant_to_system_time;
use crate::ServerError;

/// Message
#[derive(Debug, Serialize)]
pub struct KafkaOutcomeMessage {
    /// The timespan of the event outcome.
    #[serde(serialize_with = "instant_serializer")]
    pub timestamp: Instant,
    /// Organization id.
    pub org_id: Option<u64>,
    /// Project id.
    pub project_id: u64,
    /// The DSN project key id.
    pub key_id: u64,
    /// The outcome.
    pub outcome: Outcome,
    /// Reason for the outcome.
    pub reason: OutcomeReason,
    /// The event id.
    pub event_id: Option<EventId>,
}

impl Message for KafkaOutcomeMessage {
    type Result = Result<(), OutcomeError>;
}

/// Serialization function for Instant
/// Turns an instant in a RFC 3339 formatted date with the shape YYYY-MM-DDTHH:MM:SS.mmmmmmZ
/// e.g. something like: "2019-09-29T09:46:40.123456Z"
fn instant_serializer<S>(time: &Instant, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let start_time = instant_to_system_time(&time);
    let date_time: DateTime<Utc> = start_time.into();
    let formatted_str = date_time.to_rfc3339_opts(SecondsFormat::Micros, true);
    serializer.serialize_str(formatted_str.as_str())
}

/// Defines the possible outcomes from processing an event
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum Outcome {
    #[allow(dead_code)]
    Accepted = 0,
    #[allow(dead_code)]
    Filtered = 1,
    RateLimited = 2,
    Invalid = 3,
    #[allow(dead_code)]
    Abuse = 4,
}

impl Serialize for Outcome {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self as u8)
    }
}

impl Outcome {
    /// Returns the name of the outcome as recognized by Sentry.
    pub fn to_name(&self) -> &'static str {
        match self {
            Outcome::Accepted => "accepted",
            Outcome::Filtered => "filtered",
            Outcome::RateLimited => "rate_limited",
            Outcome::Invalid => "invalid",
            Outcome::Abuse => "abuse",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn outcome_serialization_test() {
        let val = serde_json::to_string(&Outcome::Accepted).unwrap_or("error".to_string());
        println!("----{:?}", val);
        let val = serde_json::to_string(&Outcome::RateLimited).unwrap_or("error".to_string());
        println!("----{:?}", val);
    }
}

//impl Serialize for Outcome {
//    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
//        serializer.serialize_u64(*self as u64)
//    }
//}

// Choose the outcome module implementation (either the real one or the fake, no-op one).
// Real outcome implementation
#[cfg(feature = "processing")]
pub use self::real_implementation::*;
// No-op outcome implementation
#[cfg(not(feature = "processing"))]
pub use self::no_op_implementation::*;

/// This is the implementation that uses kafka queues and does stuff
#[cfg(feature = "processing")]
mod real_implementation {
    use super::*;

    use futures::Future;
    use rdkafka::{
        error::KafkaError,
        producer::{FutureProducer, FutureRecord},
        ClientConfig,
    };
    use serde_json::Error as SerdeSerializationError;

    use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
    use crate::service::ServerErrorKind;
    use crate::utils::{SyncFuture, SyncHandle};
    use failure::{Fail, ResultExt};
    use semaphore_common::tryf;
    use semaphore_common::{metric, KafkaTopic, Uuid};

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
        producer: FutureProducer,
    }

    impl OutcomeProducer {
        pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
            let mut client_config = ClientConfig::new();
            for config_p in config.kafka_config() {
                client_config.set(config_p.name.as_str(), config_p.value.as_str());
            }
            let client_config = client_config
                .create()
                .context(ServerErrorKind::KafkaError)?;

            Ok(OutcomeProducer {
                config,
                shutdown: SyncHandle::new(),
                producer: client_config,
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

    impl Handler<KafkaOutcomeMessage> for OutcomeProducer {
        type Result = ResponseFuture<(), OutcomeError>;

        fn handle(
            &mut self,
            message: KafkaOutcomeMessage,
            _ctx: &mut Self::Context,
        ) -> Self::Result {
            let payload =
                tryf![serde_json::to_string(&message).map_err(OutcomeError::SerializationError)];

            metric!(counter("events.outcomes") +=1 , "reason"=>message.reason.name(), 
            "outcome"=>message.outcome.to_name() );

            // TODO RaduW. 11.Sept.2019 At the moment we support outcomes with optional EventId.
            //      Here we create a fake EventId, when we don't have the real one only, to use
            //      it in the kafka message key so that the events are nicely spread over all the
            //      kafka consumer groups.
            let key = message.event_id.unwrap_or(EventId(Uuid::new_v4())).0;

            let record = FutureRecord::to(self.config.kafka_topic_name(KafkaTopic::Outcomes))
                .payload(&payload)
                .key(key.as_bytes().as_ref());

            let future = self
                .producer
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
                slf.producer.flush(message.timeout);
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

    impl Handler<KafkaOutcomeMessage> for OutcomeProducer {
        type Result = ResponseFuture<(), OutcomeError>;

        fn handle(
            &mut self,
            _message: KafkaOutcomeMessage,
            _ctx: &mut Self::Context,
        ) -> Self::Result {
            // nothing to do here
            Box::new(futures::future::ok(()))
        }
    }

}
