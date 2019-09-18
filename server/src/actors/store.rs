//! This module contains the actor that forwards events and attachments to the Sentry store.
//! The actor uses kafka topics to forward data to Sentry

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use bytes::Bytes;
use failure::{Fail, ResultExt};
use futures::Future;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::Serialize;

use rmp_serde::encode::Error as SerdeError;

use semaphore_common::{tryf, Config, KafkaTopic};
use semaphore_general::protocol::EventId;

use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use crate::service::{ServerError, ServerErrorKind};
use crate::utils::{instant_to_unix_timestamp, SyncFuture, SyncHandle};

#[derive(Fail, Debug)]
pub enum StoreError {
    #[fail(display = "kafka message send canceled")]
    Canceled,
    #[fail(display = "failed to send kafka message")]
    SendFailed(KafkaError),
    #[fail(display = "failed to serialize message")]
    SerializeFailed(SerdeError),
    #[fail(display = "shutdown timer expired")]
    Shutdown,
}

/// Actor for publishing events to Sentry through kafka topics.
pub struct StoreForwarder {
    config: Arc<Config>,
    shutdown: SyncHandle,
    producer: FutureProducer,
}

impl StoreForwarder {
    pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
        let mut client_config = ClientConfig::new();
        for config_p in config.kafka_config() {
            client_config.set(config_p.name.as_str(), config_p.value.as_str());
        }
        let client_config = client_config
            .create()
            .context(ServerErrorKind::KafkaError)?;

        Ok(StoreForwarder {
            config,
            shutdown: SyncHandle::new(),
            producer: client_config,
        })
    }
}

/// StoreMessageForwarder is an async actor since the only thing it does is put the messages
/// in the kafka topics
impl Actor for StoreForwarder {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        log::info!("store forwarder started");
        Controller::from_registry().do_send(Subscribe(context.address().recipient()));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("store forwarder stopped");
    }
}

/// The type of a message sent to Kafka for store.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum KafkaMessageType {
    /// The serialized payload of an event with some meta data.
    Event,
    /// The binary chunk of an event attachment.
    #[allow(unused)] // future use
    AttachmentChunk,
    /// The serialized payload of a transaction event with some meta data.
    #[allow(unused)] // future use
    Transaction,
}

/// Message pack container payload for messages sent to Kafka.
#[derive(Debug, Serialize)]
struct EventKafkaMessage {
    /// The type discriminator of the message.
    ty: KafkaMessageType,
    /// Raw event payload.
    payload: Bytes,
    /// Time at which the event was received by Relay.
    start_time: u64,
    /// The event id.
    event_id: String,
    /// The project id for the current event.
    project_id: u64,
}

/// Message sent to the StoreForwarder containing an event
pub struct StoreEvent {
    pub event_id: EventId,
    pub payload: Bytes,
    pub start_time: Instant,
    pub project_id: u64,
}

impl Message for StoreEvent {
    type Result = Result<(), StoreError>;
}

impl Handler<StoreEvent> for StoreForwarder {
    type Result = ResponseFuture<(), StoreError>;

    fn handle(&mut self, message: StoreEvent, _ctx: &mut Self::Context) -> Self::Result {
        let start_timestamp = instant_to_unix_timestamp(message.start_time);

        let kafka_message = EventKafkaMessage {
            payload: message.payload,
            start_time: start_timestamp,
            ty: KafkaMessageType::Event,
            event_id: message.event_id.to_string(),
            project_id: message.project_id,
        };

        let serialized_message =
            tryf!(rmp_serde::to_vec_named(&kafka_message).map_err(StoreError::SerializeFailed));

        let record = FutureRecord::to(self.config.kafka_topic_name(KafkaTopic::Events))
            .payload(&serialized_message)
            .key(message.event_id.0.as_bytes().as_ref());

        let future = self
            .producer
            .send(record, 0)
            .then(|result| match result {
                Ok(Ok(_)) => Ok(()),
                Ok(Err((kafka_error, _message))) => Err(StoreError::SendFailed(kafka_error)),
                Err(_) => Err(StoreError::Canceled),
            })
            .sync(&self.shutdown, StoreError::Shutdown);

        Box::new(future)
    }
}

impl Handler<Shutdown> for StoreForwarder {
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
