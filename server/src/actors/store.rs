//! This module contains the actor that forwards events and attachments to the Sentry store.
//! The actor uses kafka topics to forward data to Sentry

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use bytes::Bytes;
use failure::{Fail, ResultExt};
use futures::{future, Future};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::Serialize;

use rmp_serde::encode::Error as SerdeError;

use semaphore_common::{metric, tryf, Config, KafkaTopic};
use semaphore_general::protocol::EventId;

use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use crate::envelope::{Envelope, ItemType};
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

fn produce<T>(
    producer: &FutureProducer,
    config: &Config,
    topic: KafkaTopic,
    event_id: EventId,
    message: T,
) -> ResponseFuture<(), StoreError>
where
    T: Serialize,
{
    // Use the event id as partition routing key.
    let key = event_id.0.as_bytes().as_ref();

    let serialized = tryf!(rmp_serde::to_vec_named(&message).map_err(StoreError::SerializeFailed));

    let record = FutureRecord::to(config.kafka_topic_name(topic))
        .payload(&serialized)
        .key(key);

    let future = producer.send(record, 0).then(|result| match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err((kafka_error, _message))) => Err(StoreError::SendFailed(kafka_error)),
        Err(_) => Err(StoreError::Canceled),
    });

    Box::new(future)
}

/// Actor for publishing events to Sentry through kafka topics.
pub struct StoreForwarder {
    config: Arc<Config>,
    shutdown: SyncHandle,
    producer: Arc<FutureProducer>,
}

impl StoreForwarder {
    pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
        let mut client_config = ClientConfig::new();
        for config_p in config.kafka_config() {
            client_config.set(config_p.name.as_str(), config_p.value.as_str());
        }

        let producer = client_config
            .create()
            .context(ServerErrorKind::KafkaError)?;

        Ok(StoreForwarder {
            config,
            shutdown: SyncHandle::new(),
            producer: Arc::new(producer),
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
    AttachmentChunk,
    /// The serialized payload of a transaction event with some meta data.
    #[allow(unused)] // future use
    Transaction,
}

/// Container payload for event messages.
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
    /// The client ip address.
    remote_addr: Option<String>,
}

/// Container payload for chunks of attachments.
#[derive(Debug, Serialize)]
struct AttachmentKafkaMessage {
    /// The type discriminator of the message.
    ty: KafkaMessageType,
    /// Chunk payload of the attachment.
    payload: Bytes,
    /// The event id.
    event_id: String,
    /// The project id for the current event.
    project_id: u64,
    /// Index of this attachment in the list of attachments.
    index: usize,
    /// Total attachment size in bytes.
    size: usize,
    /// Offset of the current chunk in bytes.
    offset: usize,
}

/// Message sent to the StoreForwarder containing an event
#[derive(Clone, Debug)]
pub struct StoreEvent {
    pub envelope: Envelope,
    pub start_time: Instant,
    pub project_id: u64,
}

impl Message for StoreEvent {
    type Result = Result<(), StoreError>;
}

impl Handler<StoreEvent> for StoreForwarder {
    type Result = ResponseFuture<(), StoreError>;

    fn handle(&mut self, message: StoreEvent, _ctx: &mut Self::Context) -> Self::Result {
        let producer = self.producer.clone();
        let config = self.config.clone();

        let StoreEvent {
            envelope,
            start_time,
            project_id,
        } = message;

        let event_id = envelope.event_id();

        let topic = if envelope.get_item(ItemType::Attachment).is_some() {
            KafkaTopic::Attachments
        // } else if is_transaction {
        //     KafkaTopic::Transactions
        } else {
            KafkaTopic::Events
        };

        let max_chunk_size = self.config.attachment_chunk_size();
        let mut attachment_futures = Vec::with_capacity(envelope.len());

        for (index, item) in envelope.items().enumerate() {
            // Only upload items first.
            if item.ty() != ItemType::Attachment {
                continue;
            }

            let mut offset = 0;
            let payload = item.payload();
            let size = item.len();

            while offset == 0 || offset < size {
                let chunk_size = std::cmp::min(max_chunk_size, size - offset);
                let attachment_message = AttachmentKafkaMessage {
                    ty: KafkaMessageType::AttachmentChunk,
                    payload: payload.slice(offset, offset + chunk_size),
                    event_id: event_id.to_string(),
                    project_id,
                    index,
                    offset,
                    size,
                };

                attachment_futures.push(produce(
                    &producer,
                    &config,
                    topic,
                    event_id,
                    attachment_message,
                ));

                offset += chunk_size;
            }
        }

        let mut response: ResponseFuture<(), StoreError> = match attachment_futures.len() {
            0 => Box::new(future::ok(())),
            _ => Box::new(future::join_all(attachment_futures).map(|_| ())),
        };

        if let Some(event_item) = envelope.get_item(ItemType::Event) {
            let event_message = EventKafkaMessage {
                payload: event_item.payload(),
                start_time: instant_to_unix_timestamp(start_time),
                ty: KafkaMessageType::Event,
                event_id: event_id.to_string(),
                project_id,
                remote_addr: envelope.meta().client_addr().map(|addr| addr.to_string()),
            };

            let chained_future = response
                .and_then(move |_| produce(&producer, &config, topic, event_id, event_message))
                .map(|_| metric!(counter("processing.event.produced") += 1, "type" => "event"));

            response = Box::new(chained_future);
        }

        Box::new(response.sync(&self.shutdown, StoreError::Shutdown))
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
