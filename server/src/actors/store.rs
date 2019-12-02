//! This module contains the actor that forwards events and attachments to the Sentry store.
//! The actor uses kafka topics to forward data to Sentry

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use bytes::Bytes;
use failure::{Fail, ResultExt};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, DefaultProducerContext};
use rdkafka::ClientConfig;
use serde::Serialize;

use rmp_serde::encode::Error as SerdeError;

use semaphore_common::{metric, Config, KafkaTopic};
use semaphore_general::protocol::{EventId, EventType};

use crate::envelope::{Envelope, ItemType};
use crate::service::{ServerError, ServerErrorKind};
use crate::utils::instant_to_unix_timestamp;

type ThreadedProducer = rdkafka::producer::ThreadedProducer<DefaultProducerContext>;

#[derive(Fail, Debug)]
pub enum StoreError {
    #[fail(display = "failed to send kafka message")]
    SendFailed(KafkaError),
    #[fail(display = "failed to serialize message")]
    SerializeFailed(SerdeError),
}

/// Actor for publishing events to Sentry through kafka topics.
pub struct StoreForwarder {
    config: Arc<Config>,
    producer: Arc<ThreadedProducer>,
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

        Ok(Self {
            config,
            producer: Arc::new(producer),
        })
    }

    fn produce<T>(&self, topic: KafkaTopic, event_id: EventId, message: T) -> Result<(), StoreError>
    where
        T: Serialize,
    {
        // Use the event id as partition routing key.
        let key = event_id.0.as_bytes().as_ref();

        let serialized = rmp_serde::to_vec_named(&message).map_err(StoreError::SerializeFailed)?;
        let record = BaseRecord::to(self.config.kafka_topic_name(topic))
            .payload(&serialized)
            .key(key);

        match self.producer.send(record) {
            Ok(_) => Ok(()),
            Err((kafka_error, _message)) => Err(StoreError::SendFailed(kafka_error)),
        }
    }
}

/// StoreMessageForwarder is an async actor since the only thing it does is put the messages
/// in the kafka topics
impl Actor for StoreForwarder {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Set the mailbox size to the size of the event buffer. This is a rough estimate but
        // should ensure that we're not dropping events unintentionally after we've accepted them.
        let mailbox_size = self.config.event_buffer_size() as usize;
        context.set_mailbox_capacity(mailbox_size);

        log::info!("store forwarder started");
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
    type Result = Result<(), StoreError>;

    fn handle(&mut self, message: StoreEvent, _ctx: &mut Self::Context) -> Self::Result {
        let StoreEvent {
            envelope,
            start_time,
            project_id,
        } = message;

        let event_id = envelope.event_id();
        let event_item = envelope.get_item_by_type(ItemType::Event);

        let topic = if envelope.get_item_by_type(ItemType::Attachment).is_some() {
            KafkaTopic::Attachments
        } else if event_item.and_then(|x| x.event_type()) == Some(EventType::Transaction) {
            KafkaTopic::Transactions
        } else {
            KafkaTopic::Events
        };

        for (index, item) in envelope.items().enumerate() {
            // Only upload items first.
            if item.ty() != ItemType::Attachment {
                continue;
            }

            let mut offset = 0;
            let payload = item.payload();
            let size = item.len();

            while offset == 0 || offset < size {
                let max_chunk_size = self.config.attachment_chunk_size();
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

                self.produce(topic, event_id, attachment_message)?;
                offset += chunk_size;
            }
        }

        if let Some(event_item) = event_item {
            let event_message = EventKafkaMessage {
                payload: event_item.payload(),
                start_time: instant_to_unix_timestamp(start_time),
                ty: KafkaMessageType::Event,
                event_id: event_id.to_string(),
                project_id,
                remote_addr: envelope.meta().client_addr().map(|addr| addr.to_string()),
            };

            self.produce(topic, event_id, event_message)?;
            metric!(counter("processing.event.produced") += 1, "type" => "event");
        }

        Ok(())
    }
}
