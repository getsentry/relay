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
    /// Attachments that are potentially relevant for processing.
    attachments: Vec<ChunkedAttachment>,
}

/// Container payload for chunks of attachments.
#[derive(Debug, Serialize)]
struct AttachmentChunkKafkaMessage {
    /// The type discriminator of the message.
    ty: KafkaMessageType,
    /// Chunk payload of the attachment.
    payload: Bytes,
    /// The event id.
    event_id: String,
    /// The project id for the current event.
    project_id: u64,
    /// The attachment ID within the event.
    ///
    /// The triple `(project_id, event_id, id)` identifies an attachment uniquely.
    id: String,
    /// Sequence number of chunk. Starts at 0 and ends at `AttachmentKafkaMessage.num_chunks - 1`.
    chunk_index: usize,
}

/// A "standalone" attachment.
///
/// Still belongs to an event but can be sent independently (like userfeedback) and is not
/// considered in processing.
#[derive(Debug, Serialize)]
struct AttachmentKafkaMessage {
    /// The type discriminator of the message.
    ty: KafkaMessageType,
    /// The event id.
    event_id: String,
    /// The project id for the current event.
    project_id: u64,
    /// The attachment.
    #[serde(flatten)]
    attachment: ChunkedAttachment,
}

#[derive(Debug, Serialize)]
enum AttachmentType {
    /// A regular attachment without special meaning.
    #[serde(rename = "event.attachment")]
    Attachment,

    /// A minidump crash report (binary data).
    #[serde(rename = "event.minidump")]
    #[allow(dead_code)]
    Minidump,

    /// An apple crash report (text data).
    #[serde(rename = "event.applecrashreport")]
    #[allow(dead_code)]
    AppleCrashReport,
}

/// Common attributes for both standalone attachments and processing-relevant attachments.
#[derive(Debug, Serialize)]
struct ChunkedAttachment {
    /// The attachment ID within the event.
    ///
    /// The triple `(project_id, event_id, id)` identifies an attachment uniquely.
    id: String,

    /// File name of the attachment file. Should not be `None`.
    name: Option<String>,

    /// Content type of the attachment payload.
    content_type: Option<String>,

    /// The Sentry-internal attachment type used in the processing pipeline.
    #[serde(rename = "type")]
    ty: AttachmentType,

    /// Number of chunks. Must be greater than zero.
    chunks: usize,
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
        let event_item = envelope.get_item(ItemType::Event);

        let topic = if envelope.get_item(ItemType::Attachment).is_some() {
            KafkaTopic::Attachments
        } else if event_item.and_then(|x| x.event_type()) == Some(EventType::Transaction) {
            KafkaTopic::Transactions
        } else {
            KafkaTopic::Events
        };

        let mut attachments = Vec::new();

        for (index, item) in envelope.items().enumerate() {
            // Only upload items first.
            if item.ty() != ItemType::Attachment {
                continue;
            }

            // TODO(jauer): This needs to be unique within the event.
            let id = index.to_string();

            let mut chunk_index = 0;
            let mut offset = 0;
            let payload = item.payload();
            let size = item.len();

            while offset == 0 || offset < size {
                let max_chunk_size = self.config.attachment_chunk_size();
                let chunk_size = std::cmp::min(max_chunk_size, size - offset);

                let attachment_message = AttachmentChunkKafkaMessage {
                    ty: KafkaMessageType::AttachmentChunk,
                    payload: payload.slice(offset, offset + chunk_size),
                    event_id: event_id.to_string(),
                    project_id,
                    id: id.clone(),
                    chunk_index,
                };

                self.produce(topic, event_id, attachment_message)?;
                offset += chunk_size;
                chunk_index += 1;
            }

            // The chunk_index is incremented after every loop iteration. After we exit the loop, it
            // is one larger than the last chunk, so it is equal to the number of chunks.
            debug_assert!(chunk_index > 0);

            attachments.push(ChunkedAttachment {
                id: id.clone(),
                name: item.filename().map(str::to_owned),
                content_type: item
                    .content_type()
                    .map(|content_type| content_type.as_str().to_owned()),
                ty: AttachmentType::Attachment,
                chunks: chunk_index,
            });
        }

        if let Some(event_item) = event_item {
            let event_message = EventKafkaMessage {
                payload: event_item.payload(),
                start_time: instant_to_unix_timestamp(start_time),
                ty: KafkaMessageType::Event,
                event_id: event_id.to_string(),
                project_id,
                remote_addr: envelope.meta().client_addr().map(|addr| addr.to_string()),
                attachments,
            };

            self.produce(topic, event_id, event_message)?;
            metric!(counter("processing.event.produced") += 1, "type" => "event");
        } else {
            // TODO(jauer)
            unimplemented!();
        }

        Ok(())
    }
}
