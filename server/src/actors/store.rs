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
use serde::{ser::Error, Serialize};

use rmp_serde::encode::Error as RmpError;

use semaphore_common::{metric, Config, KafkaTopic, ProjectId, Uuid};
use semaphore_general::protocol::{EventId, EventType};

use crate::envelope::{AttachmentType, Envelope, Item, ItemType};
use crate::service::{ServerError, ServerErrorKind};
use crate::utils::instant_to_unix_timestamp;

type ThreadedProducer = rdkafka::producer::ThreadedProducer<DefaultProducerContext>;

#[derive(Fail, Debug)]
pub enum StoreError {
    #[fail(display = "failed to send kafka message")]
    SendFailed(#[cause] KafkaError),
    #[fail(display = "failed to serialize kafka message")]
    SerializeFailed(#[cause] RmpError),
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

    fn produce(
        &self,
        topic: KafkaTopic,
        event_id: EventId,
        message: KafkaMessage,
    ) -> Result<(), StoreError> {
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

    fn produce_attachment_chunks(
        &self,
        event_id: EventId,
        project_id: ProjectId,
        item: &Item,
    ) -> Result<ChunkedAttachment, StoreError> {
        let id = Uuid::new_v4().to_string();

        let mut chunk_index = 0;
        let mut offset = 0;
        let payload = item.payload();
        let size = item.len();

        while offset == 0 || offset < size {
            let max_chunk_size = self.config.attachment_chunk_size();
            let chunk_size = std::cmp::min(max_chunk_size, size - offset);

            let attachment_message = KafkaMessage::AttachmentChunk(AttachmentChunkKafkaMessage {
                payload: payload.slice(offset, offset + chunk_size),
                event_id,
                project_id,
                id: id.clone(),
                chunk_index,
            });

            self.produce(KafkaTopic::Attachments, event_id, attachment_message)?;
            offset += chunk_size;
            chunk_index += 1;
        }

        // The chunk_index is incremented after every loop iteration. After we exit the loop, it
        // is one larger than the last chunk, so it is equal to the number of chunks.
        debug_assert!(chunk_index > 0);

        Ok(ChunkedAttachment {
            id: id.clone(),
            name: item.filename().map(str::to_owned),
            content_type: item
                .content_type()
                .map(|content_type| content_type.as_str().to_owned()),
            attachment_type: item.attachment_type().unwrap_or_default(),
            chunks: chunk_index,
        })
    }

    fn produce_user_report(
        &self,
        event_id: EventId,
        project_id: ProjectId,
        start_time: Instant,
        item: &Item,
    ) -> Result<(), StoreError> {
        self.produce(
            KafkaTopic::Attachments,
            event_id,
            KafkaMessage::UserReport(UserReportKafkaMessage {
                project_id,
                payload: item.payload(),
                start_time: instant_to_unix_timestamp(start_time),
            }),
        )?;

        Ok(())
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
    #[serde(serialize_with = "serialize_attachment_type")]
    attachment_type: AttachmentType,

    /// Number of chunks. Must be greater than zero.
    chunks: usize,
}

/// A hack to make rmp-serde behave more like serde-json when serializing enums.
///
/// Cannot serialize bytes.
///
/// See https://github.com/3Hren/msgpack-rust/pull/214
fn serialize_attachment_type<S, T>(t: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: serde::Serialize,
{
    serde_json::to_value(t)
        .map_err(|e| S::Error::custom(e.to_string()))?
        .serialize(serializer)
}

/// Container payload for event messages.
#[derive(Debug, Serialize)]
struct EventKafkaMessage {
    /// Raw event payload.
    payload: Bytes,
    /// Time at which the event was received by Relay.
    start_time: u64,
    /// The event id.
    event_id: EventId,
    /// The project id for the current event.
    project_id: ProjectId,
    /// The client ip address.
    remote_addr: Option<String>,
    /// Attachments that are potentially relevant for processing.
    attachments: Vec<ChunkedAttachment>,
}

/// Container payload for chunks of attachments.
#[derive(Debug, Serialize)]
struct AttachmentChunkKafkaMessage {
    /// Chunk payload of the attachment.
    payload: Bytes,
    /// The event id.
    event_id: EventId,
    /// The project id for the current event.
    project_id: ProjectId,
    /// The attachment ID within the event.
    ///
    /// The triple `(project_id, event_id, id)` identifies an attachment uniquely.
    id: String,
    /// Sequence number of chunk. Starts at 0 and ends at `AttachmentKafkaMessage.num_chunks - 1`.
    chunk_index: usize,
}

/// A "standalone" attachment.
///
/// Still belongs to an event but can be sent independently (like UserReport) and is not
/// considered in processing.
#[derive(Debug, Serialize)]
struct AttachmentKafkaMessage {
    /// The event id.
    event_id: EventId,
    /// The project id for the current event.
    project_id: ProjectId,
    /// The attachment.
    attachment: ChunkedAttachment,
}

/// User report for an event wrapped up in a message ready for consumption in Kafka.
///
/// Is always independent of an event and can be sent as part of any envelope.
#[derive(Debug, Serialize)]
struct UserReportKafkaMessage {
    /// The project id for the current event.
    project_id: ProjectId,
    start_time: u64,
    payload: Bytes,
}

/// An enum over all possible ingest messages.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum KafkaMessage {
    Event(EventKafkaMessage),
    Attachment(AttachmentKafkaMessage),
    AttachmentChunk(AttachmentChunkKafkaMessage),
    UserReport(UserReportKafkaMessage),
}

/// Message sent to the StoreForwarder containing an event
#[derive(Clone, Debug)]
pub struct StoreEvent {
    pub envelope: Envelope,
    pub start_time: Instant,
    pub project_id: ProjectId,
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
        let event_item = envelope.get_item_by(|item| item.ty() == ItemType::Event);

        let topic = if envelope
            .get_item_by(|item| item.ty() == ItemType::Attachment)
            .is_some()
        {
            KafkaTopic::Attachments
        } else if event_item.and_then(|x| x.event_type()) == Some(EventType::Transaction) {
            KafkaTopic::Transactions
        } else {
            KafkaTopic::Events
        };

        let mut attachments = Vec::new();

        for item in envelope.items() {
            match item.ty() {
                ItemType::Attachment => {
                    debug_assert!(topic == KafkaTopic::Attachments);
                    attachments.push(self.produce_attachment_chunks(event_id, project_id, item)?);
                }
                ItemType::UserReport => {
                    self.produce_user_report(event_id, project_id, start_time, item)?
                }
                _ => {}
            }
        }

        if let Some(event_item) = event_item {
            log::trace!("Sending event item of envelope to kafka");
            let event_message = KafkaMessage::Event(EventKafkaMessage {
                payload: event_item.payload(),
                start_time: instant_to_unix_timestamp(start_time),
                event_id,
                project_id,
                remote_addr: envelope.meta().client_addr().map(|addr| addr.to_string()),
                attachments,
            });

            self.produce(topic, event_id, event_message)?;
            metric!(counter("processing.event.produced") += 1, "type" => "event");
        } else {
            log::trace!("Sending individual attachments of envelope to kafka");
            for attachment in attachments {
                let attachment_message = KafkaMessage::Attachment(AttachmentKafkaMessage {
                    event_id,
                    project_id,
                    attachment,
                });

                self.produce(topic, event_id, attachment_message)?;
                metric!(counter("processing.event.produced") += 1, "type" => "attachment");
            }
        }

        Ok(())
    }
}
