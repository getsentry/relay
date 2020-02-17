//! This module contains the actor that forwards events and attachments to the Sentry store.
//! The actor uses kafka topics to forward data to Sentry

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use bytes::Bytes;
use chrono::{Duration, Utc};
use failure::{Fail, ResultExt};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, DefaultProducerContext};
use rdkafka::ClientConfig;
use rmp_serde::encode::Error as RmpError;
use serde::{ser::Error, Serialize};

use relay_common::{metric, LogError, ProjectId, Uuid};
use relay_config::{Config, KafkaTopic};
use relay_general::protocol::{EventId, EventType, SessionStatus, SessionUpdate};
use relay_general::types;

use crate::constants::MAX_SESSION_DAYS;
use crate::envelope::{AttachmentType, Envelope, Item, ItemType};
use crate::metrics::RelayCounters;
use crate::service::{ServerError, ServerErrorKind};
use crate::utils::instant_to_unix_timestamp;

type ThreadedProducer = rdkafka::producer::ThreadedProducer<DefaultProducerContext>;

#[derive(Fail, Debug)]
pub enum StoreError {
    #[fail(display = "failed to send kafka message")]
    SendFailed(#[cause] KafkaError),
    #[fail(display = "failed to serialize kafka message")]
    InvalidMsgPack(#[cause] RmpError),
    #[fail(display = "failed to serialize json message")]
    InvalidJson(#[cause] serde_json::Error),
    #[fail(display = "failed to store event because event id was missing")]
    NoEventId,
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

    fn produce(&self, topic: KafkaTopic, message: KafkaMessage) -> Result<(), StoreError> {
        let serialized = message.serialize()?;
        let record = BaseRecord::to(self.config.kafka_topic_name(topic))
            .key(message.key())
            .payload(&serialized);

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

        // This skips chunks for empty attachments. The consumer does not require chunks for
        // empty attachments. `chunks` will be `0` in this case.
        while offset < size {
            let max_chunk_size = self.config.attachment_chunk_size();
            let chunk_size = std::cmp::min(max_chunk_size, size - offset);

            let attachment_message = KafkaMessage::AttachmentChunk(AttachmentChunkKafkaMessage {
                payload: payload.slice(offset, offset + chunk_size),
                event_id,
                project_id,
                id: id.clone(),
                chunk_index,
            });

            self.produce(KafkaTopic::Attachments, attachment_message)?;
            offset += chunk_size;
            chunk_index += 1;
        }

        // The chunk_index is incremented after every loop iteration. After we exit the loop, it
        // is one larger than the last chunk, so it is equal to the number of chunks.

        Ok(ChunkedAttachment {
            id,
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
        let message = KafkaMessage::UserReport(UserReportKafkaMessage {
            project_id,
            event_id,
            payload: item.payload(),
            start_time: instant_to_unix_timestamp(start_time),
        });

        self.produce(KafkaTopic::Attachments, message)
    }

    fn produce_session(
        &self,
        org_id: u64,
        project_id: ProjectId,
        event_retention: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let session = match SessionUpdate::parse(&item.payload()) {
            Ok(session) => session,
            Err(error) => {
                // Skip gracefully here to allow sending other messages.
                log::error!("failed to store session: {}", LogError(&error));
                return Ok(());
            }
        };

        if session.sequence == u64::max_value() {
            // TODO(ja): Move this to normalization eventually.
            log::trace!("skipping session due to sequence overflow");
            return Ok(());
        }

        let session_age = Utc::now() - session.started;
        if session_age > Duration::days(MAX_SESSION_DAYS.into()) {
            log::trace!("skipping session older than {} days", MAX_SESSION_DAYS);
            return Ok(());
        }

        let message = KafkaMessage::Session(SessionKafkaMessage {
            org_id,
            project_id,
            session_id: session.session_id,
            distinct_id: session.distinct_id,
            seq: session.sequence,
            timestamp: types::datetime_to_timestamp(session.timestamp),
            started: types::datetime_to_timestamp(session.started),
            sample_rate: session.sample_rate,
            duration: session.duration.unwrap_or(0.0),
            status: session.status,
            os: session.attributes.os,
            os_version: session.attributes.os_version,
            device_family: session.attributes.device_family,
            release: session.attributes.release,
            environment: session.attributes.environment,
            retention_days: event_retention,
        });

        self.produce(KafkaTopic::Sessions, message)
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

    // Used for KafkaMessage::key
    #[serde(skip)]
    event_id: EventId,
}

#[derive(Debug, Serialize)]
struct SessionKafkaMessage {
    org_id: u64,
    project_id: u64,
    session_id: Uuid,
    distinct_id: Uuid,
    seq: u64,
    timestamp: f64,
    started: f64,
    sample_rate: f32,
    duration: f64,
    status: SessionStatus,
    os: Option<String>,
    os_version: Option<String>,
    device_family: Option<String>,
    release: Option<String>,
    environment: Option<String>,
    retention_days: u16,
}

/// An enum over all possible ingest messages.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
enum KafkaMessage {
    Event(EventKafkaMessage),
    Attachment(AttachmentKafkaMessage),
    AttachmentChunk(AttachmentChunkKafkaMessage),
    UserReport(UserReportKafkaMessage),
    Session(SessionKafkaMessage),
}

impl KafkaMessage {
    /// Returns the partitioning key for this kafka message determining.
    fn key(&self) -> &[u8] {
        let event_id = match self {
            Self::Event(message) => &message.event_id.0,
            Self::Attachment(message) => &message.event_id.0,
            Self::AttachmentChunk(message) => &message.event_id.0,
            Self::UserReport(message) => &message.event_id.0,
            Self::Session(message) => &message.session_id,
        };

        event_id.as_bytes()
    }

    /// Serializes the message into its binary format.
    fn serialize(&self) -> Result<Vec<u8>, StoreError> {
        if let KafkaMessage::Session(ref message) = *self {
            return serde_json::to_vec(&message).map_err(StoreError::InvalidJson);
        }

        rmp_serde::to_vec_named(&self).map_err(StoreError::InvalidMsgPack)
    }
}

/// Message sent to the StoreForwarder containing an event
#[derive(Clone, Debug)]
pub struct StoreEnvelope {
    pub envelope: Envelope,
    pub start_time: Instant,
    pub project_id: ProjectId,
    pub organization_id: u64,
}

impl Message for StoreEnvelope {
    type Result = Result<(), StoreError>;
}

/// Determines if the given item is considered slow.
///
/// Slow items must be routed to the `Attachments` topic.
fn is_slow_item(item: &Item) -> bool {
    item.ty() == ItemType::Attachment || item.ty() == ItemType::UserReport
}

impl Handler<StoreEnvelope> for StoreForwarder {
    type Result = Result<(), StoreError>;

    fn handle(&mut self, message: StoreEnvelope, _ctx: &mut Self::Context) -> Self::Result {
        let StoreEnvelope {
            envelope,
            start_time,
            project_id,
            organization_id,
        } = message;

        let retention = envelope.retention();
        let event_id = envelope.event_id();
        let event_item = envelope.get_item_by(|item| item.ty() == ItemType::Event);

        let topic = if envelope.get_item_by(is_slow_item).is_some() {
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
                    let attachment = self.produce_attachment_chunks(
                        event_id.ok_or(StoreError::NoEventId)?,
                        project_id,
                        item,
                    )?;
                    attachments.push(attachment);
                }
                ItemType::UserReport => {
                    debug_assert!(topic == KafkaTopic::Attachments);
                    self.produce_user_report(
                        event_id.ok_or(StoreError::NoEventId)?,
                        project_id,
                        start_time,
                        item,
                    )?;
                }
                ItemType::Session => {
                    self.produce_session(organization_id, project_id, retention, item)?;
                }
                _ => {}
            }
        }

        if let Some(event_item) = event_item {
            log::trace!("Sending event item of envelope to kafka");
            let event_message = KafkaMessage::Event(EventKafkaMessage {
                payload: event_item.payload(),
                start_time: instant_to_unix_timestamp(start_time),
                event_id: event_id.ok_or(StoreError::NoEventId)?,
                project_id,
                remote_addr: envelope.meta().client_addr().map(|addr| addr.to_string()),
                attachments,
            });

            self.produce(topic, event_message)?;
            metric!(
                counter(RelayCounters::ProcessingEventProduced) += 1,
                event_type = "event"
            );
        } else {
            log::trace!("Sending individual attachments of envelope to kafka");
            for attachment in attachments {
                let attachment_message = KafkaMessage::Attachment(AttachmentKafkaMessage {
                    event_id: event_id.ok_or(StoreError::NoEventId)?,
                    project_id,
                    attachment,
                });

                self.produce(topic, attachment_message)?;
                metric!(
                    counter(RelayCounters::ProcessingEventProduced) += 1,
                    event_type = "attachment"
                );
            }
        }

        Ok(())
    }
}
