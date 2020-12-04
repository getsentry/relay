//! This module contains the actor that forwards events and attachments to the Sentry store.
//! The actor uses kafka topics to forward data to Sentry

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use bytes::Bytes;
use failure::{Fail, ResultExt};
use rdkafka::error::KafkaError;
use rdkafka::producer::BaseRecord;
use rdkafka::ClientConfig;
use rmp_serde::encode::Error as RmpError;
use serde::{ser::Error, Serialize};

use relay_common::{metric, ProjectId, UnixTimestamp, Uuid};
use relay_config::{Config, KafkaTopic};
use relay_general::protocol::{self, EventId, SessionAggregates, SessionStatus, SessionUpdate};
use relay_quotas::Scoping;

use crate::envelope::{AttachmentType, Envelope, Item, ItemType};
use crate::metrics::RelayCounters;
use crate::service::{ServerError, ServerErrorKind};
use crate::utils::{CaptureErrorContext, ThreadedProducer};

lazy_static::lazy_static! {
    static ref NAMESPACE_DID: Uuid =
        Uuid::new_v5(&Uuid::NAMESPACE_URL, b"https://sentry.io/#did");
}

/// The maximum number of individual session updates generated for each aggregate item.
const MAX_EXPLODED_SESSIONS: usize = 100;

/// Fallback name used for attachment items without a `filename` header.
const UNNAMED_ATTACHMENT: &str = "Unnamed Attachment";

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

fn make_distinct_id(s: &str) -> Uuid {
    s.parse()
        .unwrap_or_else(|_| Uuid::new_v5(&NAMESPACE_DID, s.as_bytes()))
}

impl StoreForwarder {
    pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
        let mut client_config = ClientConfig::new();
        for config_p in config.kafka_config() {
            client_config.set(config_p.name.as_str(), config_p.value.as_str());
        }

        let producer = client_config
            .create_with_context(CaptureErrorContext)
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
            name: match item.filename() {
                Some(name) => name.to_owned(),
                None => UNNAMED_ATTACHMENT.to_owned(),
            },
            content_type: item
                .content_type()
                .map(|content_type| content_type.as_str().to_owned()),
            attachment_type: item.attachment_type().unwrap_or_default(),
            chunks: chunk_index,
            size: Some(size),
            rate_limited: Some(item.rate_limited()),
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
            start_time: UnixTimestamp::from_instant(start_time).as_secs(),
        });

        self.produce(KafkaTopic::Attachments, message)
    }

    fn produce_sessions(
        &self,
        org_id: u64,
        project_id: ProjectId,
        event_retention: u16,
        client: Option<&str>,
        item: &Item,
    ) -> Result<(), StoreError> {
        match item.ty() {
            ItemType::Session => {
                let mut session = match SessionUpdate::parse(&item.payload()) {
                    Ok(session) => session,
                    Err(_) => return Ok(()),
                };
                if session.status == SessionStatus::Errored {
                    // Individual updates should never have the status `errored`
                    session.status = SessionStatus::Exited;
                }
                self.produce_session_update(org_id, project_id, event_retention, client, session)?;
            }
            ItemType::Sessions => {
                let aggregates = match SessionAggregates::parse(&item.payload()) {
                    Ok(aggregates) => aggregates,
                    Err(_) => return Ok(()),
                };

                if self.config.explode_session_aggregates() {
                    if aggregates.num_sessions() as usize > MAX_EXPLODED_SESSIONS {
                        relay_log::warn!("exploded session items from aggregate exceed threshold");
                    }

                    for session in aggregates.into_updates_iter().take(MAX_EXPLODED_SESSIONS) {
                        self.produce_session_update(
                            org_id,
                            project_id,
                            event_retention,
                            client,
                            session,
                        )?;
                    }
                } else {
                    self.produce_sessions_from_aggregate(
                        org_id,
                        project_id,
                        event_retention,
                        client,
                        aggregates,
                    )?
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn produce_sessions_from_aggregate(
        &self,
        org_id: u64,
        project_id: ProjectId,
        event_retention: u16,
        client: Option<&str>,
        aggregates: SessionAggregates,
    ) -> Result<(), StoreError> {
        let SessionAggregates {
            aggregates,
            attributes,
        } = aggregates;
        let message = SessionKafkaMessage {
            org_id,
            project_id,
            session_id: Uuid::nil(),
            distinct_id: Uuid::nil(),
            quantity: 1,
            seq: 0,
            received: protocol::datetime_to_timestamp(chrono::Utc::now()),
            started: 0f64,
            duration: None,
            errors: 0,
            release: attributes.release,
            environment: attributes.environment,
            sdk: client.map(str::to_owned),
            retention_days: event_retention,
            status: SessionStatus::Exited,
        };

        if aggregates.len() > MAX_EXPLODED_SESSIONS {
            relay_log::warn!("aggregated session items exceed threshold");
        }

        for item in aggregates.into_iter().take(MAX_EXPLODED_SESSIONS) {
            let mut message = message.clone();
            message.started = protocol::datetime_to_timestamp(item.started);
            message.distinct_id = item
                .distinct_id
                .as_deref()
                .map(make_distinct_id)
                .unwrap_or_default();

            if item.exited > 0 {
                message.errors = 0;
                message.quantity = item.exited;
                self.send_session_message(message.clone())?;
            }
            if item.errored > 0 {
                message.errors = 1;
                message.status = SessionStatus::Errored;
                message.quantity = item.errored;
                self.send_session_message(message.clone())?;
            }
            if item.abnormal > 0 {
                message.errors = 1;
                message.status = SessionStatus::Abnormal;
                message.quantity = item.abnormal;
                self.send_session_message(message.clone())?;
            }
            if item.crashed > 0 {
                message.errors = 1;
                message.status = SessionStatus::Crashed;
                message.quantity = item.crashed;
                self.send_session_message(message)?;
            }
        }
        Ok(())
    }

    fn produce_session_update(
        &self,
        org_id: u64,
        project_id: ProjectId,
        event_retention: u16,
        client: Option<&str>,
        session: SessionUpdate,
    ) -> Result<(), StoreError> {
        self.send_session_message(SessionKafkaMessage {
            org_id,
            project_id,
            session_id: session.session_id,
            distinct_id: session
                .distinct_id
                .as_deref()
                .map(make_distinct_id)
                .unwrap_or_default(),
            quantity: 1,
            seq: if session.init { 0 } else { session.sequence },
            received: protocol::datetime_to_timestamp(session.timestamp),
            started: protocol::datetime_to_timestamp(session.started),
            duration: session.duration,
            status: session.status,
            errors: session
                .errors
                .min(u16::max_value().into())
                .max((session.status == SessionStatus::Crashed) as _) as _,
            release: session.attributes.release,
            environment: session.attributes.environment,
            sdk: client.map(str::to_owned),
            retention_days: event_retention,
        })
    }

    fn send_session_message(&self, message: SessionKafkaMessage) -> Result<(), StoreError> {
        relay_log::trace!("Sending session item to kafka");
        self.produce(KafkaTopic::Sessions, KafkaMessage::Session(message))?;
        metric!(
            counter(RelayCounters::ProcessingMessageProduced) += 1,
            event_type = "session"
        );
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

        relay_log::info!("store forwarder started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("store forwarder stopped");
    }
}

/// Common attributes for both standalone attachments and processing-relevant attachments.
#[derive(Debug, Serialize)]
struct ChunkedAttachment {
    /// The attachment ID within the event.
    ///
    /// The triple `(project_id, event_id, id)` identifies an attachment uniquely.
    id: String,

    /// File name of the attachment file.
    name: String,

    /// Content type of the attachment payload.
    content_type: Option<String>,

    /// The Sentry-internal attachment type used in the processing pipeline.
    #[serde(serialize_with = "serialize_attachment_type")]
    attachment_type: AttachmentType,

    /// Number of chunks. Must be greater than zero.
    chunks: usize,

    /// The size of the attachment in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<usize>,

    /// Whether this attachment was rate limited and should be removed after processing.
    ///
    /// By default, rate limited attachments are immediately removed from Envelopes. For processing,
    /// native crash reports still need to be retained. These attachments are marked with the
    /// `rate_limited` header, which signals to the processing pipeline that the attachment should
    /// not be persisted after processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    rate_limited: Option<bool>,
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

#[derive(Clone, Debug, Serialize)]
struct SessionKafkaMessage {
    org_id: u64,
    project_id: ProjectId,
    session_id: Uuid,
    distinct_id: Uuid,
    quantity: u32,
    seq: u64,
    received: f64,
    started: f64,
    duration: Option<f64>,
    status: SessionStatus,
    errors: u16,
    release: String,
    environment: Option<String>,
    sdk: Option<String>,
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
    pub scoping: Scoping,
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
            scoping,
        } = message;

        let retention = envelope.retention();
        let client = envelope.meta().client();
        let event_id = envelope.event_id();
        let event_item = envelope.get_item_by(|item| {
            matches!(
                item.ty(),
                ItemType::Event | ItemType::Transaction | ItemType::Security
            )
        });

        let topic = if envelope.get_item_by(is_slow_item).is_some() {
            KafkaTopic::Attachments
        } else if event_item.map(|x| x.ty()) == Some(ItemType::Transaction) {
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
                        scoping.project_id,
                        item,
                    )?;
                    attachments.push(attachment);
                }
                ItemType::UserReport => {
                    debug_assert!(topic == KafkaTopic::Attachments);
                    self.produce_user_report(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.project_id,
                        start_time,
                        item,
                    )?;
                    metric!(
                        counter(RelayCounters::ProcessingMessageProduced) += 1,
                        event_type = "user_report"
                    );
                }
                ItemType::Session | ItemType::Sessions => {
                    self.produce_sessions(
                        scoping.organization_id,
                        scoping.project_id,
                        retention,
                        client,
                        item,
                    )?;
                }
                _ => {}
            }
        }

        if let Some(event_item) = event_item {
            relay_log::trace!("Sending event item of envelope to kafka");
            let event_message = KafkaMessage::Event(EventKafkaMessage {
                payload: event_item.payload(),
                start_time: UnixTimestamp::from_instant(start_time).as_secs(),
                event_id: event_id.ok_or(StoreError::NoEventId)?,
                project_id: scoping.project_id,
                remote_addr: envelope.meta().client_addr().map(|addr| addr.to_string()),
                attachments,
            });

            self.produce(topic, event_message)?;
            metric!(
                counter(RelayCounters::ProcessingMessageProduced) += 1,
                event_type = "event"
            );
        } else if !attachments.is_empty() {
            relay_log::trace!("Sending individual attachments of envelope to kafka");
            for attachment in attachments {
                let attachment_message = KafkaMessage::Attachment(AttachmentKafkaMessage {
                    event_id: event_id.ok_or(StoreError::NoEventId)?,
                    project_id: scoping.project_id,
                    attachment,
                });

                self.produce(topic, attachment_message)?;
                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = "attachment"
                );
            }
        }

        Ok(())
    }
}
