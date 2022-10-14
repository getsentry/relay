//! This module contains the service that forwards events and attachments to the Sentry store.
//! The service uses kafka topics to forward data to Sentry

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use failure::{Fail, ResultExt};
use once_cell::sync::OnceCell;
use rdkafka::{error::KafkaError, producer::BaseRecord, ClientConfig};
use rmp_serde::encode::Error as RmpError;
use serde::{ser::Error, Serialize};

use relay_common::{ProjectId, UnixTimestamp, Uuid};
use relay_config::{Config, KafkaConfig, KafkaParams, KafkaTopic};
use relay_general::protocol::{self, EventId, SessionAggregates, SessionStatus, SessionUpdate};
use relay_log::LogError;
use relay_metrics::{Bucket, BucketValue, MetricNamespace, MetricResourceIdentifier};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{AsyncResponse, FromMessage, Interface, Sender, Service};

use crate::envelope::{AttachmentType, Envelope, Item, ItemType};
use crate::service::{ServerError, ServerErrorKind};
use crate::statsd::{RelayCounters, RelayHistograms};
use crate::utils::{CaptureErrorContext, ThreadedProducer};

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
    #[fail(display = "failed to find the shard range")]
    InvalidKafkaShardRange,
    #[fail(display = "failed to store event because event id was missing")]
    NoEventId,
}

/// Temporary map used to deduplicate kafka producers
type ReusedProducersMap<'a> = BTreeMap<Option<&'a str>, Arc<ThreadedProducer>>;

fn make_distinct_id(s: &str) -> Uuid {
    static NAMESPACE: OnceCell<Uuid> = OnceCell::new();
    let namespace =
        NAMESPACE.get_or_init(|| Uuid::new_v5(&Uuid::NAMESPACE_URL, b"https://sentry.io/#did"));

    s.parse()
        .unwrap_or_else(|_| Uuid::new_v5(namespace, s.as_bytes()))
}

pub fn make_producer<'a>(
    config: &'a Config,
    reused_producers: &mut ReusedProducersMap<'a>,
    kafka_topic: KafkaTopic,
) -> Result<Producer, ServerError> {
    let mut client_config = ClientConfig::new();
    match config
        .kafka_config(kafka_topic)
        .context(ServerErrorKind::KafkaError)?
    {
        KafkaConfig::Single(KafkaParams {
            topic_name,
            config_name,
            params,
        }) => {
            if let Some(producer) = reused_producers.get(&config_name) {
                return Ok(Producer::Single {
                    topic_name: topic_name.to_string(),
                    producer: Arc::clone(producer),
                });
            }

            for config_p in params {
                client_config.set(config_p.name.as_str(), config_p.value.as_str());
            }

            let producer = Arc::new(
                client_config
                    .create_with_context(CaptureErrorContext)
                    .context(ServerErrorKind::KafkaError)?,
            );

            reused_producers.insert(config_name, Arc::clone(&producer));
            Ok(Producer::Single {
                topic_name: topic_name.to_string(),
                producer,
            })
        }
        KafkaConfig::Sharded { shards, configs } => {
            let mut producers = BTreeMap::new();
            for (shard, kafka_params) in configs {
                if let Some(producer) = reused_producers.get(&kafka_params.config_name) {
                    let cached_producer = Arc::clone(producer);
                    producers.insert(
                        shard,
                        (kafka_params.topic_name.to_string(), cached_producer),
                    );
                    continue;
                }
                for config_p in kafka_params.params {
                    client_config.set(config_p.name.as_str(), config_p.value.as_str());
                }
                let producer = Arc::new(
                    client_config
                        .create_with_context(CaptureErrorContext)
                        .context(ServerErrorKind::KafkaError)?,
                );
                reused_producers.insert(kafka_params.config_name, Arc::clone(&producer));
                producers.insert(shard, (kafka_params.topic_name.to_string(), producer));
            }
            Ok(Producer::Sharded(ShardedProducer { shards, producers }))
        }
    }
}

/// This object containes the Kafka producer variants for single and sharded configurations.
pub enum Producer {
    Single {
        topic_name: String,
        producer: Arc<ThreadedProducer>,
    },
    Sharded(ShardedProducer),
}

/// Sharded producer configuration.
pub struct ShardedProducer {
    /// The maximum number of shards for this producer.
    shards: u64,
    /// The actual Kafka producer assigned to the range of logical shards, where the `u64` in the map is
    /// the inclusive beginning of the range.
    producers: BTreeMap<u64, (String, Arc<ThreadedProducer>)>,
}

impl ShardedProducer {
    /// Returns topic name and the Kafka producer based on the provided sharding key.
    /// Returns error [`StoreError::InvalidKafkaShardRange`] if the shard range for the provided sharding
    /// key could not be found.
    pub fn get_producer(&self, sharding_key: u64) -> Result<(&str, &ThreadedProducer), StoreError> {
        let shard = sharding_key % self.shards;
        let (topic_name, producer) = self
            .producers
            .iter()
            .take_while(|(k, _)| *k <= &shard)
            .last()
            .map(|(_, v)| v)
            .ok_or(StoreError::InvalidKafkaShardRange)?;

        Ok((topic_name, producer))
    }
}

impl Producer {
    /// Sends the message to Kafka using correct producer for the current topic.
    fn send(&self, organization_id: u64, message: KafkaMessage) -> Result<(), StoreError> {
        let serialized = message.serialize()?;
        metric!(
            histogram(RelayHistograms::KafkaMessageSize) = serialized.len() as u64,
            variant = message.variant()
        );
        let key = message.key();
        let (topic_name, producer) = match self {
            Self::Single {
                topic_name,
                producer,
            } => (topic_name.as_str(), producer.as_ref()),

            Self::Sharded(sharded) => sharded.get_producer(organization_id)?,
        };
        let record = BaseRecord::to(topic_name).key(&key).payload(&serialized);

        producer.send(record).map_err(|(kafka_error, _message)| {
            relay_log::with_scope(
                |scope| scope.set_tag("variant", message.variant()),
                || relay_log::error!("error sending kafka message: {}", kafka_error),
            );
            StoreError::SendFailed(kafka_error)
        })
    }
}

struct Producers {
    events: Producer,
    attachments: Producer,
    transactions: Producer,
    sessions: Producer,
    metrics_sessions: Producer,
    metrics_transactions: Producer,
    profiles: Producer,
    replay_events: Producer,
    replay_recordings: Producer,
}

impl Producers {
    /// Get a producer by KafkaTopic value.
    pub fn get(&self, kafka_topic: KafkaTopic) -> Option<&Producer> {
        match kafka_topic {
            KafkaTopic::Attachments => Some(&self.attachments),
            KafkaTopic::Events => Some(&self.events),
            KafkaTopic::Transactions => Some(&self.transactions),
            KafkaTopic::Outcomes | KafkaTopic::OutcomesBilling => {
                // should be unreachable
                relay_log::error!("attempted to send data to outcomes topic from store forwarder. there is another actor for that.");
                None
            }
            KafkaTopic::Sessions => Some(&self.sessions),
            KafkaTopic::MetricsSessions => Some(&self.metrics_sessions),
            KafkaTopic::MetricsTransactions => Some(&self.metrics_transactions),
            KafkaTopic::Profiles => Some(&self.profiles),
            KafkaTopic::ReplayEvents => Some(&self.replay_events),
            KafkaTopic::ReplayRecordings => Some(&self.replay_recordings),
        }
    }

    pub fn create(config: &Arc<Config>) -> Result<Self, ServerError> {
        let mut reused_producers = BTreeMap::new();
        let producers = Producers {
            attachments: make_producer(&**config, &mut reused_producers, KafkaTopic::Attachments)?,
            events: make_producer(&**config, &mut reused_producers, KafkaTopic::Events)?,
            transactions: make_producer(
                &**config,
                &mut reused_producers,
                KafkaTopic::Transactions,
            )?,
            sessions: make_producer(&**config, &mut reused_producers, KafkaTopic::Sessions)?,
            metrics_sessions: make_producer(
                &**config,
                &mut reused_producers,
                KafkaTopic::MetricsSessions,
            )?,
            metrics_transactions: make_producer(
                &**config,
                &mut reused_producers,
                KafkaTopic::MetricsTransactions,
            )?,
            profiles: make_producer(&**config, &mut reused_producers, KafkaTopic::Profiles)?,
            replay_recordings: make_producer(
                &**config,
                &mut reused_producers,
                KafkaTopic::ReplayRecordings,
            )?,
            replay_events: make_producer(
                &**config,
                &mut reused_producers,
                KafkaTopic::ReplayEvents,
            )?,
        };
        Ok(producers)
    }
}

/// Publishes an [`Envelope`] to the Sentry core application through Kafka topics.
#[derive(Clone, Debug)]
pub struct StoreEnvelope {
    pub envelope: Envelope,
    pub start_time: Instant,
    pub scoping: Scoping,
}

/// Service interface for the [`StoreEnvelope`] message.
#[derive(Debug)]
pub struct Store(StoreEnvelope, Sender<Result<(), StoreError>>);

impl Interface for Store {}

impl FromMessage<StoreEnvelope> for Store {
    type Response = AsyncResponse<Result<(), StoreError>>;

    fn from_message(message: StoreEnvelope, sender: Sender<Result<(), StoreError>>) -> Self {
        Self(message, sender)
    }
}

/// Service implementing the [`Store`] interface.
pub struct StoreService {
    config: Arc<Config>,
    producers: Producers,
}

impl StoreService {
    pub fn create(config: Arc<Config>) -> Result<Self, ServerError> {
        let producers = Producers::create(&config)?;
        Ok(Self { config, producers })
    }

    fn handle_message(&self, message: Store) {
        let Store(message, sender) = message;
        sender.send(self.handle_store_envelope(message));
    }

    fn handle_store_envelope(&self, message: StoreEnvelope) -> Result<(), StoreError> {
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
        } else if event_item.map(|x| x.ty()) == Some(&ItemType::Transaction) {
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
                        scoping.organization_id,
                        scoping.project_id,
                        item,
                    )?;
                    attachments.push(attachment);
                }
                ItemType::UserReport => {
                    debug_assert!(topic == KafkaTopic::Attachments);
                    self.produce_user_report(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.organization_id,
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
                ItemType::MetricBuckets => {
                    self.produce_metrics(scoping.organization_id, scoping.project_id, item)?
                }
                ItemType::Profile => self.produce_profile(
                    scoping.organization_id,
                    scoping.project_id,
                    scoping.key_id,
                    start_time,
                    item,
                )?,
                ItemType::ReplayRecording => {
                    let replay_recording = self.produce_replay_recording_chunks(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.organization_id,
                        scoping.project_id,
                        item,
                    )?;
                    relay_log::trace!("Sending individual replay_recordings of envelope to kafka");
                    let replay_recording_message =
                        KafkaMessage::ReplayRecording(ReplayRecordingKafkaMessage {
                            replay_id: event_id.ok_or(StoreError::NoEventId)?,
                            project_id: scoping.project_id,
                            org_id: scoping.organization_id,
                            retention_days: retention,
                            replay_recording,
                        });

                    self.produce(
                        KafkaTopic::ReplayRecordings,
                        scoping.organization_id,
                        replay_recording_message,
                    )?;
                    metric!(
                        counter(RelayCounters::ProcessingMessageProduced) += 1,
                        event_type = "replay_recording"
                    );
                }
                ItemType::ReplayEvent => self.produce_replay_event(
                    event_id.ok_or(StoreError::NoEventId)?,
                    scoping.organization_id,
                    scoping.project_id,
                    start_time,
                    retention,
                    item,
                )?,
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

            self.produce(topic, scoping.organization_id, event_message)?;
            metric!(
                counter(RelayCounters::ProcessingMessageProduced) += 1,
                event_type = &event_item.ty().to_string()
            );
        } else if !attachments.is_empty() {
            relay_log::trace!("Sending individual attachments of envelope to kafka");
            for attachment in attachments {
                let attachment_message = KafkaMessage::Attachment(AttachmentKafkaMessage {
                    event_id: event_id.ok_or(StoreError::NoEventId)?,
                    project_id: scoping.project_id,
                    attachment,
                });

                self.produce(topic, scoping.organization_id, attachment_message)?;
                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = "attachment"
                );
            }
        }

        Ok(())
    }

    fn produce(
        &self,
        topic: KafkaTopic,
        organization_id: u64,
        message: KafkaMessage,
    ) -> Result<(), StoreError> {
        if let Some(producer) = self.producers.get(topic) {
            producer.send(organization_id, message)?;
        }

        Ok(())
    }

    fn produce_attachment_chunks(
        &self,
        event_id: EventId,
        organization_id: u64,
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
            self.produce(KafkaTopic::Attachments, organization_id, attachment_message)?;
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
        organization_id: u64,
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

        self.produce(KafkaTopic::Attachments, organization_id, message)
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
                    Err(error) => {
                        relay_log::error!("failed to store session: {}", LogError(&error));
                        return Ok(());
                    }
                };

                if session.status == SessionStatus::Errored {
                    // Individual updates should never have the status `errored`
                    session.status = SessionStatus::Exited;
                }
                self.produce_session_update(org_id, project_id, event_retention, client, session)
            }
            ItemType::Sessions => {
                let aggregates = match SessionAggregates::parse(&item.payload()) {
                    Ok(aggregates) => aggregates,
                    Err(_) => return Ok(()),
                };

                self.produce_sessions_from_aggregate(
                    org_id,
                    project_id,
                    event_retention,
                    client,
                    aggregates,
                )
            }
            _ => Ok(()),
        }
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
                self.send_session_message(org_id, message.clone())?;
            }
            if item.errored > 0 {
                message.errors = 1;
                message.status = SessionStatus::Errored;
                message.quantity = item.errored;
                self.send_session_message(org_id, message.clone())?;
            }
            if item.abnormal > 0 {
                message.errors = 1;
                message.status = SessionStatus::Abnormal;
                message.quantity = item.abnormal;
                self.send_session_message(org_id, message.clone())?;
            }
            if item.crashed > 0 {
                message.errors = 1;
                message.status = SessionStatus::Crashed;
                message.quantity = item.crashed;
                self.send_session_message(org_id, message)?;
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
        self.send_session_message(
            org_id,
            SessionKafkaMessage {
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
                    .max((session.status == SessionStatus::Crashed) as _)
                    as _,
                release: session.attributes.release,
                environment: session.attributes.environment,
                sdk: client.map(str::to_owned),
                retention_days: event_retention,
            },
        )
    }

    fn send_metric_message(
        &self,
        organization_id: u64,
        message: MetricKafkaMessage,
    ) -> Result<(), StoreError> {
        let mri = MetricResourceIdentifier::parse(&message.name);
        let topic = match mri.map(|mri| mri.namespace) {
            Ok(MetricNamespace::Transactions) => KafkaTopic::MetricsTransactions,
            Ok(MetricNamespace::Sessions) => KafkaTopic::MetricsSessions,
            Ok(MetricNamespace::Unsupported) | Err(_) => {
                relay_log::with_scope(
                    |scope| {
                        scope.set_extra("metric_message.name", message.name.into());
                    },
                    || {
                        relay_log::error!("Store actor dropping unknown metric usecase");
                    },
                );
                return Ok(());
            }
        };

        relay_log::trace!("Sending metric message to kafka");
        self.produce(topic, organization_id, KafkaMessage::Metric(message))?;
        metric!(
            counter(RelayCounters::ProcessingMessageProduced) += 1,
            event_type = "metric"
        );
        Ok(())
    }

    fn produce_metrics(
        &self,
        org_id: u64,
        project_id: ProjectId,
        item: &Item,
    ) -> Result<(), StoreError> {
        let payload = item.payload();

        for bucket in Bucket::parse_all(&payload).unwrap_or_default() {
            self.send_metric_message(
                org_id,
                MetricKafkaMessage {
                    org_id,
                    project_id,
                    name: bucket.name,
                    value: bucket.value,
                    timestamp: bucket.timestamp,
                    tags: bucket.tags,
                },
            )?;
        }

        Ok(())
    }

    fn send_session_message(
        &self,
        organization_id: u64,
        message: SessionKafkaMessage,
    ) -> Result<(), StoreError> {
        relay_log::trace!("Sending session item to kafka");
        self.produce(
            KafkaTopic::Sessions,
            organization_id,
            KafkaMessage::Session(message),
        )?;
        metric!(
            counter(RelayCounters::ProcessingMessageProduced) += 1,
            event_type = "session"
        );
        Ok(())
    }

    fn produce_profile(
        &self,
        organization_id: u64,
        project_id: ProjectId,
        key_id: Option<u64>,
        start_time: Instant,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = ProfileKafkaMessage {
            organization_id,
            project_id,
            key_id,
            received: UnixTimestamp::from_instant(start_time).as_secs(),
            payload: item.payload(),
        };
        relay_log::trace!("Sending profile to Kafka");
        self.produce(
            KafkaTopic::Profiles,
            organization_id,
            KafkaMessage::Profile(message),
        )?;
        metric!(
            counter(RelayCounters::ProcessingMessageProduced) += 1,
            event_type = "profile"
        );
        Ok(())
    }

    fn produce_replay_event(
        &self,
        replay_id: EventId,
        organization_id: u64,
        project_id: ProjectId,
        start_time: Instant,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = ReplayEventKafkaMessage {
            replay_id,
            project_id,
            retention_days,
            start_time: UnixTimestamp::from_instant(start_time).as_secs(),
            payload: item.payload(),
        };
        relay_log::trace!("Sending replay event to Kafka");
        self.produce(
            KafkaTopic::ReplayEvents,
            organization_id,
            KafkaMessage::ReplayEvent(message),
        )?;
        metric!(
            counter(RelayCounters::ProcessingMessageProduced) += 1,
            event_type = "replay_event"
        );
        Ok(())
    }

    fn produce_replay_recording_chunks(
        &self,
        replay_id: EventId,
        organization_id: u64,
        project_id: ProjectId,
        item: &Item,
    ) -> Result<ChunkedReplayRecording, StoreError> {
        let id = Uuid::new_v4().to_string();

        let mut chunk_index = 0;
        let mut offset = 0;
        let payload = item.payload();
        let size = item.len();

        // This skips chunks for empty replay recordings. The consumer does not require chunks for
        // empty replay recordings. `chunks` will be `0` in this case.
        while offset < size {
            let max_chunk_size = self.config.attachment_chunk_size();
            let chunk_size = std::cmp::min(max_chunk_size, size - offset);

            let replay_recording_chunk_message =
                KafkaMessage::ReplayRecordingChunk(ReplayRecordingChunkKafkaMessage {
                    payload: payload.slice(offset, offset + chunk_size),
                    replay_id,
                    project_id,
                    id: id.clone(),
                    chunk_index,
                });
            self.produce(
                KafkaTopic::ReplayRecordings,
                organization_id,
                replay_recording_chunk_message,
            )?;
            offset += chunk_size;
            chunk_index += 1;
        }

        // The chunk_index is incremented after every loop iteration. After we exit the loop, it
        // is one larger than the last chunk, so it is equal to the number of chunks.

        Ok(ChunkedReplayRecording {
            id,
            chunks: chunk_index,
            size: Some(size),
        })
    }
}

impl Service for StoreService {
    type Interface = Store;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("store forwarder started");

            while let Some(message) = rx.recv().await {
                self.handle_message(message);
            }

            relay_log::info!("store forwarder stopped");
        });
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

/// attributes for Replay Recordings
#[derive(Debug, Serialize)]
struct ChunkedReplayRecording {
    /// The attachment ID within the event.
    ///
    /// The triple `(project_id, event_id, id)` identifies an attachment uniquely.
    id: String,

    /// Number of chunks. Must be greater than zero.
    chunks: usize,

    /// The size of the attachment in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<usize>,
}

/// A hack to make rmp-serde behave more like serde-json when serializing enums.
///
/// Cannot serialize bytes.
///
/// See <https://github.com/3Hren/msgpack-rust/pull/214>
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

#[derive(Clone, Debug, Serialize)]
struct ReplayEventKafkaMessage {
    /// Raw event payload.
    payload: Bytes,
    /// Time at which the event was received by Relay.
    start_time: u64,
    /// The event id.
    replay_id: EventId,
    /// The project id for the current event.
    project_id: ProjectId,
    retention_days: u16,
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

/// Container payload for chunks of attachments.
#[derive(Debug, Serialize)]
struct ReplayRecordingChunkKafkaMessage {
    /// Chunk payload of the replay recording.
    payload: Bytes,
    /// The replay id.
    replay_id: EventId,
    /// The project id for the current replay.
    project_id: ProjectId,
    /// The recording ID within the replay.
    id: String,
    /// Sequence number of chunk. Starts at 0 and ends at `ReplayRecordingKafkaMessage.num_chunks - 1`.
    /// the tuple (id, chunk_index) is the unique identifier for a single chunk.
    chunk_index: usize,
}

#[derive(Debug, Serialize)]
struct ReplayRecordingKafkaMessage {
    /// The replay id.
    replay_id: EventId,
    /// The project id for the current event.
    project_id: ProjectId,
    /// The org id for the current event.
    org_id: u64,
    /// The recording attachment.
    replay_recording: ChunkedReplayRecording,
    retention_days: u16,
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

#[derive(Clone, Debug, Serialize)]
struct MetricKafkaMessage {
    org_id: u64,
    project_id: ProjectId,
    name: String,
    #[serde(flatten)]
    value: BucketValue,
    timestamp: UnixTimestamp,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize)]
struct ProfileKafkaMessage {
    organization_id: u64,
    project_id: ProjectId,
    key_id: Option<u64>,
    received: u64,
    payload: Bytes,
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
    Metric(MetricKafkaMessage),
    Profile(ProfileKafkaMessage),
    ReplayEvent(ReplayEventKafkaMessage),
    ReplayRecording(ReplayRecordingKafkaMessage),
    ReplayRecordingChunk(ReplayRecordingChunkKafkaMessage),
}

impl KafkaMessage {
    fn variant(&self) -> &'static str {
        match self {
            KafkaMessage::Event(_) => "event",
            KafkaMessage::Attachment(_) => "attachment",
            KafkaMessage::AttachmentChunk(_) => "attachment_chunk",
            KafkaMessage::UserReport(_) => "user_report",
            KafkaMessage::Session(_) => "session",
            KafkaMessage::Metric(_) => "metric",
            KafkaMessage::Profile(_) => "profile",
            KafkaMessage::ReplayEvent(_) => "replay_event",
            KafkaMessage::ReplayRecording(_) => "replay_recording",
            KafkaMessage::ReplayRecordingChunk(_) => "replay_recording_chunk",
        }
    }

    /// Returns the partitioning key for this kafka message determining.
    fn key(&self) -> [u8; 16] {
        let mut uuid = match self {
            Self::Event(message) => message.event_id.0,
            Self::Attachment(message) => message.event_id.0,
            Self::AttachmentChunk(message) => message.event_id.0,
            Self::UserReport(message) => message.event_id.0,
            Self::Session(_message) => Uuid::nil(), // Explicit random partitioning for sessions
            Self::Metric(_message) => Uuid::nil(),  // TODO(ja): Determine a partitioning key
            Self::Profile(_message) => Uuid::nil(),
            Self::ReplayEvent(message) => message.replay_id.0,
            Self::ReplayRecording(message) => message.replay_id.0,
            Self::ReplayRecordingChunk(message) => message.replay_id.0,
        };

        if uuid.is_nil() {
            uuid = Uuid::new_v4();
        }

        *uuid.as_bytes()
    }

    /// Serializes the message into its binary format.
    fn serialize(&self) -> Result<Vec<u8>, StoreError> {
        match self {
            KafkaMessage::Session(message) => {
                serde_json::to_vec(message).map_err(StoreError::InvalidJson)
            }
            KafkaMessage::Metric(message) => {
                serde_json::to_vec(message).map_err(StoreError::InvalidJson)
            }
            KafkaMessage::ReplayEvent(message) => {
                serde_json::to_vec(message).map_err(StoreError::InvalidJson)
            }
            _ => rmp_serde::to_vec_named(&self).map_err(StoreError::InvalidMsgPack),
        }
    }
}

/// Determines if the given item is considered slow.
///
/// Slow items must be routed to the `Attachments` topic.
fn is_slow_item(item: &Item) -> bool {
    item.ty() == &ItemType::Attachment || item.ty() == &ItemType::UserReport
}
