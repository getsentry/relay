//! This module contains the service that forwards events and attachments to the Sentry store.
//! The service uses kafka topics to forward data to Sentry

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use once_cell::sync::OnceCell;
use relay_base_schema::data_category::DataCategory;
use relay_base_schema::project::ProjectId;
use relay_common::time::{instant_to_date_time, UnixTimestamp};
use relay_config::Config;
use relay_event_schema::protocol::{
    self, EventId, SessionAggregates, SessionStatus, SessionUpdate,
};

use relay_kafka::{ClientError, KafkaClient, KafkaTopic, Message};
use relay_metrics::{
    Bucket, BucketViewValue, BucketsView, MetricNamespace, MetricResourceIdentifier,
};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Deserializer;
use uuid::Uuid;

use crate::envelope::{AttachmentType, Envelope, Item, ItemType, SourceQuantities};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::statsd::RelayCounters;
use crate::utils::{self, ExtractionMode, ManagedEnvelope};

/// The maximum number of individual session updates generated for each aggregate item.
const MAX_EXPLODED_SESSIONS: usize = 100;

/// Fallback name used for attachment items without a `filename` header.
const UNNAMED_ATTACHMENT: &str = "Unnamed Attachment";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("failed to send the message to kafka")]
    SendFailed(#[from] ClientError),
    #[error("failed to store event because event id was missing")]
    NoEventId,
}

fn make_distinct_id(s: &str) -> Uuid {
    static NAMESPACE: OnceCell<Uuid> = OnceCell::new();
    let namespace =
        NAMESPACE.get_or_init(|| Uuid::new_v5(&Uuid::NAMESPACE_URL, b"https://sentry.io/#did"));

    s.parse()
        .unwrap_or_else(|_| Uuid::new_v5(namespace, s.as_bytes()))
}

struct Producer {
    client: KafkaClient,
}

impl Producer {
    pub fn create(config: &Arc<Config>) -> anyhow::Result<Self> {
        let mut client_builder = KafkaClient::builder();

        for topic in KafkaTopic::iter()
            .filter(|t| **t != KafkaTopic::Outcomes || **t != KafkaTopic::OutcomesBilling)
        {
            let kafka_config = &config.kafka_config(*topic)?;
            client_builder = client_builder.add_kafka_topic_config(*topic, kafka_config)?;
        }

        Ok(Self {
            client: client_builder.build(),
        })
    }
}

/// Publishes an [`Envelope`] to the Sentry core application through Kafka topics.
#[derive(Debug)]
pub struct StoreEnvelope {
    pub envelope: ManagedEnvelope,
}

/// Publishes a list of [`Bucket`]s to the Sentry core application through Kafka topics.
#[derive(Clone, Debug)]
pub struct StoreMetrics {
    pub buckets: Vec<Bucket>,
    pub scoping: Scoping,
    pub retention: u16,
    pub mode: ExtractionMode,
}

/// Service interface for the [`StoreEnvelope`] message.
#[derive(Debug)]
pub enum Store {
    Envelope(StoreEnvelope),
    Metrics(StoreMetrics),
}

impl Interface for Store {}

impl FromMessage<StoreEnvelope> for Store {
    type Response = NoResponse;

    fn from_message(message: StoreEnvelope, _: ()) -> Self {
        Self::Envelope(message)
    }
}

impl FromMessage<StoreMetrics> for Store {
    type Response = NoResponse;

    fn from_message(message: StoreMetrics, _: ()) -> Self {
        Self::Metrics(message)
    }
}

/// Service implementing the [`Store`] interface.
pub struct StoreService {
    config: Arc<Config>,
    outcome_aggregator: Addr<TrackOutcome>,
    producer: Producer,
}

impl StoreService {
    pub fn create(
        config: Arc<Config>,
        outcome_aggregator: Addr<TrackOutcome>,
    ) -> anyhow::Result<Self> {
        let producer = Producer::create(&config)?;
        Ok(Self {
            config,
            outcome_aggregator,
            producer,
        })
    }

    fn handle_message(&self, message: Store) {
        match message {
            Store::Envelope(message) => self.handle_store_envelope(message),
            Store::Metrics(message) => self.handle_store_metrics(message),
        }
    }

    fn handle_store_envelope(&self, message: StoreEnvelope) {
        let StoreEnvelope {
            envelope: mut managed,
        } = message;

        let scoping = managed.scoping();
        let envelope = managed.take_envelope();

        match self.store_envelope(envelope, managed.start_time(), scoping) {
            Ok(()) => managed.accept(),
            Err(error) => {
                managed.reject(Outcome::Invalid(DiscardReason::Internal));
                relay_log::error!(
                    error = &error as &dyn Error,
                    tags.project_key = %scoping.project_key,
                    "failed to store envelope"
                );
            }
        }
    }

    fn store_envelope(
        &self,
        envelope: Box<Envelope>,
        start_time: Instant,
        scoping: Scoping,
    ) -> Result<(), StoreError> {
        let retention = envelope.retention();
        let client = envelope.meta().client();
        let event_id = envelope.event_id();
        let event_item = envelope.get_item_by(|item| {
            matches!(
                item.ty(),
                ItemType::Event
                    | ItemType::Transaction
                    | ItemType::Security
                    | ItemType::UserReportV2
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
                ItemType::MetricBuckets => self.produce_metrics(
                    scoping.organization_id,
                    scoping.project_id,
                    item,
                    retention,
                )?,
                ItemType::Profile => self.produce_profile(
                    scoping.organization_id,
                    scoping.project_id,
                    scoping.key_id,
                    start_time,
                    item,
                )?,
                ItemType::ReplayRecording => {
                    self.produce_replay_recording(event_id, scoping, item, start_time, retention)?
                }
                ItemType::ReplayEvent => self.produce_replay_event(
                    event_id.ok_or(StoreError::NoEventId)?,
                    scoping.organization_id,
                    scoping.project_id,
                    start_time,
                    retention,
                    item,
                )?,
                ItemType::CheckIn => self.produce_check_in(
                    scoping.organization_id,
                    scoping.project_id,
                    start_time,
                    client,
                    retention,
                    item,
                )?,
                ItemType::Span => {
                    self.produce_span(scoping, start_time, event_id, retention, item)?
                }
                _ => {}
            }
        }

        if event_item.is_none() && attachments.is_empty() {
            // No event-related content. All done.
            return Ok(());
        }

        let remote_addr = envelope.meta().client_addr().map(|addr| addr.to_string());

        let kafka_messages = Self::extract_kafka_messages_for_event(
            event_item,
            event_id.ok_or(StoreError::NoEventId)?,
            scoping,
            start_time,
            remote_addr,
            attachments,
        );

        for message in kafka_messages {
            let is_attachment = matches!(&message, KafkaMessage::Attachment(_));

            self.produce(topic, scoping.organization_id, message)?;

            if is_attachment {
                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = "attachment"
                );
            } else if let Some(event_item) = event_item {
                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = &event_item.ty().to_string()
                );
            }
        }

        Ok(())
    }

    fn handle_store_metrics(&self, message: StoreMetrics) {
        let StoreMetrics {
            buckets,
            scoping,
            retention,
            mode,
        } = message;

        let batch_size = self.config.metrics_max_batch_size_bytes();
        let mut dropped = SourceQuantities::default();
        let mut error = None;

        for bucket in buckets {
            // Create a local bucket view to avoid splitting buckets unnecessarily. Since we produce
            // each bucket separately, we only need to split buckets that exceed the size, but not
            // batches.
            for view in BucketsView::new(&[bucket]).by_size(batch_size).flatten() {
                let message = MetricKafkaMessage {
                    org_id: scoping.organization_id,
                    project_id: scoping.project_id,
                    name: view.name(),
                    value: view.value(),
                    timestamp: view.timestamp(),
                    tags: view.tags(),
                    retention_days: retention,
                };

                if let Err(e) = self.send_metric_message(scoping.organization_id, message) {
                    error.get_or_insert(e);
                    dropped += utils::extract_metric_quantities([view], mode);
                }
            }
        }

        if let Some(error) = error {
            relay_log::error!("failed to produce metric buckets: {error}");

            utils::reject_metrics(
                &self.outcome_aggregator,
                dropped,
                scoping,
                Outcome::Invalid(DiscardReason::Internal),
            );
        }
    }

    fn extract_kafka_messages_for_event(
        event_item: Option<&Item>,
        event_id: EventId,
        scoping: Scoping,
        start_time: Instant,
        remote_addr: Option<String>,
        attachments: Vec<ChunkedAttachment>,
    ) -> impl Iterator<Item = KafkaMessage> {
        // There might be a better way to do this:
        let (individual_attachments, inline_attachments) = match event_item {
            Some(event_item) => {
                if matches!(event_item.ty(), ItemType::Transaction) {
                    // Sentry discards inline attachments for transactions, so send them as individual ones instead.
                    (attachments, vec![])
                } else {
                    (vec![], attachments)
                }
            }
            None => (attachments, vec![]),
        };

        let project_id = scoping.project_id;

        let event_iterator = event_item
            .map(|event_item| {
                KafkaMessage::Event(EventKafkaMessage {
                    payload: event_item.payload(),
                    start_time: UnixTimestamp::from_instant(start_time).as_secs(),
                    event_id,
                    project_id,
                    remote_addr,
                    attachments: inline_attachments,
                })
            })
            .into_iter();

        let attachment_iterator = individual_attachments.into_iter().map(move |attachment| {
            KafkaMessage::Attachment(AttachmentKafkaMessage {
                event_id,
                project_id,
                attachment,
            })
        });

        attachment_iterator.chain(event_iterator)
    }

    fn produce(
        &self,
        topic: KafkaTopic,
        organization_id: u64,
        // Takes message by value to ensure it is not being produced twice.
        message: KafkaMessage,
    ) -> Result<(), StoreError> {
        relay_log::trace!("Sending kafka message of type {}", message.variant());

        self.producer
            .client
            .send_message(topic, organization_id, &message)?;

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
                payload: payload.slice(offset..offset + chunk_size),
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
            attachment_type: item.attachment_type().cloned().unwrap_or_default(),
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
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            "failed to store session"
                        );
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
                status: session.status.clone(),
                errors: session.errors.clamp(
                    (session.status == SessionStatus::Crashed) as _,
                    u16::MAX.into(),
                ) as _,
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
        let mri = MetricResourceIdentifier::parse(message.name);
        let (topic, namespace) = match mri.map(|mri| mri.namespace) {
            Ok(namespace @ MetricNamespace::Sessions) => (KafkaTopic::MetricsSessions, namespace),
            Ok(MetricNamespace::Unsupported) | Err(_) => {
                relay_log::with_scope(
                    |scope| scope.set_extra("metric_message.name", message.name.into()),
                    || relay_log::error!("store service dropping unknown metric usecase"),
                );
                return Ok(());
            }
            Ok(namespace) => (KafkaTopic::MetricsGeneric, namespace),
        };
        let headers = BTreeMap::from([("namespace".to_string(), namespace.to_string())]);

        self.produce(
            topic,
            organization_id,
            KafkaMessage::Metric { headers, message },
        )?;
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
        retention: u16,
    ) -> Result<(), StoreError> {
        let payload = item.payload();

        for bucket in serde_json::from_slice::<Vec<Bucket>>(&payload).unwrap_or_default() {
            self.send_metric_message(
                org_id,
                MetricKafkaMessage {
                    org_id,
                    project_id,
                    name: bucket.name.as_str(),
                    value: (&bucket.value).into(),
                    timestamp: bucket.timestamp,
                    tags: &bucket.tags,
                    retention_days: retention,
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
            headers: BTreeMap::from([(
                "sampled".to_string(),
                if item.sampled() { "true" } else { "false" }.to_owned(),
            )]),
            payload: item.payload(),
        };
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

    fn produce_replay_recording(
        &self,
        event_id: Option<EventId>,
        scoping: Scoping,
        item: &Item,
        start_time: Instant,
        retention: u16,
    ) -> Result<(), StoreError> {
        // 2000 bytes are reserved for the message metadata.
        let max_message_metadata_size = 2000;

        // Remaining bytes can be filled by the payload.
        let max_payload_size = self.config.max_replay_message_size() - max_message_metadata_size;

        if item.payload().len() < max_payload_size {
            let message =
                KafkaMessage::ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage {
                    replay_id: event_id.ok_or(StoreError::NoEventId)?,
                    project_id: scoping.project_id,
                    key_id: scoping.key_id,
                    org_id: scoping.organization_id,
                    received: UnixTimestamp::from_instant(start_time).as_secs(),
                    retention_days: retention,
                    payload: item.payload(),
                });

            self.produce(
                KafkaTopic::ReplayRecordings,
                scoping.organization_id,
                message,
            )?;

            metric!(
                counter(RelayCounters::ProcessingMessageProduced) += 1,
                event_type = "replay_recording_not_chunked"
            );
        } else {
            relay_log::warn!("replay_recording over maximum size.");
        };

        Ok(())
    }

    fn produce_check_in(
        &self,
        organization_id: u64,
        project_id: ProjectId,
        start_time: Instant,
        client: Option<&str>,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = KafkaMessage::CheckIn(CheckInKafkaMessage {
            message_type: CheckInMessageType::CheckIn,
            project_id,
            retention_days,
            start_time: UnixTimestamp::from_instant(start_time).as_secs(),
            sdk: client.map(str::to_owned),
            payload: item.payload(),
            routing_key_hint: item.routing_hint(),
        });

        self.produce(KafkaTopic::Monitors, organization_id, message)?;

        metric!(
            counter(RelayCounters::ProcessingMessageProduced) += 1,
            event_type = "check_in"
        );

        Ok(())
    }

    fn produce_span(
        &self,
        scoping: Scoping,
        start_time: Instant,
        event_id: Option<EventId>,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let payload = item.payload();
        let d = &mut Deserializer::from_slice(&payload);
        let mut span: SpanKafkaMessage = match serde_path_to_error::deserialize(d) {
            Ok(span) => span,
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn std::error::Error,
                    "failed to parse span"
                );
                self.outcome_aggregator.send(TrackOutcome {
                    category: DataCategory::SpanIndexed,
                    event_id: None,
                    outcome: Outcome::Invalid(DiscardReason::InvalidSpan),
                    quantity: 1,
                    remote_addr: None,
                    scoping,
                    timestamp: instant_to_date_time(start_time),
                });
                return Ok(());
            }
        };

        span.duration_ms = ((span.end_timestamp - span.start_timestamp) * 1e3) as u32;
        span.event_id = event_id;
        span.project_id = scoping.project_id.value();
        span.retention_days = retention_days;
        span.start_timestamp_ms = (span.start_timestamp * 1e3) as u64;

        if let Some(measurements) = &mut span.measurements {
            measurements.retain(|_, v| {
                v.as_ref()
                    .and_then(|v| v.value)
                    .map_or(false, f64::is_finite)
            });
        }

        if let Some(metrics_summary) = &mut span.metrics_summary {
            metrics_summary.retain(|_, mut v| {
                if let Some(v) = &mut v {
                    v.retain(|v| {
                        if let Some(v) = v {
                            return v.min.is_some()
                                || v.max.is_some()
                                || v.sum.is_some()
                                || v.count.is_some();
                        }
                        false
                    });
                    !v.is_empty()
                } else {
                    false
                }
            });

            let group = span
                .sentry_tags
                .as_ref()
                .and_then(|sentry_tags| sentry_tags.get("group").cloned())
                .unwrap_or_default();

            for (mri, summaries) in metrics_summary {
                let Some(summaries) = summaries else {
                    continue;
                };
                for summary in summaries {
                    let Some(summary) = summary else {
                        continue;
                    };
                    // Ignore immediate errors on produce.
                    if let Err(error) = self.produce(
                        KafkaTopic::MetricsSummaries,
                        scoping.organization_id,
                        KafkaMessage::MetricsSummary(MetricsSummaryKafkaMessage {
                            count: summary.count,
                            duration_ms: span.duration_ms,
                            end_timestamp: span.end_timestamp,
                            group: &group,
                            is_segment: span.is_segment,
                            max: summary.max,
                            mri,
                            min: summary.min,
                            project_id: span.project_id,
                            retention_days: span.retention_days,
                            segment_id: span.segment_id.unwrap_or_default(),
                            span_id: span.span_id,
                            sum: summary.sum,
                            tags: summary.tags,
                            trace_id: span.trace_id,
                        }),
                    ) {
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            "failed to push metrics summary to kafka",
                        );
                    }
                }
            }
            span.metrics_summary = None;
        }

        self.produce(
            KafkaTopic::Spans,
            scoping.organization_id,
            KafkaMessage::Span(span),
        )?;

        self.outcome_aggregator.send(TrackOutcome {
            category: DataCategory::SpanIndexed,
            event_id: None,
            outcome: Outcome::Accepted,
            quantity: 1,
            remote_addr: None,
            scoping,
            timestamp: instant_to_date_time(start_time),
        });

        metric!(
            counter(RelayCounters::ProcessingMessageProduced) += 1,
            event_type = "span"
        );

        Ok(())
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
        .map_err(|e| serde::ser::Error::custom(e.to_string()))?
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

#[derive(Debug, Serialize)]
struct ReplayEventKafkaMessage {
    /// Raw event payload.
    payload: Bytes,
    /// Time at which the event was received by Relay.
    start_time: u64,
    /// The event id.
    replay_id: EventId,
    /// The project id for the current event.
    project_id: ProjectId,
    // Number of days to retain.
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
struct ReplayRecordingChunkMeta {
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

#[derive(Debug, Serialize)]
struct ReplayRecordingKafkaMessage {
    replay_id: EventId,
    /// The key_id for the current recording.
    key_id: Option<u64>,
    /// The org id for the current recording.
    org_id: u64,
    /// The project id for the current recording.
    project_id: ProjectId,
    /// The timestamp of when the recording was Received by relay
    received: u64,
    // Number of days to retain.
    retention_days: u16,
    /// The recording attachment.
    replay_recording: ReplayRecordingChunkMeta,
}

#[derive(Debug, Serialize)]
struct ReplayRecordingNotChunkedKafkaMessage {
    replay_id: EventId,
    key_id: Option<u64>,
    org_id: u64,
    project_id: ProjectId,
    received: u64,
    retention_days: u16,
    payload: Bytes,
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
struct MetricKafkaMessage<'a> {
    org_id: u64,
    project_id: ProjectId,
    name: &'a str,
    #[serde(flatten)]
    value: BucketViewValue<'a>,
    timestamp: UnixTimestamp,
    tags: &'a BTreeMap<String, String>,
    retention_days: u16,
}

#[derive(Clone, Debug, Serialize)]
struct ProfileKafkaMessage {
    organization_id: u64,
    project_id: ProjectId,
    key_id: Option<u64>,
    received: u64,
    #[serde(skip)]
    headers: BTreeMap<String, String>,
    payload: Bytes,
}

/// Used to discriminate cron monitor ingestion messages.
///
/// There are two types of messages that end up in the ingest-monitors kafka topic, "check_in" (the
/// ones produced here in relay) and "clock_pulse" messages, which are produced externally and are
/// intended to ensure the clock continues to run even when ingestion volume drops.
#[allow(dead_code)]
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckInMessageType {
    ClockPulse,
    CheckIn,
}

#[derive(Debug, Serialize)]
struct CheckInKafkaMessage {
    #[serde(skip)]
    routing_key_hint: Option<Uuid>,

    /// Used by the consumer to discrinminate the message.
    message_type: CheckInMessageType,
    /// Raw event payload.
    payload: Bytes,
    /// Time at which the event was received by Relay.
    start_time: u64,
    /// The SDK client which produced the event.
    sdk: Option<String>,
    /// The project id for the current event.
    project_id: ProjectId,
    /// Number of days to retain.
    retention_days: u16,
}

#[derive(Debug, Deserialize, Serialize)]
struct SpanMeasurement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SpanMetricsSummary<'a> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sum: Option<f64>,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    tags: Option<&'a RawValue>,
}

type SpanMetricsSummaries<'a> = Vec<Option<SpanMetricsSummary<'a>>>;

#[derive(Debug, Deserialize, Serialize)]
struct SpanKafkaMessage<'a> {
    #[serde(skip_serializing)]
    start_timestamp: f64,
    #[serde(rename(deserialize = "timestamp"), skip_serializing)]
    end_timestamp: f64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<&'a RawValue>,
    #[serde(default)]
    duration_ms: u32,
    /// The ID of the transaction event associated to this span, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_id: Option<EventId>,
    #[serde(rename(deserialize = "exclusive_time"))]
    exclusive_time_ms: f64,
    is_segment: bool,

    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    measurements: Option<BTreeMap<Cow<'a, str>, Option<SpanMeasurement>>>,
    #[serde(
        borrow,
        default,
        rename = "_metrics_summary",
        skip_serializing_if = "Option::is_none"
    )]
    metrics_summary: Option<BTreeMap<Cow<'a, str>, Option<SpanMetricsSummaries<'a>>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<&'a str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    profile_id: Option<&'a str>,
    /// The numeric ID of the project.
    #[serde(default)]
    project_id: u64,
    /// Time at which the event was received by Relay. Not to be confused with `start_timestamp_ms`.
    received: f64,
    /// Number of days until these data should be deleted.
    #[serde(default)]
    retention_days: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segment_id: Option<&'a str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sentry_tags: Option<BTreeMap<&'a str, String>>,
    span_id: &'a str,
    #[serde(default)]
    start_timestamp_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tags: Option<&'a RawValue>,
    trace_id: &'a str,
}

#[derive(Debug, Deserialize, Serialize)]
struct MetricsSummaryKafkaMessage<'a> {
    duration_ms: u32,
    end_timestamp: f64,
    group: &'a str,
    is_segment: bool,
    mri: &'a str,
    project_id: u64,
    retention_days: u16,
    segment_id: &'a str,
    span_id: &'a str,
    trace_id: &'a str,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tags: Option<&'a RawValue>,
}

/// An enum over all possible ingest messages.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
enum KafkaMessage<'a> {
    Event(EventKafkaMessage),
    Attachment(AttachmentKafkaMessage),
    AttachmentChunk(AttachmentChunkKafkaMessage),
    UserReport(UserReportKafkaMessage),
    Session(SessionKafkaMessage),
    Metric {
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: MetricKafkaMessage<'a>,
    },
    Profile(ProfileKafkaMessage),
    ReplayEvent(ReplayEventKafkaMessage),
    ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage),
    CheckIn(CheckInKafkaMessage),
    Span(SpanKafkaMessage<'a>),
    MetricsSummary(MetricsSummaryKafkaMessage<'a>),
}

impl Message for KafkaMessage<'_> {
    fn variant(&self) -> &'static str {
        match self {
            KafkaMessage::Event(_) => "event",
            KafkaMessage::Attachment(_) => "attachment",
            KafkaMessage::AttachmentChunk(_) => "attachment_chunk",
            KafkaMessage::UserReport(_) => "user_report",
            KafkaMessage::Session(_) => "session",
            KafkaMessage::Metric { .. } => "metric",
            KafkaMessage::Profile(_) => "profile",
            KafkaMessage::ReplayEvent(_) => "replay_event",
            KafkaMessage::ReplayRecordingNotChunked(_) => "replay_recording_not_chunked",
            KafkaMessage::CheckIn(_) => "check_in",
            KafkaMessage::Span(_) => "span",
            KafkaMessage::MetricsSummary(_) => "metrics_summary",
        }
    }

    /// Returns the partitioning key for this kafka message determining.
    fn key(&self) -> [u8; 16] {
        let mut uuid = match self {
            Self::Event(message) => message.event_id.0,
            Self::Attachment(message) => message.event_id.0,
            Self::AttachmentChunk(message) => message.event_id.0,
            Self::UserReport(message) => message.event_id.0,
            Self::ReplayEvent(message) => message.replay_id.0,

            // Monitor check-ins use the hinted UUID passed through from the Envelope.
            //
            // XXX(epurkhiser): In the future it would be better if all KafkaMessage's would
            // recieve the routing_key_hint form their envelopes.
            Self::CheckIn(message) => message.routing_key_hint.unwrap_or_else(Uuid::nil),

            // Random partitioning
            Self::Session(_)
            | Self::Profile(_)
            | Self::ReplayRecordingNotChunked(_)
            | Self::Span(_)
            | Self::MetricsSummary(_) => Uuid::nil(),

            // TODO(ja): Determine a partitioning key
            Self::Metric { .. } => Uuid::nil(),
        };

        if uuid.is_nil() {
            uuid = Uuid::new_v4();
        }

        *uuid.as_bytes()
    }

    fn headers(&self) -> Option<&BTreeMap<String, String>> {
        match &self {
            KafkaMessage::Metric { headers, .. } => {
                if !headers.is_empty() {
                    return Some(headers);
                }
                None
            }
            KafkaMessage::Profile(profile) => {
                if !profile.headers.is_empty() {
                    return Some(&profile.headers);
                }
                None
            }
            _ => None,
        }
    }

    /// Serializes the message into its binary format.
    fn serialize(&self) -> Result<Vec<u8>, ClientError> {
        match self {
            KafkaMessage::Session(message) => {
                serde_json::to_vec(message).map_err(ClientError::InvalidJson)
            }
            KafkaMessage::Metric { message, .. } => {
                serde_json::to_vec(message).map_err(ClientError::InvalidJson)
            }
            KafkaMessage::ReplayEvent(message) => {
                serde_json::to_vec(message).map_err(ClientError::InvalidJson)
            }
            KafkaMessage::Span(message) => {
                serde_json::to_vec(message).map_err(ClientError::InvalidJson)
            }
            KafkaMessage::MetricsSummary(message) => {
                serde_json::to_vec(message).map_err(ClientError::InvalidJson)
            }

            _ => rmp_serde::to_vec_named(&self).map_err(ClientError::InvalidMsgPack),
        }
    }
}

/// Determines if the given item is considered slow.
///
/// Slow items must be routed to the `Attachments` topic.
fn is_slow_item(item: &Item) -> bool {
    item.ty() == &ItemType::Attachment || item.ty() == &ItemType::UserReport
}

#[cfg(test)]
mod tests {
    use relay_base_schema::project::ProjectKey;

    use super::*;

    /// Helper function to get the arguments for the `fn extract_kafka_messages(...)` method.
    fn arguments_extract_kafka_msgs() -> (Instant, EventId, Scoping, Vec<ChunkedAttachment>) {
        let start_time = Instant::now();
        let event_id = EventId::new();
        let scoping = Scoping {
            organization_id: 42,
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        let attachment_vec = {
            let item = Item::new(ItemType::Attachment);

            vec![ChunkedAttachment {
                id: Uuid::new_v4().to_string(),
                name: UNNAMED_ATTACHMENT.to_owned(),
                content_type: item
                    .content_type()
                    .map(|content_type| content_type.as_str().to_owned()),
                attachment_type: item.attachment_type().cloned().unwrap_or_default(),
                chunks: 0,
                size: None,
                rate_limited: Some(item.rate_limited()),
            }]
        };

        (start_time, event_id, scoping, attachment_vec)
    }

    #[test]
    fn test_return_attachments_when_missing_event_item() {
        let (start_time, event_id, scoping, attachment_vec) = arguments_extract_kafka_msgs();
        let number_of_attachments = attachment_vec.len();

        let kafka_messages = StoreService::extract_kafka_messages_for_event(
            None,
            event_id,
            scoping,
            start_time,
            None,
            attachment_vec,
        );

        assert!(
            kafka_messages
                .filter(|msg| matches!(msg, KafkaMessage::Attachment(_)))
                .count()
                == number_of_attachments
        );
    }

    /// If there is an event_item, and it is of type transaction, then the attachments should not
    /// be sent together with the event but rather as standalones.
    #[test]
    fn test_send_standalone_attachments_when_transaction() {
        let (start_time, event_id, scoping, attachment_vec) = arguments_extract_kafka_msgs();
        let number_of_attachments = attachment_vec.len();

        let item = Item::new(ItemType::Transaction);
        let event_item = Some(&item);

        let kafka_messages = StoreService::extract_kafka_messages_for_event(
            event_item,
            event_id,
            scoping,
            start_time,
            None,
            attachment_vec,
        );

        let (event, standalone_attachments): (Vec<_>, Vec<_>) =
            kafka_messages.partition(|item| match item {
                KafkaMessage::Event(_) => true,
                KafkaMessage::Attachment(_) => false,
                _ => panic!("only expected events or attachment type"),
            });

        // Tests that the event does not contain any attachments.
        let event = &event[0];
        if let KafkaMessage::Event(event) = event {
            assert!(event.attachments.is_empty());
        } else {
            panic!("No event found")
        }

        // Tests that the attachment we sent to `extract_kafka_messages_for_event` is in the
        // standalone attachments.
        assert!(standalone_attachments.len() == number_of_attachments);
    }

    /// If there is an event_item, and it is not a transaction. The attachments should be kept in
    /// the event and not be returned as stand-alone attachments.
    #[test]
    fn test_store_attachment_in_event_when_not_a_transaction() {
        let (start_time, event_id, scoping, attachment_vec) = arguments_extract_kafka_msgs();
        let number_of_attachments = attachment_vec.len();

        let item = Item::new(ItemType::Event);
        let event_item = Some(&item);

        let kafka_messages = StoreService::extract_kafka_messages_for_event(
            event_item,
            event_id,
            scoping,
            start_time,
            None,
            attachment_vec,
        );

        let (event, standalone_attachments): (Vec<_>, Vec<_>) =
            kafka_messages.partition(|item| match item {
                KafkaMessage::Event(_) => true,
                KafkaMessage::Attachment(_) => false,
                _ => panic!("only expected events or attachment type"),
            });

        // Because it's not a transaction event, the attachment should be part of the event,
        // and therefore the standalone_attachments vec should be empty.
        assert!(standalone_attachments.is_empty());

        // Checks that the attachment is part of the event.
        let event = &event[0];
        if let KafkaMessage::Event(event) = event {
            assert!(event.attachments.len() == number_of_attachments);
        } else {
            panic!("No event found")
        }
    }
}
