//! This module contains the service that forwards events and attachments to the Sentry store.
//! The service uses Kafka topics to forward data to Sentry

use relay_base_schema::organization::OrganizationId;
use serde::ser::SerializeMap;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use futures::future::BoxFuture;
use relay_base_schema::data_category::DataCategory;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_config::Config;
use relay_event_schema::protocol::{EventId, VALID_PLATFORMS};

use crate::envelope::{AttachmentType, Envelope, Item, ItemType};
use prost::Message as _;
use prost_types::Timestamp;
use relay_kafka::{ClientError, KafkaClient, KafkaTopic, Message};
use relay_metrics::{
    Bucket, BucketView, BucketViewValue, BucketsView, ByNamespace, FiniteF64, GaugeValue,
    MetricName, MetricNamespace, SetView,
};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use relay_threading::AsyncPool;
use sentry_protos::snuba::v1::any_value::Value;
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use serde_json::value::RawValue;
use uuid::Uuid;

use crate::metrics::{ArrayEncoding, BucketEncoder, MetricOutcomes};
use crate::service::ServiceError;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::Processed;
use crate::statsd::{RelayCounters, RelayGauges, RelayTimers};
use crate::utils::{FormDataIter, TypedEnvelope};

/// Fallback name used for attachment items without a `filename` header.
const UNNAMED_ATTACHMENT: &str = "Unnamed Attachment";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("failed to send the message to kafka: {0}")]
    SendFailed(#[from] ClientError),
    #[error("failed to encode data: {0}")]
    EncodingFailed(std::io::Error),
    #[error("failed to store event because event id was missing")]
    NoEventId,
}

struct Producer {
    client: KafkaClient,
}

impl Producer {
    pub fn create(config: &Config) -> anyhow::Result<Self> {
        let mut client_builder = KafkaClient::builder();

        for topic in KafkaTopic::iter().filter(|t| {
            // Outcomes should not be sent from the store forwarder.
            // See `KafkaOutcomesProducer`.
            **t != KafkaTopic::Outcomes && **t != KafkaTopic::OutcomesBilling
        }) {
            let kafka_config = &config.kafka_config(*topic)?;
            client_builder = client_builder
                .add_kafka_topic_config(*topic, kafka_config, config.kafka_validate_topics())
                .map_err(|e| ServiceError::Kafka(e.to_string()))?;
        }

        Ok(Self {
            client: client_builder.build(),
        })
    }
}

/// Publishes an [`Envelope`] to the Sentry core application through Kafka topics.
#[derive(Debug)]
pub struct StoreEnvelope {
    pub envelope: TypedEnvelope<Processed>,
}

/// Publishes a list of [`Bucket`]s to the Sentry core application through Kafka topics.
#[derive(Clone, Debug)]
pub struct StoreMetrics {
    pub buckets: Vec<Bucket>,
    pub scoping: Scoping,
    pub retention: u16,
}

/// The asynchronous thread pool used for scheduling storing tasks in the envelope store.
pub type StoreServicePool = AsyncPool<BoxFuture<'static, ()>>;

/// Service interface for the [`StoreEnvelope`] message.
#[derive(Debug)]
pub enum Store {
    Envelope(StoreEnvelope),
    Metrics(StoreMetrics),
}

impl Store {
    /// Returns the name of the message variant.
    fn variant(&self) -> &'static str {
        match self {
            Store::Envelope(_) => "envelope",
            Store::Metrics(_) => "metrics",
        }
    }
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
    pool: StoreServicePool,
    config: Arc<Config>,
    global_config: GlobalConfigHandle,
    outcome_aggregator: Addr<TrackOutcome>,
    metric_outcomes: MetricOutcomes,
    producer: Producer,
}

impl StoreService {
    pub fn create(
        pool: StoreServicePool,
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        outcome_aggregator: Addr<TrackOutcome>,
        metric_outcomes: MetricOutcomes,
    ) -> anyhow::Result<Self> {
        let producer = Producer::create(&config)?;
        Ok(Self {
            pool,
            config,
            global_config,
            outcome_aggregator,
            metric_outcomes,
            producer,
        })
    }

    fn handle_message(&self, message: Store) {
        let ty = message.variant();
        relay_statsd::metric!(timer(RelayTimers::StoreServiceDuration), message = ty, {
            match message {
                Store::Envelope(message) => self.handle_store_envelope(message),
                Store::Metrics(message) => self.handle_store_metrics(message),
            }
        })
    }

    fn handle_store_envelope(&self, message: StoreEnvelope) {
        let StoreEnvelope {
            envelope: mut managed,
        } = message;

        let scoping = managed.scoping();
        let envelope = managed.take_envelope();

        match self.store_envelope(envelope, managed.received_at(), scoping) {
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
        mut envelope: Box<Envelope>,
        received_at: DateTime<Utc>,
        scoping: Scoping,
    ) -> Result<(), StoreError> {
        let retention = envelope.retention();

        let event_id = envelope.event_id();
        let event_item = envelope.as_mut().take_item_by(|item| {
            matches!(
                item.ty(),
                ItemType::Event | ItemType::Transaction | ItemType::Security
            )
        });
        let event_type = event_item.as_ref().map(|item| item.ty());

        let topic = if envelope.get_item_by(is_slow_item).is_some() {
            KafkaTopic::Attachments
        } else if event_item.as_ref().map(|x| x.ty()) == Some(&ItemType::Transaction) {
            KafkaTopic::Transactions
        } else {
            KafkaTopic::Events
        };

        let send_individual_attachments = matches!(event_type, None | Some(&ItemType::Transaction));

        let mut attachments = Vec::new();
        let mut replay_event = None;
        let mut replay_recording = None;

        for item in envelope.items() {
            match item.ty() {
                ItemType::Attachment => {
                    debug_assert!(topic == KafkaTopic::Attachments);
                    if let Some(attachment) = self.produce_attachment(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.project_id,
                        item,
                        send_individual_attachments,
                    )? {
                        attachments.push(attachment);
                    }
                }
                ItemType::UserReport => {
                    debug_assert!(topic == KafkaTopic::Attachments);
                    self.produce_user_report(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.project_id,
                        received_at,
                        item,
                    )?;
                }
                ItemType::UserReportV2 => {
                    let remote_addr = envelope.meta().client_addr().map(|addr| addr.to_string());
                    self.produce_user_report_v2(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.project_id,
                        received_at,
                        item,
                        remote_addr,
                    )?;
                }
                ItemType::Profile => self.produce_profile(
                    scoping.organization_id,
                    scoping.project_id,
                    scoping.key_id,
                    received_at,
                    retention,
                    item,
                )?,
                ItemType::ReplayVideo => {
                    self.produce_replay_video(
                        event_id,
                        scoping,
                        item.payload(),
                        received_at,
                        retention,
                    )?;
                }
                ItemType::ReplayRecording => {
                    replay_recording = Some(item);
                }
                ItemType::ReplayEvent => {
                    if item.replay_combined_payload() {
                        replay_event = Some(item);
                    }

                    self.produce_replay_event(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.project_id,
                        received_at,
                        retention,
                        &item.payload(),
                    )?;
                }
                ItemType::CheckIn => {
                    let client = envelope.meta().client();
                    self.produce_check_in(scoping.project_id, received_at, client, retention, item)?
                }
                ItemType::Span => {
                    self.produce_span(scoping, received_at, event_id, retention, item)?
                }
                ItemType::Log => self.produce_log(scoping, received_at, retention, item)?,
                ItemType::ProfileChunk => self.produce_profile_chunk(
                    scoping.organization_id,
                    scoping.project_id,
                    received_at,
                    retention,
                    item,
                )?,
                other => {
                    let event_type = event_item.as_ref().map(|item| item.ty().as_str());
                    let item_types = envelope
                        .items()
                        .map(|item| item.ty().as_str())
                        .collect::<Vec<_>>();
                    let attachment_types = envelope
                        .items()
                        .map(|item| {
                            item.attachment_type()
                                .map(|t| t.to_string())
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>();

                    relay_log::with_scope(
                        |scope| {
                            scope.set_extra("item_types", item_types.into());
                            scope.set_extra("attachment_types", attachment_types.into());
                            if other == &ItemType::FormData {
                                let payload = item.payload();
                                let form_data_keys = FormDataIter::new(&payload)
                                    .map(|entry| entry.key())
                                    .collect::<Vec<_>>();
                                scope.set_extra("form_data_keys", form_data_keys.into());
                            }
                        },
                        || {
                            relay_log::error!(
                                tags.project_key = %scoping.project_key,
                                tags.event_type = event_type.unwrap_or("none"),
                                "StoreService received unexpected item type: {other}"
                            )
                        },
                    )
                }
            }
        }

        if let Some(recording) = replay_recording {
            // If a recording item type was seen we produce it to Kafka with the replay-event
            // payload (should it have been provided).
            //
            // The replay_video value is always specified as `None`. We do not allow separate
            // item types for `ReplayVideo` events.
            let replay_event = replay_event.map(|rv| rv.payload());
            self.produce_replay_recording(
                event_id,
                scoping,
                &recording.payload(),
                replay_event.as_deref(),
                None,
                received_at,
                retention,
            )?;
        }

        if let Some(event_item) = event_item {
            let event_id = event_id.ok_or(StoreError::NoEventId)?;
            let project_id = scoping.project_id;
            let remote_addr = envelope.meta().client_addr().map(|addr| addr.to_string());

            self.produce(
                topic,
                KafkaMessage::Event(EventKafkaMessage {
                    payload: event_item.payload(),
                    start_time: safe_timestamp(received_at),
                    event_id,
                    project_id,
                    remote_addr,
                    attachments,
                }),
            )?;
        } else {
            debug_assert!(attachments.is_empty());
        }

        Ok(())
    }

    fn handle_store_metrics(&self, message: StoreMetrics) {
        let StoreMetrics {
            buckets,
            scoping,
            retention,
        } = message;

        let batch_size = self.config.metrics_max_batch_size_bytes();
        let mut error = None;

        let global_config = self.global_config.current();
        let mut encoder = BucketEncoder::new(&global_config);

        let now = UnixTimestamp::now();
        let mut delay_stats = ByNamespace::<(u64, u64, u64)>::default();

        for mut bucket in buckets {
            let namespace = encoder.prepare(&mut bucket);

            if let Some(received_at) = bucket.metadata.received_at {
                let delay = now.as_secs().saturating_sub(received_at.as_secs());
                let (total, count, max) = delay_stats.get_mut(namespace);
                *total += delay;
                *count += 1;
                *max = (*max).max(delay);
            }

            // Create a local bucket view to avoid splitting buckets unnecessarily. Since we produce
            // each bucket separately, we only need to split buckets that exceed the size, but not
            // batches.
            for view in BucketsView::new(std::slice::from_ref(&bucket))
                .by_size(batch_size)
                .flatten()
            {
                let message = self.create_metric_message(
                    scoping.organization_id,
                    scoping.project_id,
                    &mut encoder,
                    namespace,
                    &view,
                    retention,
                );

                let result =
                    message.and_then(|message| self.send_metric_message(namespace, message));

                let outcome = match result {
                    Ok(()) => Outcome::Accepted,
                    Err(e) => {
                        error.get_or_insert(e);
                        Outcome::Invalid(DiscardReason::Internal)
                    }
                };

                self.metric_outcomes.track(scoping, &[view], outcome);
            }
        }

        if let Some(error) = error {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                "failed to produce metric buckets: {error}"
            );
        }

        for (namespace, (total, count, max)) in delay_stats {
            if count == 0 {
                continue;
            }
            metric!(
                counter(RelayCounters::MetricDelaySum) += total,
                namespace = namespace.as_str()
            );
            metric!(
                counter(RelayCounters::MetricDelayCount) += count,
                namespace = namespace.as_str()
            );
            metric!(
                gauge(RelayGauges::MetricDelayMax) = max,
                namespace = namespace.as_str()
            );
        }
    }

    fn create_metric_message<'a>(
        &self,
        organization_id: OrganizationId,
        project_id: ProjectId,
        encoder: &'a mut BucketEncoder,
        namespace: MetricNamespace,
        view: &BucketView<'a>,
        retention_days: u16,
    ) -> Result<MetricKafkaMessage<'a>, StoreError> {
        let value = match view.value() {
            BucketViewValue::Counter(c) => MetricValue::Counter(c),
            BucketViewValue::Distribution(data) => MetricValue::Distribution(
                encoder
                    .encode_distribution(namespace, data)
                    .map_err(StoreError::EncodingFailed)?,
            ),
            BucketViewValue::Set(data) => MetricValue::Set(
                encoder
                    .encode_set(namespace, data)
                    .map_err(StoreError::EncodingFailed)?,
            ),
            BucketViewValue::Gauge(g) => MetricValue::Gauge(g),
        };

        Ok(MetricKafkaMessage {
            org_id: organization_id,
            project_id,
            name: view.name(),
            value,
            timestamp: view.timestamp(),
            tags: view.tags(),
            retention_days,
            received_at: view.metadata().received_at,
        })
    }

    fn produce(
        &self,
        topic: KafkaTopic,
        // Takes message by value to ensure it is not being produced twice.
        message: KafkaMessage,
    ) -> Result<(), StoreError> {
        relay_log::trace!("Sending kafka message of type {}", message.variant());

        let topic_name = self.producer.client.send_message(topic, &message)?;

        match &message {
            KafkaMessage::Metric {
                message: metric, ..
            } => {
                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = message.variant(),
                    topic = topic_name,
                    metric_type = metric.value.variant(),
                    metric_encoding = metric.value.encoding().unwrap_or(""),
                );
            }
            KafkaMessage::Span { message: span, .. } => {
                let is_segment = span.is_segment;
                let has_parent = span.parent_span_id.is_some();
                let platform = VALID_PLATFORMS.iter().find(|p| *p == &span.platform);

                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = message.variant(),
                    topic = topic_name,
                    platform = platform.unwrap_or(&""),
                    is_segment = bool_to_str(is_segment),
                    has_parent = bool_to_str(has_parent),
                    topic = topic_name,
                );
            }
            KafkaMessage::ReplayRecordingNotChunked(replay) => {
                let has_video = replay.replay_video.is_some();

                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = message.variant(),
                    topic = topic_name,
                    has_video = bool_to_str(has_video),
                );
            }
            message => {
                metric!(
                    counter(RelayCounters::ProcessingMessageProduced) += 1,
                    event_type = message.variant(),
                    topic = topic_name,
                );
            }
        }

        Ok(())
    }

    /// Produces Kafka messages for the content and metadata of an attachment item.
    ///
    /// The `send_individual_attachments` controls whether the metadata of an attachment
    /// is produced directly as an individual `attachment` message, or returned from this function
    /// to be later sent as part of an `event` message.
    ///
    /// Attachment contents are chunked and sent as multiple `attachment_chunk` messages,
    /// unless the `send_individual_attachments` flag is set, and the content is small enough
    /// to fit inside a message.
    /// In that case, no `attachment_chunk` is produced, but the content is sent as part
    /// of the `attachment` message instead.
    fn produce_attachment(
        &self,
        event_id: EventId,
        project_id: ProjectId,
        item: &Item,
        send_individual_attachments: bool,
    ) -> Result<Option<ChunkedAttachment>, StoreError> {
        let id = Uuid::new_v4().to_string();

        let mut chunk_index = 0;
        let payload = item.payload();
        let size = item.len();
        let max_chunk_size = self.config.attachment_chunk_size();

        // When sending individual attachments, and we have a single chunk, we want to send the
        // `data` inline in the `attachment` message.
        // This avoids a needless roundtrip through the attachments cache on the Sentry side.
        let data = if send_individual_attachments && size < max_chunk_size {
            (size > 0).then_some(payload)
        } else {
            let mut offset = 0;
            // This skips chunks for empty attachments. The consumer does not require chunks for
            // empty attachments. `chunks` will be `0` in this case.
            while offset < size {
                let chunk_size = std::cmp::min(max_chunk_size, size - offset);
                let chunk_message = AttachmentChunkKafkaMessage {
                    payload: payload.slice(offset..offset + chunk_size),
                    event_id,
                    project_id,
                    id: id.clone(),
                    chunk_index,
                };

                self.produce(
                    KafkaTopic::Attachments,
                    KafkaMessage::AttachmentChunk(chunk_message),
                )?;
                offset += chunk_size;
                chunk_index += 1;
            }
            None
        };

        // The chunk_index is incremented after every loop iteration. After we exit the loop, it
        // is one larger than the last chunk, so it is equal to the number of chunks.

        let attachment = ChunkedAttachment {
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
            data,
            size: Some(size),
            rate_limited: Some(item.rate_limited()),
        };

        if send_individual_attachments {
            let message = KafkaMessage::Attachment(AttachmentKafkaMessage {
                event_id,
                project_id,
                attachment,
            });
            self.produce(KafkaTopic::Attachments, message)?;
            Ok(None)
        } else {
            Ok(Some(attachment))
        }
    }

    fn produce_user_report(
        &self,
        event_id: EventId,
        project_id: ProjectId,
        received_at: DateTime<Utc>,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = KafkaMessage::UserReport(UserReportKafkaMessage {
            project_id,
            event_id,
            start_time: safe_timestamp(received_at),
            payload: item.payload(),
        });

        self.produce(KafkaTopic::Attachments, message)
    }

    fn produce_user_report_v2(
        &self,
        event_id: EventId,
        project_id: ProjectId,
        received_at: DateTime<Utc>,
        item: &Item,
        remote_addr: Option<String>,
    ) -> Result<(), StoreError> {
        let message = KafkaMessage::Event(EventKafkaMessage {
            project_id,
            event_id,
            payload: item.payload(),
            start_time: safe_timestamp(received_at),
            remote_addr,
            attachments: vec![],
        });
        self.produce(KafkaTopic::Feedback, message)
    }

    fn send_metric_message(
        &self,
        namespace: MetricNamespace,
        message: MetricKafkaMessage,
    ) -> Result<(), StoreError> {
        let topic = match namespace {
            MetricNamespace::Sessions => KafkaTopic::MetricsSessions,
            MetricNamespace::Unsupported => {
                relay_log::with_scope(
                    |scope| scope.set_extra("metric_message.name", message.name.as_ref().into()),
                    || relay_log::error!("store service dropping unknown metric usecase"),
                );
                return Ok(());
            }
            _ => KafkaTopic::MetricsGeneric,
        };
        let headers = BTreeMap::from([("namespace".to_string(), namespace.to_string())]);

        self.produce(topic, KafkaMessage::Metric { headers, message })?;
        Ok(())
    }

    fn produce_profile(
        &self,
        organization_id: OrganizationId,
        project_id: ProjectId,
        key_id: Option<u64>,
        received_at: DateTime<Utc>,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = ProfileKafkaMessage {
            organization_id,
            project_id,
            key_id,
            received: safe_timestamp(received_at),
            retention_days,
            headers: BTreeMap::from([(
                "sampled".to_string(),
                if item.sampled() { "true" } else { "false" }.to_owned(),
            )]),
            payload: item.payload(),
        };
        self.produce(KafkaTopic::Profiles, KafkaMessage::Profile(message))?;
        Ok(())
    }

    fn produce_replay_event(
        &self,
        replay_id: EventId,
        project_id: ProjectId,
        received_at: DateTime<Utc>,
        retention_days: u16,
        payload: &[u8],
    ) -> Result<(), StoreError> {
        let message = ReplayEventKafkaMessage {
            replay_id,
            project_id,
            retention_days,
            start_time: safe_timestamp(received_at),
            payload,
        };
        self.produce(KafkaTopic::ReplayEvents, KafkaMessage::ReplayEvent(message))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn produce_replay_recording(
        &self,
        event_id: Option<EventId>,
        scoping: Scoping,
        payload: &[u8],
        replay_event: Option<&[u8]>,
        replay_video: Option<&[u8]>,
        received_at: DateTime<Utc>,
        retention: u16,
    ) -> Result<(), StoreError> {
        // Maximum number of bytes accepted by the consumer.
        let max_payload_size = self.config.max_replay_message_size();

        // Size of the consumer message. We can be reasonably sure this won't overflow because
        // of the request size validation provided by Nginx and Relay.
        let mut payload_size = 2000; // Reserve 2KB for the message metadata.
        payload_size += replay_event.as_ref().map_or(0, |b| b.len());
        payload_size += replay_video.as_ref().map_or(0, |b| b.len());
        payload_size += payload.len();

        // If the recording payload can not fit in to the message do not produce and quit early.
        if payload_size >= max_payload_size {
            relay_log::debug!("replay_recording over maximum size.");
            self.outcome_aggregator.send(TrackOutcome {
                category: DataCategory::Replay,
                event_id,
                outcome: Outcome::Invalid(DiscardReason::TooLarge(
                    DiscardItemType::ReplayRecording,
                )),
                quantity: 1,
                remote_addr: None,
                scoping,
                timestamp: received_at,
            });
            return Ok(());
        }

        let message =
            KafkaMessage::ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage {
                replay_id: event_id.ok_or(StoreError::NoEventId)?,
                project_id: scoping.project_id,
                key_id: scoping.key_id,
                org_id: scoping.organization_id,
                received: safe_timestamp(received_at),
                retention_days: retention,
                payload,
                replay_event,
                replay_video,
            });

        self.produce(KafkaTopic::ReplayRecordings, message)?;

        Ok(())
    }

    fn produce_replay_video(
        &self,
        event_id: Option<EventId>,
        scoping: Scoping,
        payload: Bytes,
        received_at: DateTime<Utc>,
        retention: u16,
    ) -> Result<(), StoreError> {
        #[derive(Deserialize)]
        struct VideoEvent<'a> {
            replay_event: &'a [u8],
            replay_recording: &'a [u8],
            replay_video: &'a [u8],
        }

        let Ok(VideoEvent {
            replay_video,
            replay_event,
            replay_recording,
        }) = rmp_serde::from_slice::<VideoEvent>(&payload)
        else {
            self.outcome_aggregator.send(TrackOutcome {
                category: DataCategory::Replay,
                event_id,
                outcome: Outcome::Invalid(DiscardReason::InvalidReplayEvent),
                quantity: 1,
                remote_addr: None,
                scoping,
                timestamp: received_at,
            });
            return Ok(());
        };

        self.produce_replay_event(
            event_id.ok_or(StoreError::NoEventId)?,
            scoping.project_id,
            received_at,
            retention,
            replay_event,
        )?;

        self.produce_replay_recording(
            event_id,
            scoping,
            replay_recording,
            Some(replay_event),
            Some(replay_video),
            received_at,
            retention,
        )
    }

    fn produce_check_in(
        &self,
        project_id: ProjectId,
        received_at: DateTime<Utc>,
        client: Option<&str>,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = KafkaMessage::CheckIn(CheckInKafkaMessage {
            message_type: CheckInMessageType::CheckIn,
            project_id,
            retention_days,
            start_time: safe_timestamp(received_at),
            sdk: client.map(str::to_owned),
            payload: item.payload(),
            routing_key_hint: item.routing_hint(),
        });

        self.produce(KafkaTopic::Monitors, message)?;

        Ok(())
    }

    fn produce_span(
        &self,
        scoping: Scoping,
        received_at: DateTime<Utc>,
        event_id: Option<EventId>,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        relay_log::trace!("Producing span");
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
                    timestamp: received_at,
                });
                return Ok(());
            }
        };

        span.backfill_data();
        span.duration_ms =
            ((span.end_timestamp_precise - span.start_timestamp_precise) * 1e3) as u32;
        span.event_id = event_id;
        span.organization_id = scoping.organization_id.value();
        span.project_id = scoping.project_id.value();
        span.retention_days = retention_days;
        span.start_timestamp_ms = (span.start_timestamp_precise * 1e3) as u64;

        if let Some(measurements) = &mut span.measurements {
            measurements
                .retain(|_, v| v.as_ref().and_then(|v| v.value).is_some_and(f64::is_finite));
        }

        self.produce(
            KafkaTopic::Spans,
            KafkaMessage::Span {
                headers: BTreeMap::from([(
                    "project_id".to_string(),
                    scoping.project_id.to_string(),
                )]),
                message: span,
            },
        )?;

        self.outcome_aggregator.send(TrackOutcome {
            category: DataCategory::SpanIndexed,
            event_id: None,
            outcome: Outcome::Accepted,
            quantity: 1,
            remote_addr: None,
            scoping,
            timestamp: received_at,
        });
        Ok(())
    }

    fn produce_log(
        &self,
        scoping: Scoping,
        received_at: DateTime<Utc>,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        relay_log::trace!("Producing log");

        let payload = item.payload();
        let d = &mut Deserializer::from_slice(&payload);
        let logs: LogKafkaMessages = match serde_path_to_error::deserialize(d) {
            Ok(logs) => logs,
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn std::error::Error,
                    "failed to parse log"
                );
                self.outcome_aggregator.send(TrackOutcome {
                    category: DataCategory::LogItem,
                    event_id: None,
                    outcome: Outcome::Invalid(DiscardReason::InvalidLog),
                    quantity: 1,
                    remote_addr: None,
                    scoping,
                    timestamp: received_at,
                });
                self.outcome_aggregator.send(TrackOutcome {
                    category: DataCategory::LogByte,
                    event_id: None,
                    outcome: Outcome::Invalid(DiscardReason::InvalidLog),
                    quantity: payload.len() as u32,
                    remote_addr: None,
                    scoping,
                    timestamp: received_at,
                });
                return Ok(());
            }
        };

        let num_logs = logs.items.len() as u32;
        for log in logs.items {
            let timestamp_seconds = log.timestamp as i64;
            let timestamp_nanos = (log.timestamp.fract() * 1e9) as u32;
            let item_id = u128::from_be_bytes(
                *Uuid::new_v7(uuid::Timestamp::from_unix(
                    uuid::NoContext,
                    timestamp_seconds as u64,
                    timestamp_nanos,
                ))
                .as_bytes(),
            )
            .to_le_bytes()
            .to_vec();
            let mut trace_item = TraceItem {
                item_type: TraceItemType::Log.into(),
                organization_id: scoping.organization_id.value(),
                project_id: scoping.project_id.value(),
                received: Some(Timestamp {
                    seconds: safe_timestamp(received_at) as i64,
                    nanos: 0,
                }),
                retention_days: retention_days.into(),
                timestamp: Some(Timestamp {
                    seconds: timestamp_seconds,
                    nanos: 0,
                }),
                trace_id: log.trace_id.to_string(),
                item_id,
                attributes: Default::default(),
                client_sample_rate: 1.0,
                server_sample_rate: 1.0,
            };

            for (name, attribute) in log.attributes.unwrap_or_default() {
                if let Some(attribute_value) = attribute {
                    if let Some(v) = attribute_value.value {
                        let any_value = match v {
                            LogAttributeValue::String(value) => AnyValue {
                                value: Some(Value::StringValue(value)),
                            },
                            LogAttributeValue::Int(value) => AnyValue {
                                value: Some(Value::IntValue(value)),
                            },
                            LogAttributeValue::Bool(value) => AnyValue {
                                value: Some(Value::BoolValue(value)),
                            },
                            LogAttributeValue::Double(value) => AnyValue {
                                value: Some(Value::DoubleValue(value)),
                            },
                            LogAttributeValue::Unknown(_) => continue,
                        };

                        trace_item.attributes.insert(name.into(), any_value);
                    }
                }
            }

            let message = KafkaMessage::Log {
                headers: BTreeMap::from([
                    ("project_id".to_owned(), scoping.project_id.to_string()),
                    (
                        "item_type".to_owned(),
                        (TraceItemType::Log as i32).to_string(),
                    ),
                ]),
                message: trace_item,
            };

            self.produce(KafkaTopic::Items, message)?;
        }

        // We need to track the count and bytes separately for possible rate limits and quotas on both counts and bytes.
        self.outcome_aggregator.send(TrackOutcome {
            category: DataCategory::LogItem,
            event_id: None,
            outcome: Outcome::Accepted,
            quantity: num_logs,
            remote_addr: None,
            scoping,
            timestamp: received_at,
        });
        self.outcome_aggregator.send(TrackOutcome {
            category: DataCategory::LogByte,
            event_id: None,
            outcome: Outcome::Accepted,
            quantity: payload.len() as u32,
            remote_addr: None,
            scoping,
            timestamp: received_at,
        });

        Ok(())
    }

    fn produce_profile_chunk(
        &self,
        organization_id: OrganizationId,
        project_id: ProjectId,
        received_at: DateTime<Utc>,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = ProfileChunkKafkaMessage {
            organization_id,
            project_id,
            received: safe_timestamp(received_at),
            retention_days,
            payload: item.payload(),
        };
        self.produce(KafkaTopic::Profiles, KafkaMessage::ProfileChunk(message))?;
        Ok(())
    }
}

impl Service for StoreService {
    type Interface = Store;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let this = Arc::new(self);

        relay_log::info!("store forwarder started");

        while let Some(message) = rx.recv().await {
            let service = Arc::clone(&this);
            // For now, we run each task synchronously, in the future we might explore how to make
            // the store async.
            this.pool
                .spawn_async(async move { service.handle_message(message) }.boxed())
                .await;
        }

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
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,

    /// The Sentry-internal attachment type used in the processing pipeline.
    #[serde(serialize_with = "serialize_attachment_type")]
    attachment_type: AttachmentType,

    /// Number of outlined chunks.
    /// Zero if the attachment has `size: 0`, or there was only a single chunk which has been inlined into `data`.
    chunks: usize,

    /// The content of the attachment,
    /// if they are smaller than the configured `attachment_chunk_size`.
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Bytes>,

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

fn serialize_btreemap_skip_nulls<S, T>(
    map: &Option<BTreeMap<&str, Option<T>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: serde::Serialize,
{
    let Some(map) = map else {
        return serializer.serialize_none();
    };
    let mut m = serializer.serialize_map(Some(map.len()))?;
    for (key, value) in map.iter() {
        if let Some(value) = value {
            m.serialize_entry(key, value)?;
        }
    }
    m.end()
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
struct ReplayEventKafkaMessage<'a> {
    /// Raw event payload.
    payload: &'a [u8],
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

#[derive(Debug, Serialize)]
struct ReplayRecordingNotChunkedKafkaMessage<'a> {
    replay_id: EventId,
    key_id: Option<u64>,
    org_id: OrganizationId,
    project_id: ProjectId,
    received: u64,
    retention_days: u16,
    #[serde(with = "serde_bytes")]
    payload: &'a [u8],
    #[serde(with = "serde_bytes")]
    replay_event: Option<&'a [u8]>,
    #[serde(with = "serde_bytes")]
    replay_video: Option<&'a [u8]>,
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
struct MetricKafkaMessage<'a> {
    org_id: OrganizationId,
    project_id: ProjectId,
    name: &'a MetricName,
    #[serde(flatten)]
    value: MetricValue<'a>,
    timestamp: UnixTimestamp,
    tags: &'a BTreeMap<String, String>,
    retention_days: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    received_at: Option<UnixTimestamp>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "value")]
enum MetricValue<'a> {
    #[serde(rename = "c")]
    Counter(FiniteF64),
    #[serde(rename = "d")]
    Distribution(ArrayEncoding<'a, &'a [FiniteF64]>),
    #[serde(rename = "s")]
    Set(ArrayEncoding<'a, SetView<'a>>),
    #[serde(rename = "g")]
    Gauge(GaugeValue),
}

impl MetricValue<'_> {
    fn variant(&self) -> &'static str {
        match self {
            Self::Counter(_) => "counter",
            Self::Distribution(_) => "distribution",
            Self::Set(_) => "set",
            Self::Gauge(_) => "gauge",
        }
    }

    fn encoding(&self) -> Option<&'static str> {
        match self {
            Self::Distribution(ae) => Some(ae.name()),
            Self::Set(ae) => Some(ae.name()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ProfileKafkaMessage {
    organization_id: OrganizationId,
    project_id: ProjectId,
    key_id: Option<u64>,
    received: u64,
    retention_days: u16,
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
struct SpanLink<'a> {
    pub trace_id: &'a str,
    pub span_id: &'a str,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampled: Option<bool>,
    #[serde(borrow)]
    pub attributes: Option<&'a RawValue>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SpanMeasurement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SpanKafkaMessage<'a> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<&'a RawValue>,
    #[serde(default)]
    duration_ms: u32,
    /// The ID of the transaction event associated to this span, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_id: Option<EventId>,
    #[serde(rename(deserialize = "exclusive_time"))]
    exclusive_time_ms: f64,
    #[serde(default)]
    is_segment: bool,
    #[serde(default)]
    is_remote: bool,

    #[serde(default, skip_serializing_if = "none_or_empty_map", borrow)]
    data: Option<BTreeMap<String, Option<&'a RawValue>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kind: Option<&'a str>,
    #[serde(default, skip_serializing_if = "none_or_empty_vec")]
    links: Option<Vec<SpanLink<'a>>>,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    measurements: Option<BTreeMap<Cow<'a, str>, Option<SpanMeasurement>>>,
    #[serde(default)]
    organization_id: u64,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    origin: Option<Cow<'a, str>>,
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_btreemap_skip_nulls"
    )]
    #[serde(borrow)]
    sentry_tags: Option<BTreeMap<&'a str, Option<&'a RawValue>>>,
    span_id: &'a str,
    #[serde(default, skip_serializing_if = "none_or_empty_object")]
    tags: Option<&'a RawValue>,
    trace_id: EventId,

    #[serde(default)]
    start_timestamp_ms: u64,
    #[serde(rename(deserialize = "start_timestamp"))]
    start_timestamp_precise: f64,
    #[serde(rename(deserialize = "timestamp"))]
    end_timestamp_precise: f64,

    #[serde(borrow, default, skip_serializing)]
    platform: Cow<'a, str>, // We only use this for logging for now

    #[serde(
        default,
        rename = "_meta",
        skip_serializing_if = "none_or_empty_object"
    )]
    meta: Option<&'a RawValue>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    _performance_issues_spans: Option<bool>,
}

impl SpanKafkaMessage<'_> {
    /// Backfills `data` based on `sentry_tags`.
    ///
    /// Every item in `sentry_tags` is copied to `data`, with the key prefixed with `sentry.`.
    /// The only exception is the `description` tag, which is copied as `sentry.normalized_description`.
    fn backfill_data(&mut self) {
        let Some(sentry_tags) = self.sentry_tags.as_ref() else {
            return;
        };

        let data = self.data.get_or_insert_default();

        for (key, value) in sentry_tags {
            let Some(value) = value else {
                continue;
            };

            let key = if *key == "description" {
                "sentry.normalized_description".to_owned()
            } else {
                format!("sentry.{key}")
            };

            // TODO: Should a a tag supersede an existing value?
            data.insert(key, Some(value));
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", content = "value")]
enum LogAttributeValue {
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "boolean")]
    Bool(bool),
    #[serde(rename = "integer")]
    Int(i64),
    #[serde(rename = "double")]
    Double(f64),
    #[serde(rename = "unknown")]
    Unknown(()),
}

/// This is a temporary struct to convert the old attribute format to the new one.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LogAttribute {
    #[serde(flatten)]
    value: Option<LogAttributeValue>,
}

#[derive(Debug, Deserialize)]
struct LogKafkaMessages<'a> {
    #[serde(borrow)]
    items: Vec<LogKafkaMessage<'a>>,
}

#[derive(Debug, Deserialize)]
struct LogKafkaMessage<'a> {
    trace_id: EventId,
    #[serde(default)]
    timestamp: f64,
    #[serde(borrow, default)]
    attributes: Option<BTreeMap<&'a str, Option<LogAttribute>>>,
}

fn none_or_empty_object(value: &Option<&RawValue>) -> bool {
    match value {
        None => true,
        Some(raw) => raw.get() == "{}",
    }
}

fn none_or_empty_vec<T>(value: &Option<Vec<T>>) -> bool {
    match &value {
        Some(vec) => vec.is_empty(),
        None => true,
    }
}

fn none_or_empty_map<S, T>(value: &Option<BTreeMap<S, T>>) -> bool {
    value.as_ref().is_none_or(BTreeMap::is_empty)
}

#[derive(Clone, Debug, Serialize)]
struct ProfileChunkKafkaMessage {
    organization_id: OrganizationId,
    project_id: ProjectId,
    received: u64,
    retention_days: u16,
    payload: Bytes,
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
    Metric {
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: MetricKafkaMessage<'a>,
    },
    Profile(ProfileKafkaMessage),
    ReplayEvent(ReplayEventKafkaMessage<'a>),
    ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage<'a>),
    CheckIn(CheckInKafkaMessage),
    Span {
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: SpanKafkaMessage<'a>,
    },
    Log {
        headers: BTreeMap<String, String>,
        #[serde(skip)]
        message: TraceItem,
    },
    ProfileChunk(ProfileChunkKafkaMessage),
}

impl Message for KafkaMessage<'_> {
    fn variant(&self) -> &'static str {
        match self {
            KafkaMessage::Event(_) => "event",
            KafkaMessage::Attachment(_) => "attachment",
            KafkaMessage::AttachmentChunk(_) => "attachment_chunk",
            KafkaMessage::UserReport(_) => "user_report",
            KafkaMessage::Metric { message, .. } => match message.name.namespace() {
                MetricNamespace::Sessions => "metric_sessions",
                MetricNamespace::Transactions => "metric_transactions",
                MetricNamespace::Spans => "metric_spans",
                MetricNamespace::Custom => "metric_custom",
                MetricNamespace::Stats => "metric_metric_stats",
                MetricNamespace::Unsupported => "metric_unsupported",
            },
            KafkaMessage::Profile(_) => "profile",
            KafkaMessage::ReplayEvent(_) => "replay_event",
            KafkaMessage::ReplayRecordingNotChunked(_) => "replay_recording_not_chunked",
            KafkaMessage::CheckIn(_) => "check_in",
            KafkaMessage::Log { .. } => "log",
            KafkaMessage::Span { .. } => "span",
            KafkaMessage::ProfileChunk(_) => "profile_chunk",
        }
    }

    /// Returns the partitioning key for this kafka message determining.
    fn key(&self) -> Option<[u8; 16]> {
        match self {
            Self::Event(message) => Some(message.event_id.0),
            Self::Attachment(message) => Some(message.event_id.0),
            Self::AttachmentChunk(message) => Some(message.event_id.0),
            Self::UserReport(message) => Some(message.event_id.0),
            Self::ReplayEvent(message) => Some(message.replay_id.0),
            Self::Span { message, .. } => Some(message.trace_id.0),

            // Monitor check-ins use the hinted UUID passed through from the Envelope.
            //
            // XXX(epurkhiser): In the future it would be better if all KafkaMessage's would
            // recieve the routing_key_hint form their envelopes.
            Self::CheckIn(message) => message.routing_key_hint,

            // Generate a new random routing key, instead of defaulting to `rdkafka` behaviour
            // for no routing key.
            //
            // This results in significantly more work for Kafka, but we've seen that the metrics
            // indexer consumer in Sentry, cannot deal with this load shape.
            // Until the metric indexer is updated, we still need to assign random keys here.
            Self::Metric { .. } => Some(Uuid::new_v4()),

            // Random partitioning
            Self::Profile(_)
            | Self::Log { .. }
            | Self::ReplayRecordingNotChunked(_)
            | Self::ProfileChunk(_) => None,
        }
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.into_bytes())
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
            KafkaMessage::Log { headers, .. } => {
                if !headers.is_empty() {
                    return Some(headers);
                }
                None
            }
            KafkaMessage::Span { headers, .. } => {
                if !headers.is_empty() {
                    return Some(headers);
                }
                None
            }
            _ => None,
        }
    }

    /// Serializes the message into its binary format.
    fn serialize(&self) -> Result<Cow<'_, [u8]>, ClientError> {
        match self {
            KafkaMessage::Metric { message, .. } => serde_json::to_vec(message)
                .map(Cow::Owned)
                .map_err(ClientError::InvalidJson),
            KafkaMessage::ReplayEvent(message) => serde_json::to_vec(message)
                .map(Cow::Owned)
                .map_err(ClientError::InvalidJson),
            KafkaMessage::Span { message, .. } => serde_json::to_vec(message)
                .map(Cow::Owned)
                .map_err(ClientError::InvalidJson),
            KafkaMessage::Log { message, .. } => {
                let mut payload = Vec::new();

                if message.encode(&mut payload).is_err() {
                    return Err(ClientError::ProtobufEncodingFailed);
                }

                Ok(Cow::Owned(payload))
            }
            _ => rmp_serde::to_vec_named(&self)
                .map(Cow::Owned)
                .map_err(ClientError::InvalidMsgPack),
        }
    }
}

/// Determines if the given item is considered slow.
///
/// Slow items must be routed to the `Attachments` topic.
fn is_slow_item(item: &Item) -> bool {
    item.ty() == &ItemType::Attachment || item.ty() == &ItemType::UserReport
}

fn bool_to_str(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Returns a safe timestamp for Kafka.
///
/// Kafka expects timestamps to be in UTC and in seconds since epoch.
fn safe_timestamp(timestamp: DateTime<Utc>) -> u64 {
    let ts = timestamp.timestamp();
    if ts >= 0 {
        return ts as u64;
    }

    // We assume this call can't return < 0.
    Utc::now().timestamp() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disallow_outcomes() {
        let config = Config::default();
        let producer = Producer::create(&config).unwrap();

        for topic in [KafkaTopic::Outcomes, KafkaTopic::OutcomesBilling] {
            let res = producer
                .client
                .send(topic, Some(*b"0123456789abcdef"), None, "foo", b"");

            assert!(matches!(res, Err(ClientError::InvalidTopicName)));
        }
    }
}
