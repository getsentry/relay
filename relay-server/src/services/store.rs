//! This module contains the service that forwards events and attachments to the Sentry store.
//! The service uses Kafka topics to forward data to Sentry

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use relay_base_schema::data_category::DataCategory;
use relay_base_schema::project::ProjectId;
use relay_common::time::{instant_to_date_time, UnixTimestamp};
use relay_config::Config;
use relay_event_schema::protocol::{EventId, SessionStatus, VALID_PLATFORMS};

use relay_kafka::{ClientError, KafkaClient, KafkaTopic, Message};
use relay_metrics::{
    Bucket, BucketView, BucketViewValue, BucketsView, FiniteF64, GaugeValue, MetricName,
    MetricNamespace, SetView,
};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Deserializer;
use uuid::Uuid;

use crate::envelope::{AttachmentType, Envelope, Item, ItemType, SourceQuantities};
use crate::metric_stats::MetricStats;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::Processed;
use crate::statsd::RelayCounters;
use crate::utils::{
    self, is_rolled_out, ArrayEncoding, BucketEncoder, ExtractionMode, FormDataIter, TypedEnvelope,
};

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
    pub envelope: TypedEnvelope<Processed>,
}

/// Publishes a list of [`Bucket`]s to the Sentry core application through Kafka topics.
#[derive(Clone, Debug)]
pub struct StoreMetrics {
    pub buckets: Vec<Bucket>,
    pub scoping: Scoping,
    pub retention: u16,
    pub mode: ExtractionMode,
}

#[derive(Debug)]
pub struct StoreCogs(pub Vec<u8>);

/// Service interface for the [`StoreEnvelope`] message.
#[derive(Debug)]
pub enum Store {
    Envelope(StoreEnvelope),
    Metrics(StoreMetrics),
    Cogs(StoreCogs),
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

impl FromMessage<StoreCogs> for Store {
    type Response = NoResponse;

    fn from_message(message: StoreCogs, _: ()) -> Self {
        Self::Cogs(message)
    }
}

/// Service implementing the [`Store`] interface.
pub struct StoreService {
    config: Arc<Config>,
    global_config: GlobalConfigHandle,
    outcome_aggregator: Addr<TrackOutcome>,
    metric_stats: MetricStats,
    producer: Producer,
}

impl StoreService {
    pub fn create(
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        outcome_aggregator: Addr<TrackOutcome>,
        metric_stats: MetricStats,
    ) -> anyhow::Result<Self> {
        let producer = Producer::create(&config)?;
        Ok(Self {
            config,
            global_config,
            outcome_aggregator,
            metric_stats,
            producer,
        })
    }

    fn handle_message(&self, message: Store) {
        match message {
            Store::Envelope(message) => self.handle_store_envelope(message),
            Store::Metrics(message) => self.handle_store_metrics(message),
            Store::Cogs(message) => self.handle_store_cogs(message),
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
        mut envelope: Box<Envelope>,
        start_time: Instant,
        scoping: Scoping,
    ) -> Result<(), StoreError> {
        let retention = envelope.retention();
        let event_id = envelope.event_id();

        let use_feedback_ingest_v2 = self.global_config.current().options.feedback_ingest_v2;

        let event_item = envelope.as_mut().take_item_by(|item| {
            matches!(
                (item.ty(), use_feedback_ingest_v2),
                (ItemType::Event, _)
                    | (ItemType::Transaction, _)
                    | (ItemType::Security, _)
                    | (ItemType::UserReportV2, false)
            )
        });
        let client = envelope.meta().client();

        let topic = if envelope.get_item_by(is_slow_item).is_some() {
            KafkaTopic::Attachments
        } else if event_item.as_ref().map(|x| x.ty()) == Some(&ItemType::Transaction) {
            KafkaTopic::Transactions
        } else if event_item.as_ref().map(|x| x.ty()) == Some(&ItemType::UserReportV2) {
            let feedback_ingest_topic_rollout_rate = self
                .global_config
                .current()
                .options
                .feedback_ingest_topic_rollout_rate;
            if is_rolled_out(scoping.organization_id, feedback_ingest_topic_rollout_rate) {
                KafkaTopic::Feedback
            } else {
                KafkaTopic::Events
            }
        } else {
            KafkaTopic::Events
        };

        let mut attachments = Vec::new();

        let mut replay_event = None;
        let mut replay_recording = None;

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
                    // Crash report modal and older SDKs and feedback endpoints
                    debug_assert!(topic == KafkaTopic::Attachments);
                    self.produce_user_report(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.project_id,
                        start_time,
                        item,
                    )?;
                }
                ItemType::UserReportV2 => {
                    // The preferred, newest form of feedback. Main source is user feedback widget
                    if use_feedback_ingest_v2 {
                        let remote_addr =
                            envelope.meta().client_addr().map(|addr| addr.to_string());
                        self.produce_user_report_v2(
                            event_id.ok_or(StoreError::NoEventId)?,
                            scoping.project_id,
                            scoping.organization_id,
                            start_time,
                            item,
                            remote_addr,
                        )?;
                    }
                }
                ItemType::Profile => self.produce_profile(
                    scoping.organization_id,
                    scoping.project_id,
                    scoping.key_id,
                    start_time,
                    retention,
                    item,
                )?,
                ItemType::ReplayVideo => {
                    self.produce_replay_video(
                        event_id,
                        scoping,
                        item.payload(),
                        start_time,
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
                        start_time,
                        retention,
                        &item.payload(),
                    )?;
                }
                ItemType::CheckIn => {
                    self.produce_check_in(scoping.project_id, start_time, client, retention, item)?
                }
                ItemType::Span => {
                    self.produce_span(scoping, start_time, event_id, retention, item)?
                }
                ItemType::ProfileChunk => self.produce_profile_chunk(
                    scoping.organization_id,
                    scoping.project_id,
                    start_time,
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
                start_time,
                retention,
            )?;
        }

        if event_item.is_none() && attachments.is_empty() {
            // No event-related content. All done.
            return Ok(());
        }

        let remote_addr = envelope.meta().client_addr().map(|addr| addr.to_string());

        let kafka_messages = Self::extract_kafka_messages_for_event(
            event_item.as_ref(),
            event_id.ok_or(StoreError::NoEventId)?,
            scoping,
            start_time,
            remote_addr,
            attachments,
        );

        for message in kafka_messages {
            self.produce(topic, message)?;
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
        let mut error = None;

        let mut dropped_quantities = SourceQuantities::default();

        let global_config = self.global_config.current();
        let mut encoder = BucketEncoder::new(&global_config);

        for mut bucket in buckets {
            let namespace = encoder.prepare(&mut bucket);

            let mut has_success = false;
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

                match result {
                    Ok(()) => {
                        has_success = true;
                    }
                    Err(e) => {
                        error.get_or_insert(e);
                        dropped_quantities += utils::extract_metric_quantities([view], mode);
                    }
                }
            }

            // Tracking the volume here is slightly off, only one of the multiple bucket views can
            // fail to produce. Since the views are sliced from the original bucket we cannot
            // correctly attribute the amount of merges (volume) to the amount of slices that
            // succeeded or not. -> Attribute the entire volume if at least one slice successfully
            // produced.
            //
            // This logic will be improved iterated on and change once we move serialization logic
            // back into the processor service.
            self.metric_stats.track_metric(
                scoping,
                &bucket,
                if has_success {
                    Outcome::Accepted
                } else {
                    Outcome::Invalid(DiscardReason::Internal)
                },
            );
        }

        if let Some(error) = error {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                "failed to produce metric buckets: {error}"
            );

            utils::reject_metrics::<&Bucket>(
                &self.outcome_aggregator,
                &self.metric_stats,
                dropped_quantities,
                scoping,
                Outcome::Invalid(DiscardReason::Internal),
                None,
            );
        }
    }

    fn handle_store_cogs(&self, StoreCogs(payload): StoreCogs) {
        let message = KafkaMessage::Cogs(CogsKafkaMessage(payload));
        if let Err(error) = self.produce(KafkaTopic::Cogs, message) {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                "failed to store cogs measurement"
            );
        }
    }

    fn create_metric_message<'a>(
        &self,
        organization_id: u64,
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
        })
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
                payload: payload.slice(offset..offset + chunk_size),
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
            attachment_type: item.attachment_type().cloned().unwrap_or_default(),
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

    fn produce_user_report_v2(
        &self,
        event_id: EventId,
        project_id: ProjectId,
        organization_id: u64,
        start_time: Instant,
        item: &Item,
        remote_addr: Option<String>,
    ) -> Result<(), StoreError> {
        // check rollout rate option (effectively a FF) to determine whether to produce to new infra
        let global_config = self.global_config.current();
        let feedback_ingest_topic_rollout_rate =
            global_config.options.feedback_ingest_topic_rollout_rate;
        let topic = if is_rolled_out(organization_id, feedback_ingest_topic_rollout_rate) {
            KafkaTopic::Feedback
        } else {
            KafkaTopic::Events
        };

        let message = KafkaMessage::Event(EventKafkaMessage {
            project_id,
            event_id,
            payload: item.payload(),
            start_time: UnixTimestamp::from_instant(start_time).as_secs(),
            remote_addr,
            attachments: vec![],
        });
        self.produce(topic, message)
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
            MetricNamespace::Profiles => {
                if !self
                    .global_config
                    .current()
                    .options
                    .profiles_function_generic_metrics_enabled
                {
                    return Ok(());
                }
                KafkaTopic::MetricsGeneric
            }
            _ => KafkaTopic::MetricsGeneric,
        };
        let headers = BTreeMap::from([("namespace".to_string(), namespace.to_string())]);

        self.produce(topic, KafkaMessage::Metric { headers, message })?;
        Ok(())
    }

    fn produce_profile(
        &self,
        organization_id: u64,
        project_id: ProjectId,
        key_id: Option<u64>,
        start_time: Instant,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = ProfileKafkaMessage {
            organization_id,
            project_id,
            key_id,
            received: UnixTimestamp::from_instant(start_time).as_secs(),
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
        start_time: Instant,
        retention_days: u16,
        payload: &[u8],
    ) -> Result<(), StoreError> {
        let message = ReplayEventKafkaMessage {
            replay_id,
            project_id,
            retention_days,
            start_time: UnixTimestamp::from_instant(start_time).as_secs(),
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
        start_time: Instant,
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
            relay_log::warn!("replay_recording over maximum size.");
            self.outcome_aggregator.send(TrackOutcome {
                category: DataCategory::Replay,
                event_id,
                outcome: Outcome::Invalid(DiscardReason::TooLarge),
                quantity: 1,
                remote_addr: None,
                scoping,
                timestamp: instant_to_date_time(start_time),
            });
            return Ok(());
        }

        let message =
            KafkaMessage::ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage {
                replay_id: event_id.ok_or(StoreError::NoEventId)?,
                project_id: scoping.project_id,
                key_id: scoping.key_id,
                org_id: scoping.organization_id,
                received: UnixTimestamp::from_instant(start_time).as_secs(),
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
        start_time: Instant,
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
                timestamp: instant_to_date_time(start_time),
            });
            return Ok(());
        };

        self.produce_replay_event(
            event_id.ok_or(StoreError::NoEventId)?,
            scoping.project_id,
            start_time,
            retention,
            replay_event,
        )?;

        self.produce_replay_recording(
            event_id,
            scoping,
            replay_recording,
            Some(replay_event),
            Some(replay_video),
            start_time,
            retention,
        )
    }

    fn produce_check_in(
        &self,
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

        self.produce(KafkaTopic::Monitors, message)?;

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
                    timestamp: instant_to_date_time(start_time),
                });
                return Ok(());
            }
        };

        span.duration_ms = ((span.end_timestamp - span.start_timestamp) * 1e3) as u32;
        span.event_id = event_id;
        span.organization_id = scoping.organization_id;
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

        self.produce_metrics_summary(item, &span);

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
            timestamp: instant_to_date_time(start_time),
        });

        Ok(())
    }

    fn produce_metrics_summary(&self, item: &Item, span: &SpanKafkaMessage) {
        let payload = item.payload();
        let d = &mut Deserializer::from_slice(&payload);
        let mut metrics_summary: SpanWithMetricsSummary = match serde_path_to_error::deserialize(d)
        {
            Ok(span) => span,
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn std::error::Error,
                    "failed to parse metrics summary of span"
                );
                return;
            }
        };
        let Some(metrics_summary) = &mut metrics_summary.metrics_summary else {
            return;
        };
        let &SpanKafkaMessage {
            duration_ms,
            end_timestamp,
            is_segment,
            project_id,
            received,
            retention_days,
            segment_id,
            span_id,
            trace_id,
            ..
        } = span;
        let group = span
            .sentry_tags
            .as_ref()
            .and_then(|sentry_tags| sentry_tags.get("group"))
            .map_or("", String::as_str);

        for (mri, summaries) in metrics_summary {
            let Some(summaries) = summaries else {
                continue;
            };
            for summary in summaries {
                let Some(SpanMetricsSummary {
                    count,
                    max,
                    min,
                    sum,
                    tags,
                }) = summary
                else {
                    continue;
                };

                let &mut Some(count) = count else {
                    continue;
                };

                if count == 0 {
                    continue;
                }

                // If none of the values are there, the summary is invalid.
                if max.is_none() && min.is_none() && sum.is_none() {
                    continue;
                }

                let tags = tags
                    .iter_mut()
                    .filter_map(|(k, v)| Some((k.as_str(), v.as_deref()?)))
                    .collect();

                // Ignore immediate errors on produce.
                if let Err(error) = self.produce(
                    KafkaTopic::MetricsSummaries,
                    KafkaMessage::MetricsSummary(MetricsSummaryKafkaMessage {
                        count,
                        duration_ms,
                        end_timestamp,
                        group,
                        is_segment,
                        max,
                        min,
                        mri,
                        project_id,
                        received,
                        retention_days,
                        segment_id: segment_id.unwrap_or_default(),
                        span_id,
                        sum,
                        tags,
                        trace_id,
                    }),
                ) {
                    relay_log::error!(
                        error = &error as &dyn std::error::Error,
                        "failed to push metrics summary to kafka",
                    );
                }
            }
        }
    }

    fn produce_profile_chunk(
        &self,
        organization_id: u64,
        project_id: ProjectId,
        start_time: Instant,
        retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        let message = ProfileChunkKafkaMessage {
            organization_id,
            project_id,
            received: UnixTimestamp::from_instant(start_time).as_secs(),
            retention_days,
            payload: item.payload(),
        };
        self.produce(KafkaTopic::Profiles, KafkaMessage::ProfileChunk(message))?;
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    org_id: u64,
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
    name: &'a MetricName,
    #[serde(flatten)]
    value: MetricValue<'a>,
    timestamp: UnixTimestamp,
    tags: &'a BTreeMap<String, String>,
    retention_days: u16,
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

impl<'a> MetricValue<'a> {
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
    organization_id: u64,
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
struct SpanMeasurement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<f64>,
}

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
    #[serde(default)]
    is_segment: bool,

    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    measurements: Option<BTreeMap<Cow<'a, str>, Option<SpanMeasurement>>>,
    #[serde(default)]
    organization_id: u64,
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
    #[serde(default, skip_serializing_if = "none_or_empty_object")]
    tags: Option<&'a RawValue>,
    trace_id: EventId,

    #[serde(borrow, default, skip_serializing)]
    platform: Cow<'a, str>, // We only use this for logging for now
}

fn none_or_empty_object(value: &Option<&RawValue>) -> bool {
    match value {
        None => true,
        Some(raw) => raw.get() == "{}",
    }
}

#[derive(Debug, Deserialize)]
struct SpanMetricsSummary {
    #[serde(default)]
    count: Option<u64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default)]
    tags: BTreeMap<String, Option<String>>,
}

type SpanMetricsSummaries = Vec<Option<SpanMetricsSummary>>;

#[derive(Debug, Deserialize)]
struct SpanWithMetricsSummary {
    #[serde(default, rename(deserialize = "_metrics_summary"))]
    metrics_summary: Option<BTreeMap<String, Option<SpanMetricsSummaries>>>,
}

#[derive(Debug, Serialize)]
struct MetricsSummaryKafkaMessage<'a> {
    duration_ms: u32,
    end_timestamp: f64,
    group: &'a str,
    is_segment: bool,
    mri: &'a str,
    project_id: u64,
    received: f64,
    retention_days: u16,
    segment_id: &'a str,
    span_id: &'a str,
    trace_id: EventId,

    count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max: &'a Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min: &'a Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sum: &'a Option<f64>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    tags: BTreeMap<&'a str, &'a str>,
}

#[derive(Clone, Debug, Serialize)]
struct ProfileChunkKafkaMessage {
    organization_id: u64,
    project_id: ProjectId,
    received: u64,
    retention_days: u16,
    payload: Bytes,
}

#[derive(Debug, Serialize)]
#[serde(transparent)]
struct CogsKafkaMessage(Vec<u8>);

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
    MetricsSummary(MetricsSummaryKafkaMessage<'a>),
    Cogs(CogsKafkaMessage),
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
                MetricNamespace::Profiles => "metric_profiles",
                MetricNamespace::Custom => "metric_custom",
                MetricNamespace::Stats => "metric_metric_stats",
                MetricNamespace::Unsupported => "metric_unsupported",
            },
            KafkaMessage::Profile(_) => "profile",
            KafkaMessage::ReplayEvent(_) => "replay_event",
            KafkaMessage::ReplayRecordingNotChunked(_) => "replay_recording_not_chunked",
            KafkaMessage::CheckIn(_) => "check_in",
            KafkaMessage::Span { .. } => "span",
            KafkaMessage::MetricsSummary(_) => "metrics_summary",
            KafkaMessage::Cogs(_) => "cogs",
            KafkaMessage::ProfileChunk(_) => "profile_chunk",
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
            Self::Span { message, .. } => message.trace_id.0,

            // Monitor check-ins use the hinted UUID passed through from the Envelope.
            //
            // XXX(epurkhiser): In the future it would be better if all KafkaMessage's would
            // recieve the routing_key_hint form their envelopes.
            Self::CheckIn(message) => message.routing_key_hint.unwrap_or_else(Uuid::nil),

            // Random partitioning
            Self::Profile(_)
            | Self::ReplayRecordingNotChunked(_)
            | Self::MetricsSummary(_)
            | Self::Cogs(_)
            | Self::ProfileChunk(_) => Uuid::nil(),

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
            KafkaMessage::MetricsSummary(message) => serde_json::to_vec(message)
                .map(Cow::Owned)
                .map_err(ClientError::InvalidJson),

            KafkaMessage::Cogs(CogsKafkaMessage(payload)) => Ok(payload.into()),

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
    if value {
        "true"
    } else {
        "false"
    }
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

    #[test]
    fn disallow_outcomes() {
        let config = Config::default();
        let producer = Producer::create(&config).unwrap();

        for topic in [KafkaTopic::Outcomes, KafkaTopic::OutcomesBilling] {
            let res = producer
                .client
                .send(topic, b"0123456789abcdef", None, "foo", b"");

            assert!(matches!(res, Err(ClientError::InvalidTopicName)));
        }
    }
}
