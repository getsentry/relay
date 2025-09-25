//! This module contains the service that forwards events and attachments to the Sentry store.
//! The service uses Kafka topics to forward data to Sentry

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use futures::future::BoxFuture;
use prost::Message as _;
use sentry_protos::snuba::v1::{TraceItem, TraceItemType};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use serde_json::value::RawValue;
use uuid::Uuid;

use relay_base_schema::data_category::DataCategory;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_config::Config;
use relay_event_schema::protocol::{EventId, VALID_PLATFORMS, datetime_to_timestamp};
use relay_kafka::{ClientError, KafkaClient, KafkaTopic, Message, SerializationOutput};
use relay_metrics::{
    Bucket, BucketView, BucketViewValue, BucketsView, ByNamespace, GaugeValue, MetricName,
    MetricNamespace, SetView,
};
use relay_protocol::FiniteF64;
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use relay_threading::AsyncPool;

use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::managed::{Counted, Managed, OutcomeError, Quantities, TypedEnvelope};
use crate::metrics::{ArrayEncoding, BucketEncoder, MetricOutcomes};
use crate::service::ServiceError;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::Processed;
use crate::statsd::{RelayCounters, RelayGauges, RelayTimers};
use crate::utils::{self, FormDataIter};

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

impl OutcomeError for StoreError {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        (Some(Outcome::Invalid(DiscardReason::Internal)), self)
    }
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
            let kafka_configs = config.kafka_configs(*topic)?;
            client_builder = client_builder
                .add_kafka_topic_config(*topic, &kafka_configs, config.kafka_validate_topics())
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

/// Publishes a log item to Sentry core application through Kafka.
#[derive(Debug)]
pub struct StoreTraceItem {
    /// The final trace item which will be produced to Kafka.
    pub trace_item: TraceItem,
    /// Outcomes to be emitted when successfully producing the item to Kafka.
    ///
    /// Note: this is only a temporary measure, long term these outcomes will be part of the trace
    /// item and emitted by Snuba to guarantee a delivery to storage.
    pub quantities: Quantities,
}

impl Counted for StoreTraceItem {
    fn quantities(&self) -> Quantities {
        self.quantities.clone()
    }
}

/// The asynchronous thread pool used for scheduling storing tasks in the envelope store.
pub type StoreServicePool = AsyncPool<BoxFuture<'static, ()>>;

/// Service interface for the [`StoreEnvelope`] message.
#[derive(Debug)]
pub enum Store {
    /// An envelope containing a mixture of items.
    ///
    /// Note: Some envelope items are not supported to be submitted at all or through an envelope,
    /// for example logs must be submitted via [`Self::TraceItem`] instead.
    ///
    /// Long term this variant is going to be replaced with fully typed variants of items which can
    /// be stored instead.
    Envelope(StoreEnvelope),
    /// Aggregated generic metrics.
    Metrics(StoreMetrics),
    /// A singular [`TraceItem`].
    TraceItem(Managed<StoreTraceItem>),
}

impl Store {
    /// Returns the name of the message variant.
    fn variant(&self) -> &'static str {
        match self {
            Store::Envelope(_) => "envelope",
            Store::Metrics(_) => "metrics",
            Store::TraceItem(_) => "log",
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

impl FromMessage<Managed<StoreTraceItem>> for Store {
    type Response = NoResponse;

    fn from_message(message: Managed<StoreTraceItem>, _: ()) -> Self {
        Self::TraceItem(message)
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
                Store::TraceItem(message) => self.handle_store_trace_item(message),
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
        let downsampled_retention = envelope.downsampled_retention();

        let event_id = envelope.event_id();
        let event_item = envelope.as_mut().take_item_by(|item| {
            matches!(
                item.ty(),
                ItemType::Event | ItemType::Transaction | ItemType::Security
            )
        });
        let event_type = event_item.as_ref().map(|item| item.ty());

        // Some error events like minidumps need all attachment chunks to be processed _before_
        // the event payload on the consumer side. Transaction attachments do not require this ordering
        // guarantee, so they do not have to go to the same topic as their event payload.
        let event_topic = if event_item.as_ref().map(|x| x.ty()) == Some(&ItemType::Transaction) {
            KafkaTopic::Transactions
        } else if envelope.get_item_by(is_slow_item).is_some() {
            KafkaTopic::Attachments
        } else {
            KafkaTopic::Events
        };

        let send_individual_attachments = matches!(event_type, None | Some(&ItemType::Transaction));

        let mut attachments = Vec::new();
        let mut replay_event = None;
        let mut replay_recording = None;

        // Whether Relay will submit the replay-event to snuba or not.
        let replay_relay_snuba_publish_disabled = utils::sample(
            self.global_config
                .current()
                .options
                .replay_relay_snuba_publish_disabled_sample_rate,
        )
        .is_keep();

        for item in envelope.items() {
            let content_type = item.content_type();
            match item.ty() {
                ItemType::Attachment => {
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
                    debug_assert!(event_topic == KafkaTopic::Attachments);
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
                        replay_relay_snuba_publish_disabled,
                    )?;
                }
                ItemType::ReplayRecording => {
                    replay_recording = Some(item);
                }
                ItemType::ReplayEvent => {
                    replay_event = Some(item);
                    self.produce_replay_event(
                        event_id.ok_or(StoreError::NoEventId)?,
                        scoping.project_id,
                        received_at,
                        retention,
                        &item.payload(),
                        replay_relay_snuba_publish_disabled,
                    )?;
                }
                ItemType::CheckIn => {
                    let client = envelope.meta().client();
                    self.produce_check_in(scoping.project_id, received_at, client, retention, item)?
                }
                ItemType::Span if content_type == Some(&ContentType::Json) => self.produce_span(
                    scoping,
                    received_at,
                    event_id,
                    retention,
                    downsampled_retention,
                    item,
                )?,
                ItemType::Span if content_type == Some(&ContentType::CompatSpan) => self
                    .produce_span_v2(
                        scoping,
                        received_at,
                        event_id,
                        retention,
                        downsampled_retention,
                        item,
                    )?,
                ty @ ItemType::Log => {
                    debug_assert!(
                        false,
                        "received {ty} through an envelope, \
                        this item must be submitted via a specific store message instead"
                    );
                    relay_log::error!(
                        tags.project_key = %scoping.project_key,
                        "StoreService received unsupported item type '{ty}' in envelope"
                    );
                }
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
                replay_relay_snuba_publish_disabled,
            )?;
        }

        if let Some(event_item) = event_item {
            let event_id = event_id.ok_or(StoreError::NoEventId)?;
            let project_id = scoping.project_id;
            let remote_addr = envelope.meta().client_addr().map(|addr| addr.to_string());

            self.produce(
                event_topic,
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

    fn handle_store_trace_item(&self, message: Managed<StoreTraceItem>) {
        let scoping = message.scoping();
        let received_at = message.received_at();

        let quantities = message.try_accept(|item| {
            let item_type = item.trace_item.item_type();
            let message = KafkaMessage::Item {
                headers: BTreeMap::from([
                    ("project_id".to_owned(), scoping.project_id.to_string()),
                    ("item_type".to_owned(), (item_type as i32).to_string()),
                ]),
                message: item.trace_item,
                item_type,
            };

            self.produce(KafkaTopic::Items, message)
                .map(|()| item.quantities)
        });

        // Accepted outcomes when items have been successfully produced to rdkafka.
        //
        // This is only a temporary measure, long term these outcomes will be part of the trace
        // item and emitted by Snuba to guarantee a delivery to storage.
        if let Ok(quantities) = quantities {
            for (category, quantity) in quantities {
                self.outcome_aggregator.send(TrackOutcome {
                    category,
                    event_id: None,
                    outcome: Outcome::Accepted,
                    quantity: u32::try_from(quantity).unwrap_or(u32::MAX),
                    remote_addr: None,
                    scoping,
                    timestamp: received_at,
                });
            }
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

        let payload = item.payload();
        let size = item.len();
        let max_chunk_size = self.config.attachment_chunk_size();

        // When sending individual attachments, and we have a single chunk, we want to send the
        // `data` inline in the `attachment` message.
        // This avoids a needless roundtrip through the attachments cache on the Sentry side.
        let payload = if size == 0 {
            AttachmentPayload::Chunked(0)
        } else if send_individual_attachments && size < max_chunk_size {
            AttachmentPayload::Inline(payload)
        } else {
            let mut chunk_index = 0;
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

            // The chunk_index is incremented after every loop iteration. After we exit the loop, it
            // is one larger than the last chunk, so it is equal to the number of chunks.
            AttachmentPayload::Chunked(chunk_index)
        };

        let attachment = ChunkedAttachment {
            id,
            name: match item.filename() {
                Some(name) => name.to_owned(),
                None => UNNAMED_ATTACHMENT.to_owned(),
            },
            rate_limited: item.rate_limited(),
            content_type: item
                .content_type()
                .map(|content_type| content_type.as_str().to_owned()),
            attachment_type: item.attachment_type().cloned().unwrap_or_default(),
            size,
            payload,
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
        let headers = BTreeMap::from([("namespace".to_owned(), namespace.to_string())]);

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
                "sampled".to_owned(),
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
        relay_snuba_publish_disabled: bool,
    ) -> Result<(), StoreError> {
        if relay_snuba_publish_disabled {
            return Ok(());
        }

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
        relay_snuba_publish_disabled: bool,
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
                relay_snuba_publish_disabled,
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
        relay_snuba_publish_disabled: bool,
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
            relay_snuba_publish_disabled,
        )?;

        self.produce_replay_recording(
            event_id,
            scoping,
            replay_recording,
            Some(replay_event),
            Some(replay_video),
            received_at,
            retention,
            relay_snuba_publish_disabled,
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
        downsampled_retention_days: u16,
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

        // Discard measurements with empty `value`s.
        if let Some(measurements) = &mut span.measurements {
            measurements.retain(|_, v| v.as_ref().and_then(|v| v.value).is_some());
        }

        span.backfill_data();
        span.duration_ms =
            ((span.end_timestamp_precise - span.start_timestamp_precise) * 1e3) as u32;
        span.event_id = event_id;
        span.organization_id = scoping.organization_id.value();
        span.project_id = scoping.project_id.value();
        span.retention_days = retention_days;
        span.downsampled_retention_days = downsampled_retention_days;
        span.start_timestamp_ms = (span.start_timestamp_precise * 1e3) as u64;
        span.key_id = scoping.key_id;

        self.produce(
            KafkaTopic::Spans,
            KafkaMessage::Span {
                headers: BTreeMap::from([(
                    "project_id".to_owned(),
                    scoping.project_id.to_string(),
                )]),
                message: span,
            },
        )?;

        // XXX: Temporarily produce span outcomes also for JSON spans. Keep in sync with either EAP
        // or the segments consumer, depending on which will produce outcomes later.
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

    fn produce_span_v2(
        &self,
        scoping: Scoping,
        received_at: DateTime<Utc>,
        event_id: Option<EventId>,
        retention_days: u16,
        downsampled_retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        debug_assert_eq!(item.ty(), &ItemType::Span);
        debug_assert_eq!(item.content_type(), Some(&ContentType::CompatSpan));

        let Scoping {
            organization_id,
            project_id,
            project_key: _,
            key_id,
        } = scoping;

        let payload = item.payload();
        let message = SpanV2KafkaMessage {
            meta: SpanMeta {
                organization_id,
                project_id,
                key_id,
                event_id,
                retention_days,
                downsampled_retention_days,
                received: datetime_to_timestamp(received_at),
            },
            span: serde_json::from_slice(&payload)
                .map_err(|e| StoreError::EncodingFailed(e.into()))?,
        };

        relay_statsd::metric!(counter(RelayCounters::SpanV2Produced) += 1);
        self.produce(
            KafkaTopic::Spans,
            KafkaMessage::SpanV2 {
                routing_key: item.routing_hint(),
                headers: BTreeMap::from([(
                    "project_id".to_owned(),
                    scoping.project_id.to_string(),
                )]),
                message,
            },
        )?;

        // XXX: Temporarily produce span outcomes. Keep in sync with either EAP
        // or the segments consumer, depending on which will produce outcomes later.
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

/// This signifies how the attachment payload is being transfered.
#[derive(Debug, Serialize)]
enum AttachmentPayload {
    /// The payload has been split into multiple chunks.
    ///
    /// The individual chunks are being sent as separate [`AttachmentChunkKafkaMessage`] messages.
    /// If the payload `size == 0`, the number of chunks will also be `0`.
    #[serde(rename = "chunks")]
    Chunked(usize),

    /// The payload is inlined here directly, and thus into the [`ChunkedAttachment`].
    #[serde(rename = "data")]
    Inline(Bytes),

    /// The attachment has already been stored into the objectstore, with the given Id.
    #[serde(rename = "stored_id")]
    #[allow(unused)] // TODO: actually storing it in objectstore first is still WIP
    Stored(String),
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

    /// Whether this attachment was rate limited and should be removed after processing.
    ///
    /// By default, rate limited attachments are immediately removed from Envelopes. For processing,
    /// native crash reports still need to be retained. These attachments are marked with the
    /// `rate_limited` header, which signals to the processing pipeline that the attachment should
    /// not be persisted after processing.
    rate_limited: bool,

    /// Content type of the attachment payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,

    /// The Sentry-internal attachment type used in the processing pipeline.
    #[serde(serialize_with = "serialize_attachment_type")]
    attachment_type: AttachmentType,

    /// The size of the attachment in bytes.
    size: usize,

    /// The attachment payload, chunked, inlined, or already stored.
    #[serde(flatten)]
    payload: AttachmentPayload,
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

fn serialize_btreemap_skip_nulls<K, S, T>(
    map: &Option<BTreeMap<K, Option<T>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    K: Serialize,
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
    relay_snuba_publish_disabled: bool,
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

#[derive(Debug, Deserialize, Serialize, Clone)]
struct SpanLink<'a> {
    pub trace_id: &'a str,
    pub span_id: &'a str,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampled: Option<bool>,
    #[serde(borrow)]
    pub attributes: Option<&'a RawValue>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct SpanMeasurement<'a> {
    #[serde(skip_serializing_if = "Option::is_none", borrow)]
    value: Option<&'a RawValue>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct SpanKafkaMessage<'a> {
    #[serde(skip_serializing_if = "Option::is_none", borrow)]
    description: Option<Cow<'a, str>>,
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

    #[serde(skip_serializing_if = "none_or_empty_map", borrow)]
    data: Option<BTreeMap<Cow<'a, str>, Option<&'a RawValue>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kind: Option<Cow<'a, str>>,
    #[serde(default, skip_serializing_if = "none_or_empty_vec")]
    links: Option<Vec<SpanLink<'a>>>,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    measurements: Option<BTreeMap<Cow<'a, str>, Option<SpanMeasurement<'a>>>>,
    #[serde(default)]
    organization_id: u64,
    #[serde(borrow, default, skip_serializing_if = "Option::is_none")]
    origin: Option<Cow<'a, str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<Cow<'a, str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    profile_id: Option<Cow<'a, str>>,
    /// The numeric ID of the project.
    #[serde(default)]
    project_id: u64,
    /// Time at which the event was received by Relay. Not to be confused with `start_timestamp_ms`.
    received: f64,
    /// Number of days until these data should be deleted.
    #[serde(default)]
    retention_days: u16,
    /// Number of days until the downsampled version of this data should be deleted.
    #[serde(default)]
    downsampled_retention_days: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segment_id: Option<Cow<'a, str>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_btreemap_skip_nulls"
    )]
    #[serde(borrow)]
    sentry_tags: Option<BTreeMap<Cow<'a, str>, Option<&'a RawValue>>>,
    span_id: Cow<'a, str>,
    #[serde(skip_serializing_if = "none_or_empty_map", borrow)]
    tags: Option<BTreeMap<Cow<'a, str>, Option<&'a RawValue>>>,
    trace_id: EventId,

    #[serde(default)]
    start_timestamp_ms: u64,
    #[serde(rename(deserialize = "start_timestamp"))]
    start_timestamp_precise: f64,
    #[serde(rename(deserialize = "timestamp"))]
    end_timestamp_precise: f64,

    #[serde(borrow, default, skip_serializing)]
    platform: Cow<'a, str>, // We only use this for logging for now

    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_sample_rate: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    server_sample_rate: Option<f64>,

    #[serde(
        default,
        rename = "_meta",
        skip_serializing_if = "none_or_empty_object"
    )]
    meta: Option<&'a RawValue>,

    #[serde(
        default,
        rename = "_performance_issues_spans",
        skip_serializing_if = "Option::is_none"
    )]
    performance_issues_spans: Option<bool>,

    // Required for the buffer to emit outcomes scoped to the DSN.
    #[serde(skip_serializing_if = "Option::is_none")]
    key_id: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SpanV2KafkaMessage<'a> {
    #[serde(flatten)]
    meta: SpanMeta,
    #[serde(flatten)]
    span: BTreeMap<&'a str, &'a RawValue>,
}

#[derive(Debug, Serialize)]
struct SpanMeta {
    organization_id: OrganizationId,
    project_id: ProjectId,
    // Required for the buffer to emit outcomes scoped to the DSN.
    #[serde(skip_serializing_if = "Option::is_none")]
    key_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: Option<EventId>,
    /// Time at which the event was received by Relay. Not to be confused with `start_timestamp_ms`.
    received: f64,
    /// Number of days until these data should be deleted.
    retention_days: u16,
    /// Number of days until the downsampled version of this data should be deleted.
    downsampled_retention_days: u16,
}

impl SpanKafkaMessage<'_> {
    /// Backfills `data` based on `sentry_tags`, `tags`, and `measurements`.
    ///
    /// * Every item in `sentry_tags` is copied to `data`, with the key prefixed with `sentry.`.
    ///   The only exception is the `description` tag, which is copied as `sentry.normalized_description`.
    ///
    /// * Every item in `tags` is copied to `data` verbatim, with the exception of `description`, which
    ///   is copied as `sentry.normalized_description`.
    ///
    /// * The value of every item in `measurements` is copied to `data` with the same key, with the exceptions
    ///   of `client_sample_rate` and `server_sample_rate`. Those measurements are instead written to the top-level
    ///   fields of the same names.
    ///
    /// In no case are existing keys overwritten. Thus, from highest to lowest, the order of precedence is
    /// * existing values in `data`
    /// * `measurements`
    /// * `tags`
    /// * `sentry_tags`
    fn backfill_data(&mut self) {
        let data = self.data.get_or_insert_default();

        if let Some(measurements) = &self.measurements {
            for (key, value) in measurements {
                let Some(value) = value.as_ref().and_then(|v| v.value) else {
                    continue;
                };

                match key.as_ref() {
                    "client_sample_rate" => {
                        data.entry(Cow::Borrowed("sentry.client_sample_rate"))
                            .or_insert(Some(value));

                        if let Ok(client_sample_rate) = Deserialize::deserialize(value) {
                            self.client_sample_rate = Some(client_sample_rate);
                        }
                    }
                    "server_sample_rate" => {
                        data.entry(Cow::Borrowed("sentry.server_sample_rate"))
                            .or_insert(Some(value));

                        if let Ok(server_sample_rate) = Deserialize::deserialize(value) {
                            self.server_sample_rate = Some(server_sample_rate);
                        }
                    }
                    _ => {
                        data.entry(key.clone()).or_insert(Some(value));
                    }
                }
            }
        }

        if let Some(tags) = &self.tags {
            for (key, value) in tags {
                let Some(value) = value else {
                    continue;
                };

                let key = if *key == "description" {
                    Cow::Borrowed("sentry.normalized_description")
                } else {
                    key.clone()
                };

                data.entry(key).or_insert(Some(value));
            }
        }

        if let Some(sentry_tags) = &self.sentry_tags {
            for (key, value) in sentry_tags {
                let Some(value) = value else {
                    continue;
                };

                let key = if *key == "description" {
                    Cow::Borrowed("sentry.normalized_description")
                } else {
                    Cow::Owned(format!("sentry.{key}"))
                };

                data.entry(key).or_insert(Some(value));
            }
        }
    }
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
    UserReport(UserReportKafkaMessage),
    Metric {
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: MetricKafkaMessage<'a>,
    },
    CheckIn(CheckInKafkaMessage),
    Item {
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(skip)]
        item_type: TraceItemType,
        #[serde(skip)]
        message: TraceItem,
    },
    Span {
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: SpanKafkaMessage<'a>,
    },
    SpanV2 {
        #[serde(skip)]
        routing_key: Option<Uuid>,
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: SpanV2KafkaMessage<'a>,
    },

    Attachment(AttachmentKafkaMessage),
    AttachmentChunk(AttachmentChunkKafkaMessage),

    Profile(ProfileKafkaMessage),
    ProfileChunk(ProfileChunkKafkaMessage),

    ReplayEvent(ReplayEventKafkaMessage<'a>),
    ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage<'a>),
}

impl Message for KafkaMessage<'_> {
    fn variant(&self) -> &'static str {
        match self {
            KafkaMessage::Event(_) => "event",
            KafkaMessage::UserReport(_) => "user_report",
            KafkaMessage::Metric { message, .. } => match message.name.namespace() {
                MetricNamespace::Sessions => "metric_sessions",
                MetricNamespace::Transactions => "metric_transactions",
                MetricNamespace::Spans => "metric_spans",
                MetricNamespace::Custom => "metric_custom",
                MetricNamespace::Unsupported => "metric_unsupported",
            },
            KafkaMessage::CheckIn(_) => "check_in",
            KafkaMessage::Span { .. } | KafkaMessage::SpanV2 { .. } => "span",
            KafkaMessage::Item { item_type, .. } => item_type.as_str_name(),

            KafkaMessage::Attachment(_) => "attachment",
            KafkaMessage::AttachmentChunk(_) => "attachment_chunk",

            KafkaMessage::Profile(_) => "profile",
            KafkaMessage::ProfileChunk(_) => "profile_chunk",

            KafkaMessage::ReplayEvent(_) => "replay_event",
            KafkaMessage::ReplayRecordingNotChunked(_) => "replay_recording_not_chunked",
        }
    }

    /// Returns the partitioning key for this Kafka message determining.
    fn key(&self) -> Option<relay_kafka::Key> {
        match self {
            Self::Event(message) => Some(message.event_id.0),
            Self::UserReport(message) => Some(message.event_id.0),
            Self::Span { message, .. } => Some(message.trace_id.0),
            Self::SpanV2 { routing_key, .. } => *routing_key,

            // Monitor check-ins use the hinted UUID passed through from the Envelope.
            //
            // XXX(epurkhiser): In the future it would be better if all KafkaMessage's would
            // recieve the routing_key_hint form their envelopes.
            Self::CheckIn(message) => message.routing_key_hint,

            Self::Attachment(message) => Some(message.event_id.0),
            Self::AttachmentChunk(message) => Some(message.event_id.0),
            Self::ReplayEvent(message) => Some(message.replay_id.0),

            // Random partitioning
            Self::Metric { .. }
            | Self::Item { .. }
            | Self::Profile(_)
            | Self::ProfileChunk(_)
            | Self::ReplayRecordingNotChunked(_) => None,
        }
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.as_u128())
    }

    fn headers(&self) -> Option<&BTreeMap<String, String>> {
        match &self {
            KafkaMessage::Metric { headers, .. }
            | KafkaMessage::Span { headers, .. }
            | KafkaMessage::SpanV2 { headers, .. }
            | KafkaMessage::Item { headers, .. }
            | KafkaMessage::Profile(ProfileKafkaMessage { headers, .. }) => Some(headers),

            KafkaMessage::Event(_)
            | KafkaMessage::UserReport(_)
            | KafkaMessage::CheckIn(_)
            | KafkaMessage::Attachment(_)
            | KafkaMessage::AttachmentChunk(_)
            | KafkaMessage::ProfileChunk(_)
            | KafkaMessage::ReplayEvent(_)
            | KafkaMessage::ReplayRecordingNotChunked(_) => None,
        }
    }

    fn serialize(&self) -> Result<SerializationOutput<'_>, ClientError> {
        match self {
            KafkaMessage::Metric { message, .. } => serialize_as_json(message),
            KafkaMessage::ReplayEvent(message) => serialize_as_json(message),
            KafkaMessage::Span { message, .. } => serialize_as_json(message),
            KafkaMessage::SpanV2 { message, .. } => serialize_as_json(message),
            KafkaMessage::Item { message, .. } => {
                let mut payload = Vec::new();
                match message.encode(&mut payload) {
                    Ok(_) => Ok(SerializationOutput::Protobuf(Cow::Owned(payload))),
                    Err(_) => Err(ClientError::ProtobufEncodingFailed),
                }
            }
            KafkaMessage::Event(_)
            | KafkaMessage::UserReport(_)
            | KafkaMessage::CheckIn(_)
            | KafkaMessage::Attachment(_)
            | KafkaMessage::AttachmentChunk(_)
            | KafkaMessage::Profile(_)
            | KafkaMessage::ProfileChunk(_)
            | KafkaMessage::ReplayRecordingNotChunked(_) => match rmp_serde::to_vec_named(&self) {
                Ok(x) => Ok(SerializationOutput::MsgPack(Cow::Owned(x))),
                Err(err) => Err(ClientError::InvalidMsgPack(err)),
            },
        }
    }
}

fn serialize_as_json<T: serde::Serialize>(
    value: &T,
) -> Result<SerializationOutput<'_>, ClientError> {
    match serde_json::to_vec(value) {
        Ok(vec) => Ok(SerializationOutput::Json(Cow::Owned(vec))),
        Err(err) => Err(ClientError::InvalidJson(err)),
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
                .send(topic, Some(0x0123456789abcdef), None, "foo", b"");

            assert!(matches!(res, Err(ClientError::InvalidTopicName)));
        }
    }

    #[test]
    fn backfill() {
        let json = r#"{
            "description": "/api/0/relays/projectconfigs/",
            "duration_ms": 152,
            "exclusive_time": 0.228,
            "is_segment": true,
            "data": {
                "sentry.environment": "development",
                "sentry.release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
                "thread.name": "uWSGIWorker1Core0",
                "thread.id": "8522009600",
                "sentry.segment.name": "/api/0/relays/projectconfigs/",
                "sentry.sdk.name": "sentry.python.django",
                "sentry.sdk.version": "2.7.0",
                "my.float.field": 101.2,
                "my.int.field": 2000,
                "my.neg.field": -100,
                "my.neg.float.field": -101.2,
                "my.true.bool.field": true,
                "my.false.bool.field": false,
                "my.dict.field": {
                    "id": 42,
                    "name": "test"
                },
                "my.u64.field": 9447000002305251000,
                "my.array.field": [1, 2, ["nested", "array"]]
            },
            "measurements": {
                "num_of_spans": {"value": 50.0},
                "client_sample_rate": {"value": 0.1},
                "server_sample_rate": {"value": 0.2}
            },
            "profile_id": "56c7d1401ea14ad7b4ac86de46baebae",
            "organization_id": 1,
            "origin": "auto.http.django",
            "project_id": 1,
            "received": 1721319572.877828,
            "retention_days": 90,
            "segment_id": "8873a98879faf06d",
            "sentry_tags": {
                "description": "normalized_description",
                "category": "http",
                "environment": "development",
                "op": "http.server",
                "platform": "python",
                "release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
                "sdk.name": "sentry.python.django",
                "sdk.version": "2.7.0",
                "status": "ok",
                "status_code": "200",
                "thread.id": "8522009600",
                "thread.name": "uWSGIWorker1Core0",
                "trace.status": "ok",
                "transaction": "/api/0/relays/projectconfigs/",
                "transaction.method": "POST",
                "transaction.op": "http.server",
                "user": "ip:127.0.0.1"
            },
            "span_id": "8873a98879faf06d",
            "tags": {
                "http.status_code": "200",
                "relay_endpoint_version": "3",
                "relay_id": "88888888-4444-4444-8444-cccccccccccc",
                "relay_no_cache": "False",
                "relay_protocol_version": "3",
                "relay_use_post_or_schedule": "True",
                "relay_use_post_or_schedule_rejected": "version",
                "server_name": "D23CXQ4GK2.local",
                "spans_over_limit": "False"
            },
            "trace_id": "d099bf9ad5a143cf8f83a98081d0ed3b",
            "start_timestamp_ms": 1721319572616,
            "start_timestamp": 1721319572.616648,
            "timestamp": 1721319572.768806
        }"#;
        let mut span: SpanKafkaMessage = serde_json::from_str(json).unwrap();
        span.backfill_data();

        assert_eq!(
            serde_json::to_string_pretty(&span.data).unwrap(),
            r#"{
  "http.status_code": "200",
  "my.array.field": [1, 2, ["nested", "array"]],
  "my.dict.field": {
                    "id": 42,
                    "name": "test"
                },
  "my.false.bool.field": false,
  "my.float.field": 101.2,
  "my.int.field": 2000,
  "my.neg.field": -100,
  "my.neg.float.field": -101.2,
  "my.true.bool.field": true,
  "my.u64.field": 9447000002305251000,
  "num_of_spans": 50.0,
  "relay_endpoint_version": "3",
  "relay_id": "88888888-4444-4444-8444-cccccccccccc",
  "relay_no_cache": "False",
  "relay_protocol_version": "3",
  "relay_use_post_or_schedule": "True",
  "relay_use_post_or_schedule_rejected": "version",
  "sentry.category": "http",
  "sentry.client_sample_rate": 0.1,
  "sentry.environment": "development",
  "sentry.normalized_description": "normalized_description",
  "sentry.op": "http.server",
  "sentry.platform": "python",
  "sentry.release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
  "sentry.sdk.name": "sentry.python.django",
  "sentry.sdk.version": "2.7.0",
  "sentry.segment.name": "/api/0/relays/projectconfigs/",
  "sentry.server_sample_rate": 0.2,
  "sentry.status": "ok",
  "sentry.status_code": "200",
  "sentry.thread.id": "8522009600",
  "sentry.thread.name": "uWSGIWorker1Core0",
  "sentry.trace.status": "ok",
  "sentry.transaction": "/api/0/relays/projectconfigs/",
  "sentry.transaction.method": "POST",
  "sentry.transaction.op": "http.server",
  "sentry.user": "ip:127.0.0.1",
  "server_name": "D23CXQ4GK2.local",
  "spans_over_limit": "False",
  "thread.id": "8522009600",
  "thread.name": "uWSGIWorker1Core0"
}"#
        );
    }
}
