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
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::smallvec;
use uuid::Uuid;

use relay_base_schema::data_category::DataCategory;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_config::Config;
use relay_event_schema::protocol::{EventId, SpanV2, datetime_to_timestamp};
use relay_kafka::{ClientError, KafkaClient, KafkaTopic, Message, SerializationOutput};
use relay_metrics::{
    Bucket, BucketView, BucketViewValue, BucketsView, ByNamespace, GaugeValue, MetricName,
    MetricNamespace, SetView,
};
use relay_protocol::{Annotated, FiniteF64, SerializableAnnotated};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use relay_threading::AsyncPool;

use crate::envelope::{AttachmentType, ContentType, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, TypedEnvelope};
use crate::metrics::{ArrayEncoding, BucketEncoder, MetricOutcomes};
use crate::service::ServiceError;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::Processed;
use crate::statsd::{RelayCounters, RelayGauges, RelayTimers};
use crate::utils::{self, FormDataIter};

mod sessions;

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

/// Publishes an [`Envelope`](crate::envelope::Envelope) to the Sentry core application through Kafka topics.
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

/// Publishes a log item to the Sentry core application through Kafka.
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

/// Publishes a span item to the Sentry core application through Kafka.
#[derive(Debug)]
pub struct StoreSpanV2 {
    /// Routing key to assign a Kafka partition.
    pub routing_key: Option<Uuid>,
    /// Default retention of the span.
    pub retention_days: u16,
    /// Downsampled retention of the span.
    pub downsampled_retention_days: u16,
    /// The final Sentry compatible span item.
    pub item: SpanV2,
}

impl Counted for StoreSpanV2 {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::SpanIndexed, 1)]
    }
}

/// Publishes a singular profile chunk to Kafka.
#[derive(Debug)]
pub struct StoreProfileChunk {
    /// Default retention of the span.
    pub retention_days: u16,
    /// The serialized profile chunk payload.
    pub payload: Bytes,
    /// Outcome quantities associated with this profile.
    ///
    /// Quantities are different for backend and ui profile chunks.
    pub quantities: Quantities,
}

impl Counted for StoreProfileChunk {
    fn quantities(&self) -> Quantities {
        self.quantities.clone()
    }
}

/// A replay to be stored to Kafka.
#[derive(Debug)]
pub struct StoreReplay {
    /// The event ID.
    pub event_id: EventId,
    /// Number of days to retain.
    pub retention_days: u16,
    /// The recording payload (rrweb data).
    pub payload: Bytes,
    /// Optional replay event payload (JSON).
    pub replay_event: Option<Bytes>,
    /// Optional replay video.
    pub replay_video: Option<Bytes>,
}

impl Counted for StoreReplay {
    fn quantities(&self) -> Quantities {
        // Web replays currently count as 2 since they are 2 items in the envelop (event + recording).
        if self.replay_event.is_some() && self.replay_video.is_none() {
            smallvec![(DataCategory::Replay, 2)]
        } else {
            smallvec![(DataCategory::Replay, 1)]
        }
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
    /// A singular Span.
    Span(Managed<Box<StoreSpanV2>>),
    /// A singular profile chunk.
    ProfileChunk(Managed<StoreProfileChunk>),
    /// A singular replay.
    Replay(Managed<StoreReplay>),
}

impl Store {
    /// Returns the name of the message variant.
    fn variant(&self) -> &'static str {
        match self {
            Store::Envelope(_) => "envelope",
            Store::Metrics(_) => "metrics",
            Store::TraceItem(_) => "trace_item",
            Store::Span(_) => "span",
            Store::ProfileChunk(_) => "profile_chunk",
            Store::Replay(_) => "replay",
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

impl FromMessage<Managed<Box<StoreSpanV2>>> for Store {
    type Response = NoResponse;

    fn from_message(message: Managed<Box<StoreSpanV2>>, _: ()) -> Self {
        Self::Span(message)
    }
}

impl FromMessage<Managed<StoreProfileChunk>> for Store {
    type Response = NoResponse;

    fn from_message(message: Managed<StoreProfileChunk>, _: ()) -> Self {
        Self::ProfileChunk(message)
    }
}

impl FromMessage<Managed<StoreReplay>> for Store {
    type Response = NoResponse;

    fn from_message(message: Managed<StoreReplay>, _: ()) -> Self {
        Self::Replay(message)
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
                Store::Span(message) => self.handle_store_span(message),
                Store::ProfileChunk(message) => self.handle_store_profile_chunk(message),
                Store::Replay(message) => self.handle_store_replay(message),
            }
        })
    }

    fn handle_store_envelope(&self, message: StoreEnvelope) {
        let StoreEnvelope { mut envelope } = message;

        let scoping = envelope.scoping();
        match self.store_envelope(&mut envelope) {
            Ok(()) => envelope.accept(),
            Err(error) => {
                envelope.reject(Outcome::Invalid(DiscardReason::Internal));
                relay_log::error!(
                    error = &error as &dyn Error,
                    tags.project_key = %scoping.project_key,
                    "failed to store envelope"
                );
            }
        }
    }

    fn store_envelope(&self, managed_envelope: &mut ManagedEnvelope) -> Result<(), StoreError> {
        let mut envelope = managed_envelope.take_envelope();
        let received_at = managed_envelope.received_at();
        let scoping = managed_envelope.scoping();

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
                    )?;
                }
                ItemType::ReplayRecording => {
                    replay_recording = Some(item);
                }
                ItemType::ReplayEvent => {
                    replay_event = Some(item);
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

        let emit_sessions_to_eap = utils::is_rolled_out(
            scoping.organization_id.value(),
            global_config.options.sessions_eap_rollout_rate,
        )
        .is_keep();

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

            if emit_sessions_to_eap
                && let Some(trace_item) = sessions::to_trace_item(scoping, bucket, retention)
            {
                let message = KafkaMessage::for_item(scoping, trace_item);
                let _ = self.produce(KafkaTopic::Items, message);
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
            let message = KafkaMessage::for_item(scoping, item.trace_item);
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

    fn handle_store_span(&self, message: Managed<Box<StoreSpanV2>>) {
        let scoping = message.scoping();
        let received_at = message.received_at();

        let meta = SpanMeta {
            organization_id: scoping.organization_id,
            project_id: scoping.project_id,
            key_id: scoping.key_id,
            event_id: None,
            retention_days: message.retention_days,
            downsampled_retention_days: message.downsampled_retention_days,
            received: datetime_to_timestamp(received_at),
        };

        let result = message.try_accept(|span| {
            let item = Annotated::new(span.item);
            let message = KafkaMessage::SpanV2 {
                routing_key: span.routing_key,
                headers: BTreeMap::from([(
                    "project_id".to_owned(),
                    scoping.project_id.to_string(),
                )]),
                message: SpanKafkaMessage {
                    meta,
                    span: SerializableAnnotated(&item),
                },
            };

            self.produce(KafkaTopic::Spans, message)
        });

        if result.is_ok() {
            relay_statsd::metric!(
                counter(RelayCounters::SpanV2Produced) += 1,
                via = "processing"
            );

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
        }
    }

    fn handle_store_profile_chunk(&self, message: Managed<StoreProfileChunk>) {
        let scoping = message.scoping();
        let received_at = message.received_at();

        let _ = message.try_accept(|message| {
            let message = ProfileChunkKafkaMessage {
                organization_id: scoping.organization_id,
                project_id: scoping.project_id,
                received: safe_timestamp(received_at),
                retention_days: message.retention_days,
                headers: BTreeMap::from([(
                    "project_id".to_owned(),
                    scoping.project_id.to_string(),
                )]),
                payload: message.payload,
            };

            self.produce(KafkaTopic::Profiles, KafkaMessage::ProfileChunk(message))
        });
    }

    fn handle_store_replay(&self, message: Managed<StoreReplay>) {
        let scoping = message.scoping();
        let received_at = message.received_at();

        let _ = message.try_accept(|replay| {
            let kafka_msg =
                KafkaMessage::ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage {
                    replay_id: replay.event_id,
                    key_id: scoping.key_id,
                    org_id: scoping.organization_id,
                    project_id: scoping.project_id,
                    received: safe_timestamp(received_at),
                    retention_days: replay.retention_days,
                    payload: &replay.payload,
                    replay_event: replay.replay_event.as_deref(),
                    replay_video: replay.replay_video.as_deref(),
                    // Hardcoded to `true` to indicate to the consumer that it should always publish the
                    // replay_event as relay no longer does it.
                    relay_snuba_publish_disabled: true,
                });
            self.produce(KafkaTopic::ReplayRecordings, kafka_msg)
        });
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

        let topic_name = self
            .producer
            .client
            .send_message(topic, &message)
            .inspect_err(|err| {
                relay_log::error!(
                    error = err as &dyn Error,
                    tags.topic = ?topic,
                    tags.message = message.variant(),
                    "failed to produce to Kafka"
                )
            })?;

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

        let payload = if size == 0 {
            AttachmentPayload::Chunked(0)
        } else if let Some(stored_key) = item.stored_key() {
            AttachmentPayload::Stored(stored_key.into())
        } else if send_individual_attachments && size < max_chunk_size {
            // When sending individual attachments, and we have a single chunk, we want to send the
            // `data` inline in the `attachment` message.
            // This avoids a needless roundtrip through the attachments cache on the Sentry side.
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
            headers: BTreeMap::from([
                (
                    "sampled".to_owned(),
                    if item.sampled() { "true" } else { "false" }.to_owned(),
                ),
                ("project_id".to_owned(), project_id.to_string()),
            ]),
            payload: item.payload(),
        };
        self.produce(KafkaTopic::Profiles, KafkaMessage::Profile(message))?;
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
                // Hardcoded to `true` to indicate to the consumer that it should always publish the
                // replay_event as relay no longer does it.
                relay_snuba_publish_disabled: true,
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
        downsampled_retention_days: u16,
        item: &Item,
    ) -> Result<(), StoreError> {
        debug_assert_eq!(item.ty(), &ItemType::Span);
        debug_assert_eq!(item.content_type(), Some(&ContentType::Json));

        let Scoping {
            organization_id,
            project_id,
            project_key: _,
            key_id,
        } = scoping;

        let payload = item.payload();
        let message = SpanKafkaMessageRaw {
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

        // Verify that this is a V2 span:
        debug_assert!(message.span.contains_key("attributes"));
        relay_statsd::metric!(
            counter(RelayCounters::SpanV2Produced) += 1,
            via = "envelope"
        );

        self.produce(
            KafkaTopic::Spans,
            KafkaMessage::SpanRaw {
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

#[derive(Debug, Serialize)]
struct SpanKafkaMessageRaw<'a> {
    #[serde(flatten)]
    meta: SpanMeta,
    #[serde(flatten)]
    span: BTreeMap<&'a str, &'a RawValue>,
}

#[derive(Debug, Serialize)]
struct SpanKafkaMessage<'a> {
    #[serde(flatten)]
    meta: SpanMeta,
    #[serde(flatten)]
    span: SerializableAnnotated<'a, SpanV2>,
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

#[derive(Clone, Debug, Serialize)]
struct ProfileChunkKafkaMessage {
    organization_id: OrganizationId,
    project_id: ProjectId,
    received: u64,
    retention_days: u16,
    #[serde(skip)]
    headers: BTreeMap<String, String>,
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
    SpanRaw {
        #[serde(skip)]
        routing_key: Option<Uuid>,
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: SpanKafkaMessageRaw<'a>,
    },
    SpanV2 {
        #[serde(skip)]
        routing_key: Option<Uuid>,
        #[serde(skip)]
        headers: BTreeMap<String, String>,
        #[serde(flatten)]
        message: SpanKafkaMessage<'a>,
    },

    Attachment(AttachmentKafkaMessage),
    AttachmentChunk(AttachmentChunkKafkaMessage),

    Profile(ProfileKafkaMessage),
    ProfileChunk(ProfileChunkKafkaMessage),

    ReplayRecordingNotChunked(ReplayRecordingNotChunkedKafkaMessage<'a>),
}

impl KafkaMessage<'_> {
    /// Creates a [`KafkaMessage`] for a [`TraceItem`].
    fn for_item(scoping: Scoping, item: TraceItem) -> KafkaMessage<'static> {
        let item_type = item.item_type();
        KafkaMessage::Item {
            headers: BTreeMap::from([
                ("project_id".to_owned(), scoping.project_id.to_string()),
                ("item_type".to_owned(), (item_type as i32).to_string()),
            ]),
            message: item,
            item_type,
        }
    }
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
            KafkaMessage::SpanRaw { .. } | KafkaMessage::SpanV2 { .. } => "span",
            KafkaMessage::Item { item_type, .. } => item_type.as_str_name(),

            KafkaMessage::Attachment(_) => "attachment",
            KafkaMessage::AttachmentChunk(_) => "attachment_chunk",

            KafkaMessage::Profile(_) => "profile",
            KafkaMessage::ProfileChunk(_) => "profile_chunk",

            KafkaMessage::ReplayRecordingNotChunked(_) => "replay_recording_not_chunked",
        }
    }

    /// Returns the partitioning key for this Kafka message determining.
    fn key(&self) -> Option<relay_kafka::Key> {
        match self {
            Self::Event(message) => Some(message.event_id.0),
            Self::UserReport(message) => Some(message.event_id.0),
            Self::SpanRaw { routing_key, .. } | Self::SpanV2 { routing_key, .. } => *routing_key,

            // Monitor check-ins use the hinted UUID passed through from the Envelope.
            //
            // XXX(epurkhiser): In the future it would be better if all KafkaMessage's would
            // recieve the routing_key_hint form their envelopes.
            Self::CheckIn(message) => message.routing_key_hint,

            Self::Attachment(message) => Some(message.event_id.0),
            Self::AttachmentChunk(message) => Some(message.event_id.0),

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
            | KafkaMessage::SpanRaw { headers, .. }
            | KafkaMessage::SpanV2 { headers, .. }
            | KafkaMessage::Item { headers, .. }
            | KafkaMessage::Profile(ProfileKafkaMessage { headers, .. })
            | KafkaMessage::ProfileChunk(ProfileChunkKafkaMessage { headers, .. }) => Some(headers),

            KafkaMessage::Event(_)
            | KafkaMessage::UserReport(_)
            | KafkaMessage::CheckIn(_)
            | KafkaMessage::Attachment(_)
            | KafkaMessage::AttachmentChunk(_)
            | KafkaMessage::ReplayRecordingNotChunked(_) => None,
        }
    }

    fn serialize(&self) -> Result<SerializationOutput<'_>, ClientError> {
        match self {
            KafkaMessage::Metric { message, .. } => serialize_as_json(message),
            KafkaMessage::SpanRaw { message, .. } => serialize_as_json(message),
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
}
