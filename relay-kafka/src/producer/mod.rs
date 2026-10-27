//! This module contains the Kafka producer related code.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use rdkafka::message::Header;
use rdkafka::producer::BaseRecord;
use relay_statsd::metric;
use sentry_arroyo::backends::ProducerError;
use sentry_arroyo::backends::kafka::config_blob::ProducerSelector;
use sentry_arroyo::backends::kafka::reloading_producer::{
    ReloadConfig, ReloadError, ReloadingKafkaProducer,
};
use sentry_arroyo::types::Topic;
use thiserror::Error;

use crate::KafkaTopicConfig;
use crate::config::{KafkaParams, KafkaTopic};
use crate::debounced::Debounced;
use crate::limits::KafkaRateLimits;
use crate::producer::utils::KafkaHeaders;
use crate::statsd::{KafkaCounters, KafkaDistributions, KafkaGauges};

mod utils;
use utils::Context;

#[cfg(debug_assertions)]
mod schemas;

const REPORT_FREQUENCY_SECS: u64 = 1;
const KAFKA_FETCH_METADATA_TIMEOUT: Duration = Duration::from_secs(30);

/// Key type used for partitioning.
pub type Key = u128;

/// Kafka producer errors.
#[derive(Error, Debug)]
pub enum ClientError {
    /// Failed to send a Kafka message using Arroyo.
    #[error("failed to send kafka message")]
    SendFailed(#[source] ProducerError),

    /// Failed to find configured producer for the requested kafka topic.
    #[error("failed to find producer for the requested kafka topic")]
    InvalidTopicName,

    /// Failed to serialize the message.
    #[error("failed to serialize kafka message")]
    InvalidMsgPack(#[source] rmp_serde::encode::Error),

    /// Failed to serialize the json message using serde.
    #[error("failed to serialize json message")]
    InvalidJson(#[from] serde_json::Error),

    /// Failed to run schema validation on message.
    #[cfg(debug_assertions)]
    #[error("schema validation failed")]
    SchemaValidationFailed(#[source] schemas::SchemaError),

    /// Configuration is wrong and it cannot be used to identify the number of a shard.
    #[error("no kafka configuration for topic")]
    MissingTopic,

    /// Failed to validate the topic using Arroyo.
    #[error("failed to validate the topic with name {0}: {1}")]
    TopicError(String, #[source] rdkafka::error::KafkaError),

    /// Failed to create a kafka producer, for example because the config blob does not resolve
    /// for the topic or the resolved config is invalid.
    #[error("failed to create kafka producer")]
    Reload(#[from] ReloadError),

    /// Failed to encode the protobuf into the buffer
    /// because the buffer is too small.
    #[error("failed to encode protobuf because the buffer is too small")]
    ProtobufEncodingFailed,
}

/// Describes the type which can be sent using kafka producer provided by this crate.
pub trait Message {
    /// Returns the partitioning key for this kafka message determining.
    fn key(&self) -> Option<Key>;

    /// Returns the type of the message.
    fn variant(&self) -> &'static str;

    /// Return the list of headers to be provided when payload is sent to Kafka.
    fn headers(&self) -> Option<&BTreeMap<String, String>>;

    /// Serializes the message into its binary format.
    ///
    /// # Errors
    /// Returns the [`ClientError::InvalidMsgPack`], [`ClientError::InvalidJson`] or [`ClientError::ProtobufEncodingFailed`]  if the
    /// serialization failed.
    fn serialize(&self) -> Result<SerializationOutput<'_>, ClientError>;
}

/// The output of serializing a message for kafka.
#[derive(Debug, Clone)]
pub enum SerializationOutput<'a> {
    /// Serialized as Json.
    Json(Cow<'a, [u8]>),

    /// Serialized as MsgPack.
    MsgPack(Cow<'a, [u8]>),

    /// Serialized as Protobuf.
    Protobuf(Cow<'a, [u8]>),
}

impl SerializationOutput<'_> {
    /// Return the serialized bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SerializationOutput::Json(cow) => cow,
            SerializationOutput::MsgPack(cow) => cow,
            SerializationOutput::Protobuf(cow) => cow,
        }
    }
}

struct TopicProducers {
    /// All configured producers.
    producers: Vec<TopicProducer>,
}

impl TopicProducers {
    fn new() -> Self {
        Self {
            producers: Vec::new(),
        }
    }

    fn select(&self, key: u128) -> Option<&TopicProducer> {
        debug_assert!(!self.producers.is_empty());

        if self.producers.is_empty() {
            return None;
        } else if self.producers.len() == 1 {
            return self.producers.first();
        }

        let select = (key % self.producers.len() as u128) as usize;
        self.producers.get(select)
    }

    /// Validates the topic by fetching the metadata of the topic directly from Kafka.
    fn validate_topic(&self) -> Result<(), ClientError> {
        for tp in &self.producers {
            tp.producer
                .validate_topic(Topic::new(&tp.topic_name), KAFKA_FETCH_METADATA_TIMEOUT)
                .map_err(|error| ClientError::TopicError(tp.topic_name.clone(), error))?;
        }

        Ok(())
    }
}

struct TopicProducer {
    pub topic_name: String,
    pub producer: Arc<ReloadingKafkaProducer<Context>>,
    pub rate_limiter: Option<KafkaRateLimits>,
}

/// Where producers take their initial Kafka config from, and how they reload it.
#[derive(Debug, Clone, Copy)]
pub struct KafkaConfigSource<'a> {
    /// The Kafka config blob the initial clients are resolved from.
    pub blob: &'a [u8],
    /// Which application's overrides in the blob apply. For Relay, `"relay"`.
    pub app: &'a str,
    /// Upper bound on the random delay before a swap, to spread swaps across the fleet.
    pub jitter: Duration,
}

/// How Relay wants reloads carried out.
fn reload_config(jitter: Duration) -> ReloadConfig {
    ReloadConfig {
        // Relay keys messages to spread them evenly across partitions, not to
        // order them: an absent key is replaced by a generated one, see
        // `Producer::next_key`. Nothing downstream depends on two messages
        // with the same key keeping their order, so buffering produce calls
        // and draining the old client before installing the new one would
        // stall produce for no benefit.
        ignore_key_ordering: true,
        jitter,
        ..ReloadConfig::default()
    }
}

/// Single kafka producer config with assigned topic.
struct Producer {
    /// Topic to producer and rate limiter mappings for sharding.
    topic_producers: TopicProducers,
    /// Debouncer for metrics.
    metrics: Debounced,
    next_key: AtomicU32,
}

impl Producer {
    fn new(topic_producers: TopicProducers) -> Self {
        Self {
            topic_producers,
            metrics: Debounced::new(REPORT_FREQUENCY_SECS),
            next_key: AtomicU32::new(0),
        }
    }
}

impl Producer {
    /// Sends the payload to the correct producer for the current topic.
    fn send(
        &self,
        key: Option<Key>,
        headers: Option<&BTreeMap<String, String>>,
        variant: &str,
        payload: &[u8],
    ) -> Result<&str, ClientError> {
        let now = Instant::now();

        // Always generate a key to force a slightly more equal distribution across partitions,
        // see also the documentation for `Self::next_key`.
        let mut key = key.unwrap_or_else(|| self.next_key());

        let Some(TopicProducer {
            topic_name,
            producer,
            rate_limiter,
        }) = self.topic_producers.select(key)
        else {
            return Err(ClientError::MissingTopic);
        };

        relay_log::configure_scope(|s| s.set_tag("topic", topic_name));

        let producer_name = producer.context().producer_name();

        metric!(
            distribution(KafkaDistributions::KafkaMessageSize) = payload.len() as u64,
            variant = variant,
            topic = topic_name,
            producer_name = producer_name
        );

        let mut headers = headers
            .unwrap_or(&BTreeMap::new())
            .iter()
            .map(|(key, value)| Header {
                key,
                value: Some(value),
            })
            .collect::<KafkaHeaders>();

        // Always rate limit if there is a rate limiter defined.
        // Defining a rate limiter for a topic which does not have a consistent routing key is just
        // a misconfiguration.
        if let Some(limiter) = &rate_limiter {
            let is_limited = limiter.try_increment(now, key, 1) < 1;

            if is_limited {
                metric!(
                    counter(KafkaCounters::ProducerPartitionKeyRateLimit) += 1,
                    variant = variant,
                    topic = topic_name,
                    producer_name = producer_name
                );

                headers.insert(Header {
                    key: "sentry-reshuffled",
                    value: Some("1"),
                });

                // Force a 'random' partition, instead the originally assigned partition.
                key = self.next_key();
            }
        }

        let key = u128::to_be_bytes(key);
        let mut record = BaseRecord::to(topic_name).payload(payload).key(&key);
        if let Some(headers) = headers.into_inner() {
            record = record.headers(headers);
        }

        self.metrics.debounce(now, || {
            metric!(
                gauge(KafkaGauges::InFlightCount) = producer.in_flight_count() as u64,
                variant = variant,
                topic = topic_name,
                producer_name = producer_name
            );
            // Flat at zero for a static producer. Watching it move is how a
            // config reload is confirmed to have reached this topic.
            metric!(
                gauge(KafkaGauges::ReloadGeneration) = producer.generation(),
                topic = topic_name,
                producer_name = producer_name
            );
        });

        producer
            .produce_record(record)
            .map_err(ClientError::SendFailed)
            .inspect_err(|_| {
                metric!(
                    counter(KafkaCounters::ProducerEnqueueError) += 1,
                    variant = variant,
                    topic = topic_name,
                    producer_name = producer_name
                );
            })?;

        Ok(topic_name)
    }

    /// Returns a newly generated key.
    ///
    /// Keys are created in sequence, they can be used as a Kafka partition key to force an equal
    /// distribution across partitions.
    ///
    /// Producing a Kafka message without a 'random' key, produces slightly uneven batches to
    /// partitions. We've seen that this pattern does not play well with our Arroyo consumers
    /// and leads to higher partition lags.
    fn next_key(&self) -> u128 {
        // Overflowing is fine, as it would wrap. The only use for the atomic is to generate
        // a different key to the one before.
        self.next_key.fetch_add(1, Ordering::Relaxed) as u128
    }
}

impl fmt::Debug for Producer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic_names: Vec<&String> = self
            .topic_producers
            .producers
            .iter()
            .map(|tp| &tp.topic_name)
            .collect();
        f.debug_struct("Producer")
            .field("topic_names", &topic_names)
            .field("producers", &"<KafkaProducers>")
            .finish_non_exhaustive()
    }
}

/// Keeps all the configured Kafka producers and responsible for the routing of the messages.
#[derive(Debug)]
pub struct KafkaClient {
    producers: HashMap<KafkaTopic, Producer>,
    #[cfg(debug_assertions)]
    schema_validator: schemas::Validator,
}

impl KafkaClient {
    /// Creates a Kafka client builder.
    pub fn builder() -> KafkaClientBuilder {
        KafkaClientBuilder::new()
    }

    /// Sends message to the provided Kafka topic.
    ///
    /// Returns the name of the Kafka topic to which the message was produced.
    pub fn send_message(
        &self,
        topic: KafkaTopic,
        message: &impl Message,
    ) -> Result<&str, ClientError> {
        let serialized = message.serialize()?;

        #[cfg(debug_assertions)]
        if let SerializationOutput::Json(ref bytes) = serialized {
            self.schema_validator
                .validate_message_schema(topic, bytes)
                .map_err(ClientError::SchemaValidationFailed)?;
        }
        self.send(
            topic,
            message.key(),
            message.headers(),
            message.variant(),
            serialized.as_bytes(),
        )
    }

    /// Sends the payload to the correct producer for the current topic.
    ///
    /// Returns the name of the Kafka topic to which the message was produced.
    fn send(
        &self,
        topic: KafkaTopic,
        key: Option<Key>,
        headers: Option<&BTreeMap<String, String>>,
        variant: &str,
        payload: &[u8],
    ) -> Result<&str, ClientError> {
        let producer = self
            .producers
            .get(&topic)
            .ok_or_else(|| ClientError::InvalidTopicName)?;

        producer.send(key, headers, variant, payload)
    }

    /// Hands a new Kafka config blob to every topic's producer.
    ///
    /// A blob that does not resolve for a topic is logged and counted rather
    /// than returned; that topic keeps the client it is running.
    ///
    /// A successful push only means the rollout started. Arroyo probes a
    /// broker before swapping; watch `kafka.reload_generation` for the swap.
    pub fn push_kafka_config(&self, blob: &[u8]) {
        for producer in self.producers.values() {
            for tp in &producer.topic_producers.producers {
                let producer_name = tp.producer.context().producer_name();

                match tp.producer.push_config(blob) {
                    Ok(_) => metric!(
                        counter(KafkaCounters::ConfigReloadPushed) += 1,
                        topic = &tp.topic_name,
                        producer_name = producer_name
                    ),
                    Err(error) => {
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            tags.topic = tp.topic_name.as_str(),
                            "could not apply the kafka config, staying on the running one",
                        );
                        metric!(
                            counter(KafkaCounters::ConfigReloadRejected) += 1,
                            topic = &tp.topic_name,
                            producer_name = producer_name
                        );
                    }
                }
            }
        }
    }
}

/// Helper structure responsible for building the actual [`KafkaClient`].
#[derive(Default)]
pub struct KafkaClientBuilder {
    /// Producers by physical topic name.
    ///
    /// A producer resolves its config out of the blob by physical topic, so it can only be
    /// shared by logical topics that produce to that same physical topic.
    reused_producers: BTreeMap<String, Arc<ReloadingKafkaProducer<Context>>>,
    producers: HashMap<KafkaTopic, Producer>,
}

impl KafkaClientBuilder {
    /// Creates an empty Kafka client builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds topic configuration to the current [`KafkaClientBuilder`], which in return assigns
    /// dedicates producer to the topic which can will be used to send the messages.
    ///
    /// Each physical topic's client is resolved from the config blob in `source`, and is
    /// replaced whenever a newer blob is pushed in through [`KafkaClient::push_kafka_config`].
    ///
    /// # Errors
    /// Returns [`ClientError::Reload`] if a producer could not be created, for example because
    /// the blob has no entry for one of the topic's physical topics.
    pub fn add_kafka_topic_config(
        mut self,
        topic: KafkaTopic,
        topic_config: &KafkaTopicConfig<'_>,
        validate_topic: bool,
        source: &KafkaConfigSource<'_>,
    ) -> Result<Self, ClientError> {
        let mut topic_producers = TopicProducers::new();

        // Process each shard configuration (one KafkaParams per shard)
        // We must preserve the original order from the configuration
        // because hash-based routing depends on shard index positions
        for params in topic_config.topics() {
            let KafkaParams {
                topic_name,
                config_name,
                params: config_params,
                key_rate_limit,
            } = params;

            let rate_limiter = key_rate_limit.map(|limit| {
                KafkaRateLimits::new(
                    limit.limit_per_window,
                    Duration::from_secs(limit.window_secs),
                )
            });

            // Get or create the producer for this physical topic
            let kafka_producer = if let Some(producer) = self.reused_producers.get(topic_name) {
                Arc::clone(producer)
            } else {
                // Extract producer name from client.id, fallback to config name, then "unknown"
                let producer_name = config_params
                    .iter()
                    .find(|p| p.name == "client.id")
                    .map(|p| p.value.clone())
                    .or_else(|| config_name.map(str::to_owned))
                    .unwrap_or_else(|| "unknown".to_owned());

                let producer = Arc::new(ReloadingKafkaProducer::new_with_context(
                    source.blob,
                    ProducerSelector::new(topic_name.clone(), source.app),
                    reload_config(source.jitter),
                    Context::new(producer_name),
                )?);

                self.reused_producers
                    .insert(topic_name.clone(), Arc::clone(&producer));

                producer
            };

            topic_producers.producers.push(TopicProducer {
                topic_name: topic_name.clone(),
                producer: kafka_producer,
                rate_limiter,
            });
        }

        let producer = Producer::new(topic_producers);
        if validate_topic {
            producer.topic_producers.validate_topic()?;
        }
        self.producers.insert(topic, producer);

        Ok(self)
    }

    /// Consumes self and returns the built [`KafkaClient`].
    pub fn build(self) -> KafkaClient {
        KafkaClient {
            producers: self.producers,
            #[cfg(debug_assertions)]
            schema_validator: schemas::Validator::default(),
        }
    }
}

impl fmt::Debug for KafkaClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaClientBuilder")
            .field("reused_producers", &"<CachedProducers>")
            .field("producers", &self.producers)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::mocking::MockCluster;

    use super::*;
    use crate::config::{KafkaConfigParam, TopicAssignment};

    /// A blob covering the topics the tests produce to, in the shape
    /// [`ConfigSnapshot::kafka_config_blob`] emits.
    fn blob(servers: &str, topics: &[&str]) -> Vec<u8> {
        let topics: serde_json::Map<_, _> = topics
            .iter()
            .map(|topic| {
                (
                    (*topic).to_owned(),
                    serde_json::json!({"physical_topic": {"cluster": "default"}}),
                )
            })
            .collect();

        serde_json::to_vec(&serde_json::json!({
            "clusters": {"default": {"config": {"bootstrap.servers": servers}}},
            "topics": topics,
        }))
        .unwrap()
    }

    fn source(blob: &[u8]) -> KafkaConfigSource<'_> {
        KafkaConfigSource {
            blob,
            app: "relay",
            jitter: Duration::ZERO,
        }
    }

    fn params(servers: &str) -> Vec<KafkaConfigParam> {
        vec![KafkaConfigParam {
            name: "bootstrap.servers".to_owned(),
            value: servers.to_owned(),
        }]
    }

    fn producers(
        client: &KafkaClient,
        topic: KafkaTopic,
    ) -> Vec<&Arc<ReloadingKafkaProducer<Context>>> {
        client.producers[&topic]
            .topic_producers
            .producers
            .iter()
            .map(|tp| &tp.producer)
            .collect()
    }

    /// Every shard needs its own producer, since each is a separate physical
    /// topic with its own entry in the blob.
    #[test]
    fn test_sharded_topic_gets_one_producer_per_shard() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let assignment: TopicAssignment =
            serde_yaml::from_str(r#"["ingest-events-1", "ingest-events-2"]"#).unwrap();
        let blob = blob(&servers, &["ingest-events-1", "ingest-events-2"]);
        let params = params(&servers);
        let secondary = BTreeMap::new();
        let configs = assignment.kafka_configs(&params, &secondary).unwrap();

        let client = KafkaClient::builder()
            .add_kafka_topic_config(KafkaTopic::Events, &configs, false, &source(&blob))
            .unwrap()
            .build();

        let producers = producers(&client, KafkaTopic::Events);
        assert_eq!(producers.len(), 2);
        // Sharding routes by index, so two shards sharing one producer would
        // send both to whichever topic that producer resolved.
        assert!(
            !Arc::ptr_eq(producers[0], producers[1]),
            "shards share a producer, so one of them produces to the wrong topic"
        );
    }

    /// Logical topics producing to the same physical topic share a producer,
    /// while different physical topics on the same cluster do not.
    #[test]
    fn test_producers_are_shared_by_physical_topic() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let params = params(&servers);
        let secondary = BTreeMap::new();
        let blob = blob(&servers, &["outcomes", "ingest-events"]);
        let outcomes = TopicAssignment::from("outcomes".to_owned());
        let events = TopicAssignment::from("ingest-events".to_owned());
        let outcomes = outcomes.kafka_configs(&params, &secondary).unwrap();
        let events = events.kafka_configs(&params, &secondary).unwrap();

        let client = KafkaClient::builder()
            .add_kafka_topic_config(KafkaTopic::Outcomes, &outcomes, false, &source(&blob))
            .unwrap()
            .add_kafka_topic_config(
                KafkaTopic::OutcomesBilling,
                &outcomes,
                false,
                &source(&blob),
            )
            .unwrap()
            .add_kafka_topic_config(KafkaTopic::Events, &events, false, &source(&blob))
            .unwrap()
            .build();

        let outcomes = producers(&client, KafkaTopic::Outcomes)[0];
        let billing = producers(&client, KafkaTopic::OutcomesBilling)[0];
        let events = producers(&client, KafkaTopic::Events)[0];

        assert!(Arc::ptr_eq(outcomes, billing));
        assert!(!Arc::ptr_eq(outcomes, events));
    }

    /// A topic missing from the blob cannot get a producer, so building fails
    /// instead of producing with a config nobody asked for.
    #[test]
    fn test_topic_missing_from_blob_is_an_error() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let blob = blob(&servers, &["ingest-events"]);
        let params = params(&servers);
        let secondary = BTreeMap::new();
        let assignment = TopicAssignment::from("ingest-transactions".to_owned());
        let configs = assignment.kafka_configs(&params, &secondary).unwrap();

        let result = KafkaClient::builder().add_kafka_topic_config(
            KafkaTopic::Transactions,
            &configs,
            false,
            &source(&blob),
        );

        assert!(matches!(result, Err(ClientError::Reload(_))));
    }
}
