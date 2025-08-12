//! This module contains the Kafka producer related code.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::message::Header;
use rdkafka::producer::{BaseRecord, Producer as _};
use relay_statsd::metric;
use thiserror::Error;

use crate::KafkaTopicConfig;
use crate::config::{KafkaParams, KafkaTopic};
use crate::debounced::Debounced;
use crate::limits::KafkaRateLimits;
use crate::producer::utils::KafkaHeaders;
use crate::statsd::{KafkaCounters, KafkaGauges, KafkaHistograms};

mod utils;
use utils::{Context, ThreadedProducer};

#[cfg(debug_assertions)]
mod schemas;

const REPORT_FREQUENCY_SECS: u64 = 1;
const KAFKA_FETCH_METADATA_TIMEOUT: Duration = Duration::from_secs(30);

/// Key type used for partitioning.
pub type Key = u128;

/// Kafka producer errors.
#[derive(Error, Debug)]
pub enum ClientError {
    /// Failed to send a kafka message.
    #[error("failed to send kafka message")]
    SendFailed(#[source] rdkafka::error::KafkaError),

    /// Failed to find configured producer for the requested kafka topic.
    #[error("failed to find producer for the requested kafka topic")]
    InvalidTopicName,

    /// Failed to create a kafka producer because of the invalid configuration.
    #[error("failed to create kafka producer: invalid kafka config")]
    InvalidConfig(#[source] rdkafka::error::KafkaError),

    /// Failed to serialize the message.
    #[error("failed to serialize kafka message")]
    InvalidMsgPack(#[source] rmp_serde::encode::Error),

    /// Failed to serialize the json message using serde.
    #[error("failed to serialize json message")]
    InvalidJson(#[source] serde_json::Error),

    /// Failed to run schema validation on message.
    #[cfg(debug_assertions)]
    #[error("failed to run schema validation on message")]
    SchemaValidationFailed(#[source] schemas::SchemaError),

    /// Configuration is wrong and it cannot be used to identify the number of a shard.
    #[error("no kafka configuration for topic")]
    MissingTopic,

    /// Failed to fetch the metadata of Kafka.
    #[error("failed to fetch the metadata of Kafka")]
    MetadataFetchError(rdkafka::error::KafkaError),

    /// Failed to validate the topic.
    #[error("failed to validate the topic with name {0}: {1:?}")]
    TopicError(String, rdkafka_sys::rd_kafka_resp_err_t),

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
            let client = tp.producer.client();
            let metadata = client
                .fetch_metadata(Some(&tp.topic_name), KAFKA_FETCH_METADATA_TIMEOUT)
                .map_err(ClientError::MetadataFetchError)?;

            for topic in metadata.topics() {
                if let Some(error) = topic.error() {
                    return Err(ClientError::TopicError(topic.name().to_owned(), error));
                }
            }
        }

        Ok(())
    }
}

struct TopicProducer {
    pub topic_name: String,
    pub producer: Arc<ThreadedProducer>,
    pub rate_limiter: Option<KafkaRateLimits>,
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

        let producer_name = producer.context().producer_name();

        metric!(
            histogram(KafkaHistograms::KafkaMessageSize) = payload.len() as u64,
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
        });

        producer.send(record).map_err(|(error, _message)| {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                tags.variant = variant,
                tags.topic = topic_name,
                "error sending kafka message",
            );
            metric!(
                counter(KafkaCounters::ProducerEnqueueError) += 1,
                variant = variant,
                topic = topic_name,
                producer_name = producer_name
            );
            ClientError::SendFailed(error)
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
            .field("producers", &"<ThreadedProducers>")
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
    /// Returns the [`KafkaClientBuilder`]
    pub fn builder() -> KafkaClientBuilder {
        KafkaClientBuilder::default()
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
    pub fn send(
        &self,
        topic: KafkaTopic,
        key: Option<Key>,
        headers: Option<&BTreeMap<String, String>>,
        variant: &str,
        payload: &[u8],
    ) -> Result<&str, ClientError> {
        let producer = self.producers.get(&topic).ok_or_else(|| {
            relay_log::error!(
                "attempted to send message to {topic:?} using an unconfigured kafka producer",
            );
            ClientError::InvalidTopicName
        })?;

        producer.send(key, headers, variant, payload)
    }
}

/// Helper structure responsible for building the actual [`KafkaClient`].
#[derive(Default)]
pub struct KafkaClientBuilder {
    reused_producers: BTreeMap<Option<String>, Arc<ThreadedProducer>>,
    producers: HashMap<KafkaTopic, Producer>,
}

impl KafkaClientBuilder {
    /// Creates an empty KafkaClientBuilder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds topic configuration to the current [`KafkaClientBuilder`], which in return assigns
    /// dedicates producer to the topic which can will be used to send the messages.
    ///
    /// # Errors
    /// Returns [`ClientError::InvalidConfig`] error if the provided configuration is wrong and
    /// the producer could not be created.
    pub fn add_kafka_topic_config(
        mut self,
        topic: KafkaTopic,
        topic_config: &KafkaTopicConfig<'_>,
        validate_topic: bool,
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

            let config_name = config_name.map(str::to_string);

            // Get or create producer for this broker config
            let threaded_producer = if let Some(producer) = self.reused_producers.get(&config_name)
            {
                Arc::clone(producer)
            } else {
                let mut client_config = ClientConfig::new();
                for config_p in *config_params {
                    client_config.set(config_p.name.as_str(), config_p.value.as_str());
                }

                // Extract producer name from client.id, fallback to config name, then "unknown"
                let producer_name = config_params
                    .iter()
                    .find(|p| p.name == "client.id")
                    .map(|p| p.value.clone())
                    .or_else(|| config_name.clone())
                    .unwrap_or_else(|| "unknown".to_owned());

                let producer = Arc::new(
                    client_config
                        .create_with_context(Context::new(producer_name))
                        .map_err(ClientError::InvalidConfig)?,
                );

                self.reused_producers
                    .insert(config_name, Arc::clone(&producer));

                producer
            };

            topic_producers.producers.push(TopicProducer {
                topic_name: topic_name.clone(),
                producer: threaded_producer,
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
