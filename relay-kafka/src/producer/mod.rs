//! This module contains the Kafka producer related code.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::message::Header;
use rdkafka::producer::{BaseRecord, Producer as _};
use relay_statsd::metric;
use thiserror::Error;

use crate::config::{KafkaParams, KafkaTopic};
use crate::debounced::Debounced;
use crate::limits::KafkaRateLimits;
use crate::producer::utils::KafkaHeaders;
use crate::statsd::{KafkaCounters, KafkaGauges, KafkaHistograms};

mod utils;
use utils::{Context, ThreadedProducer};

#[cfg(feature = "schemas")]
mod schemas;

const REPORT_FREQUENCY_SECS: u64 = 1;
const KAFKA_FETCH_METADATA_TIMEOUT: Duration = Duration::from_secs(30);

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
    #[cfg(feature = "schemas")]
    #[error("failed to run schema validation on message")]
    SchemaValidationFailed(#[source] schemas::SchemaError),

    /// Configuration is wrong and it cannot be used to identify the number of a shard.
    #[error("invalid kafka shard")]
    InvalidShard,

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
    fn key(&self) -> Option<[u8; 16]>;

    /// Returns the type of the message.
    fn variant(&self) -> &'static str;

    /// Return the list of headers to be provided when payload is sent to Kafka.
    fn headers(&self) -> Option<&BTreeMap<String, String>>;

    /// Serializes the message into its binary format.
    ///
    /// # Errors
    /// Returns the [`ClientError::InvalidMsgPack`] or [`ClientError::InvalidJson`] if the
    /// serialization failed.
    fn serialize(&self) -> Result<Cow<'_, [u8]>, ClientError>;
}

/// Single kafka producer config with assigned topic.
struct Producer {
    /// Kafka topic name.
    topic_name: String,
    /// Real kafka producer.
    producer: Arc<ThreadedProducer>,
    /// Debouncer for metrics.
    metrics: Debounced,
    /// Optional rate limits to apply.
    rate_limiter: Option<KafkaRateLimits>,
}

impl Producer {
    fn new(
        topic_name: String,
        producer: Arc<ThreadedProducer>,
        rate_limiter: Option<KafkaRateLimits>,
    ) -> Self {
        Self {
            topic_name,
            producer,
            metrics: Debounced::new(REPORT_FREQUENCY_SECS),
            rate_limiter,
        }
    }

    /// Validates the topic by fetching the metadata of the topic directly from Kafka.
    fn validate_topic(&self) -> Result<(), ClientError> {
        let client = self.producer.client();
        let metadata = client
            .fetch_metadata(Some(&self.topic_name), KAFKA_FETCH_METADATA_TIMEOUT)
            .map_err(ClientError::MetadataFetchError)?;

        for topic in metadata.topics() {
            if let Some(error) = topic.error() {
                return Err(ClientError::TopicError(topic.name().to_string(), error));
            }
        }

        Ok(())
    }
}

impl fmt::Debug for Producer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer")
            .field("topic_name", &self.topic_name)
            .field("producer", &"<ThreadedProducer>")
            .finish_non_exhaustive()
    }
}

/// Keeps all the configured kafka producers and responsible for the routing of the messages.
#[derive(Debug)]
pub struct KafkaClient {
    producers: HashMap<KafkaTopic, Producer>,
    #[cfg(feature = "schemas")]
    schema_validator: schemas::Validator,
}

impl KafkaClient {
    /// Returns the [`KafkaClientBuilder`]
    pub fn builder() -> KafkaClientBuilder {
        KafkaClientBuilder::default()
    }

    /// Sends message to the provided kafka topic.
    ///
    /// Returns the name of the kafka topic to which the message was produced.
    pub fn send_message(
        &self,
        topic: KafkaTopic,
        message: &impl Message,
    ) -> Result<&str, ClientError> {
        let serialized = message.serialize()?;
        #[cfg(feature = "schemas")]
        self.schema_validator
            .validate_message_schema(topic, &serialized)
            .map_err(ClientError::SchemaValidationFailed)?;

        self.send(
            topic,
            message.key(),
            message.headers(),
            message.variant(),
            &serialized,
        )
    }

    /// Sends the payload to the correct producer for the current topic.
    ///
    /// Returns the name of the kafka topic to which the message was produced.
    pub fn send(
        &self,
        topic: KafkaTopic,
        key: Option<[u8; 16]>,
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
        params: &KafkaParams,
        validate_topic: bool,
    ) -> Result<Self, ClientError> {
        let mut client_config = ClientConfig::new();

        let KafkaParams {
            topic_name,
            config_name,
            params,
            key_rate_limit,
        } = params;

        let rate_limiter = key_rate_limit.map(|limit| {
            KafkaRateLimits::new(
                limit.limit_per_window,
                Duration::from_secs(limit.window_secs),
            )
        });

        let config_name = config_name.map(str::to_string);

        if let Some(producer) = self.reused_producers.get(&config_name) {
            let producer = Producer::new(
                (*topic_name).to_string(),
                Arc::clone(producer),
                rate_limiter,
            );
            if validate_topic {
                producer.validate_topic()?;
            }
            self.producers.insert(topic, producer);
            return Ok(self);
        }

        for config_p in *params {
            client_config.set(config_p.name.as_str(), config_p.value.as_str());
        }

        let producer = Arc::new(
            client_config
                .create_with_context(Context)
                .map_err(ClientError::InvalidConfig)?,
        );

        self.reused_producers
            .insert(config_name, Arc::clone(&producer));

        let producer = Producer::new((*topic_name).to_string(), producer, rate_limiter);
        if validate_topic {
            producer.validate_topic()?;
        }
        self.producers.insert(topic, producer);

        Ok(self)
    }

    /// Consumes self and returns the built [`KafkaClient`].
    pub fn build(self) -> KafkaClient {
        KafkaClient {
            producers: self.producers,
            #[cfg(feature = "schemas")]
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

impl Producer {
    /// Sends the payload to the correct producer for the current topic.
    fn send(
        &self,
        key: Option<[u8; 16]>,
        headers: Option<&BTreeMap<String, String>>,
        variant: &str,
        payload: &[u8],
    ) -> Result<&str, ClientError> {
        let now = Instant::now();
        let topic_name = self.topic_name.as_str();

        metric!(
            histogram(KafkaHistograms::KafkaMessageSize) = payload.len() as u64,
            variant = variant,
            topic = topic_name,
        );

        let mut headers = headers
            .unwrap_or(&BTreeMap::new())
            .iter()
            .map(|(key, value)| Header {
                key,
                value: Some(value),
            })
            .collect::<KafkaHeaders>();

        let key = match (key, self.rate_limiter.as_ref()) {
            (Some(key), Some(limiter)) => {
                let is_limited = limiter.try_increment(now, key, 1) < 1;

                if is_limited {
                    metric!(
                        counter(KafkaCounters::ProducerPartitionKeyRateLimit) += 1,
                        variant = variant,
                        topic = topic_name,
                    );

                    headers.insert(Header {
                        key: "sentry-reshuffled",
                        value: Some("1"),
                    });

                    None
                } else {
                    Some(key)
                }
            }
            (key, _) => key,
        };

        let mut record = BaseRecord::to(topic_name).payload(payload);
        if let Some(headers) = headers.into_inner() {
            record = record.headers(headers);
        }
        if let Some(key) = key.as_ref() {
            record = record.key(key);
        }

        self.metrics.debounce(now, || {
            metric!(
                gauge(KafkaGauges::InFlightCount) = self.producer.in_flight_count() as u64,
                variant = variant,
                topic = topic_name
            );
        });

        self.producer.send(record).map_err(|(error, _message)| {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                tags.variant = variant,
                tags.topic = topic_name,
                "error sending kafka message",
            );
            metric!(
                counter(KafkaCounters::ProducerEnqueueError) += 1,
                variant = variant,
                topic = topic_name
            );
            ClientError::SendFailed(error)
        })?;

        Ok(topic_name)
    }
}
