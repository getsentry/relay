//! This module contains the kafka producer related code.
//!
//! There are two different producers are supported in Relay right now:
//! - [`SingleProducer`] - which sends all the messages to the defined kafka [`KafkaTopic`]
//! - [`ShardedProducer`] - which expects to have at least one shard configured, and depending on
//! the shard number the different message will be sent to different topic using the configured
//! producer for the this exact shard.

#[cfg(debug_assertions)]
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::BaseRecord;
use rdkafka::ClientConfig;
use relay_statsd::metric;
use thiserror::Error;

use crate::config::{KafkaConfig, KafkaParams, KafkaTopic};
#[cfg(debug_assertions)]
use crate::producer::schemas::Validator;
use crate::statsd::KafkaHistograms;

mod utils;
use utils::{CaptureErrorContext, ThreadedProducer};

#[cfg(debug_assertions)]
mod schemas;

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
    #[error("invalid kafka shard")]
    InvalidShard,
}

/// Describes the type which can be sent using kafka producer provided by this crate.
pub trait Message {
    /// Returns the partitioning key for this kafka message determining.
    fn key(&self) -> [u8; 16];

    /// Returns the type of the message.
    fn variant(&self) -> &'static str;

    /// Return the list of headers to be provided when payload is sent to Kafka.
    fn headers(&self) -> Option<&BTreeMap<String, String>>;

    /// Serializes the message into its binary format.
    ///
    /// # Errors
    /// Returns the [`ClientError::InvalidMsgPack`] or [`ClientError::InvalidJson`] if the
    /// serialization failed.
    fn serialize(&self) -> Result<Vec<u8>, ClientError>;
}

/// Single kafka producer config with assigned topic.
struct SingleProducer {
    /// Kafka topic name.
    topic_name: String,
    /// Real kafka producer.
    producer: Arc<ThreadedProducer>,
}

impl fmt::Debug for SingleProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Single")
            .field("topic_name", &self.topic_name)
            .field("producer", &"<ThreadedProducer>")
            .finish()
    }
}

/// Sharded producer configuration.
struct ShardedProducer {
    /// The maximum number of shards for this producer.
    shards: u64,
    /// The actual Kafka producer assigned to the range of logical shards, where the `u64` in the map is
    /// the inclusive beginning of the range.
    producers: BTreeMap<u64, (String, Arc<ThreadedProducer>)>,
}

impl ShardedProducer {
    /// Returns topic name and the Kafka producer based on the provided sharding key.
    /// Returns error [`ClientError::InvalidShard`] if the shard range for the provided sharding
    /// key could not be found.
    ///
    /// # Errors
    /// Returns [`ClientError::InvalidShard`] error if the provided `sharding_key` could not be
    /// placed in any configured shard ranges.
    pub fn get_producer(
        &self,
        sharding_key: u64,
    ) -> Result<(&str, &ThreadedProducer), ClientError> {
        let shard = sharding_key % self.shards;
        let (topic_name, producer) = self
            .producers
            .iter()
            .take_while(|(k, _)| *k <= &shard)
            .last()
            .map(|(_, v)| v)
            .ok_or(ClientError::InvalidShard)?;

        Ok((topic_name, producer))
    }
}

impl fmt::Debug for ShardedProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let producers = &self
            .producers
            .iter()
            .map(|(shard, (topic, _))| (shard, topic))
            .collect::<BTreeMap<_, _>>();
        f.debug_struct("ShardedProducer")
            .field("shards", &self.shards)
            .field("producers", producers)
            .finish()
    }
}

/// Keeps all the configured kafka producers and responsible for the routing of the messages.
#[derive(Debug)]
pub struct KafkaClient {
    producers: HashMap<KafkaTopic, Producer>,
    #[cfg(debug_assertions)]
    schema_validator: RefCell<schemas::Validator>,
}

impl KafkaClient {
    /// Returns the [`KafkaClientBuilder`]
    pub fn builder() -> KafkaClientBuilder {
        KafkaClientBuilder::default()
    }

    /// Sends message to the provided kafka topic.
    pub fn send_message(
        &self,
        topic: KafkaTopic,
        organization_id: u64,
        message: &impl Message,
    ) -> Result<(), ClientError> {
        let serialized = message.serialize()?;
        #[cfg(debug_assertions)]
        self.schema_validator
            .borrow_mut()
            .validate_message_schema(topic, &serialized)
            .map_err(ClientError::SchemaValidationFailed)?;
        let key = message.key();
        self.send(
            topic,
            organization_id,
            &key,
            message.headers(),
            message.variant(),
            &serialized,
        )
    }

    /// Sends the payload to the correct producer for the current topic.
    pub fn send(
        &self,
        topic: KafkaTopic,
        organization_id: u64,
        key: &[u8; 16],
        headers: Option<&BTreeMap<String, String>>,
        variant: &str,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        let producer = self.producers.get(&topic).ok_or_else(|| {
            relay_log::error!(
                "attempted to send message to {topic:?} using an unconfigured kafka producer",
            );
            ClientError::InvalidTopicName
        })?;
        producer.send(organization_id, key, headers, variant, payload)
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
        config: &KafkaConfig,
    ) -> Result<Self, ClientError> {
        let mut client_config = ClientConfig::new();
        match config {
            KafkaConfig::Single { params } => {
                let KafkaParams {
                    topic_name,
                    config_name,
                    params,
                } = params;

                let config_name = config_name.map(str::to_string);

                if let Some(producer) = self.reused_producers.get(&config_name) {
                    self.producers.insert(
                        topic,
                        Producer::Single(SingleProducer {
                            topic_name: (*topic_name).to_string(),
                            producer: Arc::clone(producer),
                        }),
                    );
                    return Ok(self);
                }

                for config_p in *params {
                    client_config.set(config_p.name.as_str(), config_p.value.as_str());
                }

                let producer = Arc::new(
                    client_config
                        .create_with_context(CaptureErrorContext)
                        .map_err(ClientError::InvalidConfig)?,
                );

                self.reused_producers
                    .insert(config_name, Arc::clone(&producer));
                self.producers.insert(
                    topic,
                    Producer::Single(SingleProducer {
                        topic_name: (*topic_name).to_string(),
                        producer,
                    }),
                );
                Ok(self)
            }
            KafkaConfig::Sharded { shards, configs } => {
                let mut producers = BTreeMap::new();
                for (shard, kafka_params) in configs {
                    let config_name = kafka_params.config_name.map(str::to_string);
                    if let Some(producer) = self.reused_producers.get(&config_name) {
                        let cached_producer = Arc::clone(producer);
                        producers.insert(
                            *shard,
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
                            .map_err(ClientError::InvalidConfig)?,
                    );
                    self.reused_producers
                        .insert(config_name, Arc::clone(&producer));
                    producers.insert(*shard, (kafka_params.topic_name.to_string(), producer));
                }
                self.producers.insert(
                    topic,
                    Producer::Sharded(ShardedProducer {
                        shards: *shards,
                        producers,
                    }),
                );
                Ok(self)
            }
        }
    }

    /// Consumes self and returns the built [`KafkaClient`].
    pub fn build(self) -> KafkaClient {
        KafkaClient {
            producers: self.producers,
            #[cfg(debug_assertions)]
            schema_validator: Validator::default().into(),
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

/// This object contains the Kafka producer variants for single and sharded configurations.
#[derive(Debug)]
enum Producer {
    /// Configuration variant for the single kafka producer.
    Single(SingleProducer),
    /// Configuration variant for sharded kafka producer, when one topic has different producers
    /// dedicated to the range of the shards.
    Sharded(ShardedProducer),
}

impl Producer {
    /// Sends the payload to the correct producer for the current topic.
    fn send(
        &self,
        organization_id: u64,
        key: &[u8; 16],
        headers: Option<&BTreeMap<String, String>>,
        variant: &str,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        metric!(
            histogram(KafkaHistograms::KafkaMessageSize) = payload.len() as u64,
            variant = variant
        );
        let (topic_name, producer) = match self {
            Self::Single(SingleProducer {
                topic_name,
                producer,
            }) => (topic_name.as_str(), producer.as_ref()),

            Self::Sharded(sharded) => sharded.get_producer(organization_id)?,
        };
        let mut record = BaseRecord::to(topic_name).key(key).payload(payload);

        // Make sure to set the headers if provided.
        if let Some(headers) = headers {
            let mut kafka_headers = OwnedHeaders::new();
            for (key, value) in headers {
                kafka_headers = kafka_headers.insert(Header {
                    key,
                    value: Some(value),
                });
            }
            record = record.headers(kafka_headers);
        }

        producer.send(record).map_err(|(error, _message)| {
            relay_log::error!(
                error = &error as &dyn std::error::Error,
                tags.variant = variant,
                "error sending kafka message"
            );
            ClientError::SendFailed(error)
        })
    }
}
