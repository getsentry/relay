//! This module contains the kafka producer related code.
//!
//! There are two different producers that are supported in Relay right now:
//! - [`SingleProducer`] - which sends all the messages to the defined kafka [`KafkaTopic`],

use std::borrow::Cow;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, Producer as _};
use rdkafka::ClientConfig;
use relay_statsd::metric;
use thiserror::Error;

use crate::config::{KafkaConfig, KafkaParams, KafkaTopic};
use crate::statsd::{KafkaCounters, KafkaGauges, KafkaHistograms};

mod utils;
use utils::{Context, ThreadedProducer};

#[cfg(feature = "schemas")]
mod schemas;

const REPORT_FREQUENCY: Duration = Duration::from_secs(1);

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
    fn serialize(&self) -> Result<Cow<'_, [u8]>, ClientError>;
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

/// Keeps all the configured kafka producers and responsible for the routing of the messages.
#[derive(Debug)]
pub struct KafkaClient {
    producers: HashMap<KafkaTopic, Producer>,
    #[cfg(feature = "schemas")]
    schema_validator: std::cell::RefCell<schemas::Validator>,
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
            .borrow_mut()
            .validate_message_schema(topic, &serialized)
            .map_err(ClientError::SchemaValidationFailed)?;
        let key = message.key();
        self.send(
            topic,
            &key,
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
        key: &[u8; 16],
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
                        Producer::single((*topic_name).to_string(), Arc::clone(producer)),
                    );
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
                self.producers
                    .insert(topic, Producer::single((*topic_name).to_string(), producer));
                Ok(self)
            }
        }
    }

    /// Consumes self and returns the built [`KafkaClient`].
    pub fn build(self) -> KafkaClient {
        KafkaClient {
            producers: self.producers,
            #[cfg(feature = "schemas")]
            schema_validator: schemas::Validator::default().into(),
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

/// This object contains the Kafka producer variants for single.
#[derive(Debug)]
enum ProducerInner {
    /// Configuration variant for the single kafka producer.
    Single(SingleProducer),
}

#[derive(Debug)]
struct Producer {
    last_report: Cell<Instant>,
    inner: ProducerInner,
}

impl Producer {
    fn single(topic_name: String, producer: Arc<ThreadedProducer>) -> Self {
        Self {
            last_report: Instant::now().into(),
            inner: ProducerInner::Single(SingleProducer {
                topic_name,
                producer,
            }),
        }
    }

    /// Sends the payload to the correct producer for the current topic.
    fn send(
        &self,
        key: &[u8; 16],
        headers: Option<&BTreeMap<String, String>>,
        variant: &str,
        payload: &[u8],
    ) -> Result<&str, ClientError> {
        metric!(
            histogram(KafkaHistograms::KafkaMessageSize) = payload.len() as u64,
            variant = variant
        );
        let (topic_name, producer) = match &self.inner {
            ProducerInner::Single(SingleProducer {
                topic_name,
                producer,
            }) => (topic_name.as_str(), producer.as_ref()),
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

        if self.last_report.get().elapsed() > REPORT_FREQUENCY {
            self.last_report.replace(Instant::now());
            metric!(
                gauge(KafkaGauges::InFlightCount) = producer.in_flight_count() as u64,
                variant = variant,
                topic = topic_name
            );
        }

        producer
            .send(record)
            .map(|_| topic_name)
            .map_err(|(error, _message)| {
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
            })
    }
}
