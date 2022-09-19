use std::{collections::BTreeMap, sync::Arc};

use rdkafka::{producer::BaseRecord, ClientConfig};

use relay_statsd::metric;

use crate::{
    config::{KafkaConfig, KafkaParams},
    utils::{CaptureErrorContext, KafkaHistograms, ThreadedProducer},
    Message, ProducerError,
};

/// Temporary map used to deduplicate kafka producers
type ReusedProducersMap<'a> = BTreeMap<Option<&'a str>, Arc<ThreadedProducer>>;

/// This object containes the Kafka producer variants for single and sharded configurations.
pub enum Producer {
    Single {
        topic_name: String,
        producer: Arc<ThreadedProducer>,
    },
    Sharded(ShardedProducer),
}

impl Producer {
    /// Creates the kafka [`Producer`] based on the provided configuration.
    ///
    /// # Errors
    /// Returns [`ProducerError::InvalidConfig`] error if the provided configuration is wrong and
    /// the producer could not be created.
    pub fn create<'a>(
        config: &'a KafkaConfig,
        reused_producers: &mut ReusedProducersMap<'a>,
    ) -> Result<Self, ProducerError> {
        let mut client_config = ClientConfig::new();
        match config {
            KafkaConfig::Single(KafkaParams {
                topic_name,
                config_name,
                params,
            }) => {
                if let Some(producer) = reused_producers.get(config_name) {
                    return Ok(Producer::Single {
                        topic_name: (*topic_name).to_string(),
                        producer: Arc::clone(producer),
                    });
                }

                for config_p in *params {
                    client_config.set(config_p.name.as_str(), config_p.value.as_str());
                }

                let producer = Arc::new(
                    client_config
                        .create_with_context(CaptureErrorContext)
                        .map_err(ProducerError::InvalidConfig)?,
                );

                reused_producers.insert(*config_name, Arc::clone(&producer));
                Ok(Self::Single {
                    topic_name: (*topic_name).to_string(),
                    producer,
                })
            }
            KafkaConfig::Sharded { shards, configs } => {
                let mut producers = BTreeMap::new();
                for (shard, kafka_params) in configs {
                    if let Some(producer) = reused_producers.get(&kafka_params.config_name) {
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
                            .map_err(ProducerError::InvalidConfig)?,
                    );
                    reused_producers.insert(kafka_params.config_name, Arc::clone(&producer));
                    producers.insert(*shard, (kafka_params.topic_name.to_string(), producer));
                }
                Ok(Self::Sharded(ShardedProducer {
                    shards: *shards,
                    producers,
                }))
            }
        }
    }

    /// Sends the message to Kafka using correct producer for the current topic.
    pub fn send_message(
        &self,
        organization_id: u64,
        message: &impl Message,
    ) -> Result<(), ProducerError> {
        let serialized = message.serialize()?;
        metric!(
            histogram(KafkaHistograms::KafkaMessageSize) = serialized.len() as u64,
            variant = message.variant()
        );
        let key = message.key();
        self.send(organization_id, &key, message.variant(), &serialized)
    }

    /// Sends the payload to the correct producer for the current topic.
    pub fn send(
        &self,
        organization_id: u64,
        key: &[u8; 16],
        variant: &str,
        payload: &[u8],
    ) -> Result<(), ProducerError> {
        let (topic_name, producer) = match self {
            Self::Single {
                topic_name,
                producer,
            } => (topic_name.as_str(), producer.as_ref()),

            Self::Sharded(sharded) => sharded.get_producer(organization_id)?,
        };
        let record = BaseRecord::to(topic_name).key(key).payload(payload);

        producer.send(record).map_err(|(kafka_error, _message)| {
            relay_log::with_scope(
                |scope| scope.set_tag("variant", variant),
                || relay_log::error!("error sending kafka message: {}", kafka_error),
            );
            ProducerError::SendFailed(kafka_error)
        })
    }
}

/// Sharded producer configuration.
pub struct ShardedProducer {
    /// The maximum number of shards for this producer.
    shards: u64,
    /// The actual Kafka producer assigned to the range of logical shards, where the `u64` in the map is
    /// the inclusive beginning of the range.
    producers: BTreeMap<u64, (String, Arc<ThreadedProducer>)>,
}

impl ShardedProducer {
    /// Returns topic name and the Kafka producer based on the provided sharding key.
    /// Returns error [`ProducerError::InvalidShard`] if the shard range for the provided sharding
    /// key could not be found.
    ///
    /// # Errors
    /// Returns [`ProducerError::InvalidShard`] error if the provided `sharding_key` could not be
    /// placed in any configured shard ranges.
    pub fn get_producer(
        &self,
        sharding_key: u64,
    ) -> Result<(&str, &ThreadedProducer), ProducerError> {
        let shard = sharding_key % self.shards;
        let (topic_name, producer) = self
            .producers
            .iter()
            .take_while(|(k, _)| *k <= &shard)
            .last()
            .map(|(_, v)| v)
            .ok_or(ProducerError::InvalidShard)?;

        Ok((topic_name, producer))
    }
}
