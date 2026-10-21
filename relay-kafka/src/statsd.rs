//! Kafka producer metrics.
//!
//! ## Producer Name Tag
//!
//! Many metrics include a `producer_name` tag to identify the deployment or application instance
//! producing messages. This tag is derived using the following fallback strategy:
//!
//! 1. **`client.id`** - If explicitly configured in Kafka config parameters
//! 2. **Config name** - The name of the secondary Kafka config (e.g., "profiles_cluster", "cluster1")  
//! 3. **`"unknown"`** - Final fallback for the default Kafka configuration
//!
//! Example configurations:
//! ```yaml
//! # Explicit client.id (highest priority)
//! secondary_kafka_configs:
//!   profiles_cluster:
//!     - name: "client.id"
//!       value: "relay-profiles-prod"
//! # Results in: producer_name="relay-profiles-prod"
//!
//! # Config name fallback
//! secondary_kafka_configs:
//!   cluster1:
//!     - name: "bootstrap.servers"
//!       value: "kafka:9092"
//! # Results in: producer_name="cluster1"
//!
//! # Default config fallback
//! kafka:
//!   - name: "bootstrap.servers"
//!     value: "kafka:9092"
//! # Results in: producer_name="unknown"
//! ```

use relay_statsd::{CounterMetric, DistributionMetric, GaugeMetric};

pub enum KafkaCounters {
    /// Number of messages that failed to be enqueued in the Kafka producer's memory buffer.
    ///
    /// These errors include, for example, _"UnknownTopic"_ errors when attempting to send a
    /// message a topic that does not exist.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `variant`: The Kafka message variant.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProducerEnqueueError,

    /// Number of messages that were written to the wrong partition because of configured rate limits.
    ///
    /// Each topic in Relay can optionally be configured with a per-partition-key rate limit. This
    /// rate limit does not drop messages, but instead disables semantic partitioning. Everytime
    /// this happens for a message, this counter is incremented.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `variant`: The Kafka message variant.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProducerPartitionKeyRateLimit,

    /// Number of successful message produce operations.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProduceStatusSuccess,

    /// Number of messages successfully produced to Kafka brokers.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProcessingMessageProduced,

    /// Number of failed message produce operations.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProduceStatusError,

    /// Number of Kafka config blobs handed to a topic's reloading producer.
    ///
    /// Counts pushes that were accepted for rollout, which includes blobs that resolve to the
    /// config already running. It does not mean the client was replaced; watch
    /// [`KafkaGauges::ReloadGeneration`] for that.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ConfigReloadPushed,

    /// Number of Kafka config blobs a topic's reloading producer refused.
    ///
    /// Rejection leaves the running client in place. Sustained non-zero values mean this topic
    /// is no longer applying config changes.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ConfigReloadRejected,
}

impl CounterMetric for KafkaCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::ProducerEnqueueError => "producer.enqueue.error",
            Self::ProducerPartitionKeyRateLimit => "producer.partition_key.rate_limit",
            Self::ProduceStatusSuccess => "producer.produce_status.success",
            Self::ProcessingMessageProduced => "processing.event.produced",
            Self::ProduceStatusError => "producer.produce_status.error",
            Self::ConfigReloadPushed => "producer.config_reload.pushed",
            Self::ConfigReloadRejected => "producer.config_reload.rejected",
        }
    }
}

pub enum KafkaDistributions {
    /// Size of emitted kafka message in bytes.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `variant`: The Kafka message variant.
    /// - `producer_name`: The configured producer name/deployment identifier.
    KafkaMessageSize,
}

impl DistributionMetric for KafkaDistributions {
    fn name(&self) -> &'static str {
        match self {
            Self::KafkaMessageSize => "kafka.message_size",
        }
    }
}
/// Gauge metrics for the Kafka producer.
pub enum KafkaGauges {
    /// The number of messages waiting to be sent to, or acknowledged by, the broker.
    ///
    /// See <https://docs.confluent.io/platform/7.5/clients/librdkafka/html/rdkafka_8h.html#ad4b3b7659cf9a79d3353810d6b625bb7>.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `variant`: The Kafka message variant.
    /// - `producer_name`: The configured producer name/deployment identifier.
    InFlightCount,

    /// The number of times this topic's Kafka client has been replaced by a config reload.
    ///
    /// Always `0` for topics not using the reloading producer. Arroyo reports the same number
    /// under `arroyo.producer.config_reload_generation`; this one is tagged the way Relay tags
    /// the rest of its produce metrics, so it lines up with them.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ReloadGeneration,
}

impl GaugeMetric for KafkaGauges {
    fn name(&self) -> &'static str {
        match self {
            KafkaGauges::InFlightCount => "kafka.in_flight_count",
            KafkaGauges::ReloadGeneration => "kafka.reload_generation",
        }
    }
}
