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

use relay_statsd::{CounterMetric, DistributionMetric};

pub enum KafkaCounters {
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

    /// Number of messages successfully produced to Kafka brokers.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProcessingMessageProduced,
}

impl CounterMetric for KafkaCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::ProducerPartitionKeyRateLimit => "producer.partition_key.rate_limit",
            Self::ProcessingMessageProduced => "processing.event.produced",
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
