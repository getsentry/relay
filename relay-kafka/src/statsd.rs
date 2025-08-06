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

use relay_statsd::{CounterMetric, GaugeMetric, HistogramMetric};

pub enum KafkaCounters {
    /// Number of producer errors occurred after an envelope was already enqueued for sending to
    /// Kafka.
    ///
    /// These errors include, for example, _"MessageTooLarge"_ errors when the broker does not
    /// accept the requests over a certain size, which is usually due to invalid or inconsistent
    /// broker/producer configurations.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProcessingProduceError,

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
}

impl CounterMetric for KafkaCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::ProcessingProduceError => "processing.produce.error",
            Self::ProducerEnqueueError => "producer.enqueue.error",
            Self::ProducerPartitionKeyRateLimit => "producer.partition_key.rate_limit",
        }
    }
}

pub enum KafkaHistograms {
    /// Size of emitted kafka message in bytes.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `variant`: The Kafka message variant.
    /// - `producer_name`: The configured producer name/deployment identifier.
    KafkaMessageSize,
}

impl HistogramMetric for KafkaHistograms {
    fn name(&self) -> &'static str {
        match self {
            Self::KafkaMessageSize => "kafka.message_size",
        }
    }
}
/// Gauge metrics for the Kafka producer.
///
/// Most of these metrics are taken from the [`rdkafka::statistics`] module.
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

    /// The current number of messages in producer queues.
    ///
    /// This metric is tagged with:
    /// - `producer_name`: The configured producer name/deployment identifier.
    MessageCount,

    /// The maximum number of messages allowed in the producer queues.
    ///
    /// This metric is tagged with:
    /// - `producer_name`: The configured producer name/deployment identifier.
    MessageCountMax,

    /// The current total size of messages in producer queues.
    ///
    /// This metric is tagged with:
    /// - `producer_name`: The configured producer name/deployment identifier.
    MessageSize,

    /// The maximum total size of messages allowed in the producer queues.
    ///
    /// This metric is tagged with:
    /// - `producer_name`: The configured producer name/deployment identifier.
    MessageSizeMax,

    /// The number of requests awaiting transmission to the broker.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    OutboundBufferRequests,

    /// The number of messages awaiting transmission to the broker.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    OutboundBufferMessages,

    /// The number of connection attempts, including successful and failed attempts, and name resolution failures.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    Connects,

    /// The number of disconnections, whether triggered by the broker, the network, the load balancer, or something else.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    Disconnects,

    /// Maximum internal producer queue latency, in microseconds.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    ProducerQueueLatency,

    /// Maximum internal request queue latency, in microseconds.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    RequestQueueLatency,

    /// Average round-trip time to the broker, in microseconds.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    BrokerRtt,

    /// Total number of requests sent to the broker.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    BrokerTx,

    /// Total number of messages sent to the broker.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    BrokerTxMsgs,

    /// Total number of bytes sent to the broker.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    /// - `producer_name`: The configured producer name/deployment identifier.
    BrokerTxBytes,
}

impl GaugeMetric for KafkaGauges {
    fn name(&self) -> &'static str {
        match self {
            KafkaGauges::InFlightCount => "kafka.in_flight_count",
            KafkaGauges::MessageCount => "kafka.stats.message_count",
            KafkaGauges::MessageCountMax => "kafka.stats.message_count_max",
            KafkaGauges::MessageSize => "kafka.stats.message_size",
            KafkaGauges::MessageSizeMax => "kafka.stats.message_size_max",
            KafkaGauges::OutboundBufferRequests => "kafka.stats.broker.outbuf.requests",
            KafkaGauges::OutboundBufferMessages => "kafka.stats.broker.outbuf.messages",
            KafkaGauges::Connects => "kafka.stats.broker.connects",
            KafkaGauges::Disconnects => "kafka.stats.broker.disconnects",
            KafkaGauges::ProducerQueueLatency => "kafka.stats.broker.int_latency",
            KafkaGauges::RequestQueueLatency => "kafka.stats.broker.outbuf_latency",
            KafkaGauges::BrokerRtt => "kafka.stats.broker.rtt",
            KafkaGauges::BrokerTx => "kafka.stats.broker.tx",
            KafkaGauges::BrokerTxMsgs => "kafka.stats.broker.txmsgs",
            KafkaGauges::BrokerTxBytes => "kafka.stats.broker.txbytes",
        }
    }
}
