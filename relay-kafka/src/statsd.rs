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
    ///  - `topic`: The Kafka topic being produced to.
    ProcessingProduceError,

    /// Number of messages that failed to be enqueued in the Kafka producer's memory buffer.
    ///
    /// These errors include, for example, _"UnknownTopic"_ errors when attempting to send a
    /// message a topic that does not exist.
    ///
    /// This metric is tagged with:
    /// - `topic`: The Kafka topic being produced to.
    /// - `variant`: The Kafka message variant.
    ProducerEnqueueError,
}

impl CounterMetric for KafkaCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::ProcessingProduceError => "processing.produce.error",
            Self::ProducerEnqueueError => "producer.enqueue.error",
        }
    }
}

pub enum KafkaHistograms {
    /// Size of emitted kafka message in bytes, tagged by message type.
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
    InFlightCount,

    /// The current number of messages in producer queues.
    MessageCount,

    /// The maximum number of messages allowed in the producer queues.
    MessageCountMax,

    /// The current total size of messages in producer queues.
    MessageSize,

    /// The maximum total size of messages allowed in the producer queues.
    MessageSizeMax,

    /// The number of requests awaiting transmission to the broker.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    OutboundBufferRequests,

    /// The number of messages awaiting transmission to the broker.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    OutboundBufferMessages,

    /// The number of connection attempts, including successful and failed attempts, and name resolution failures.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    Connects,

    /// The number of disconnections, whether triggered by the broker, the network, the load balancer, or something else.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    Disconnects,

    /// Maximum internal producer queue latency, in microseconds.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    ProducerQueueLatency,

    /// Maximum internal request queue latency, in microseconds.
    ///
    /// This metric is tagged with:
    /// - `broker_name`: The broker hostname, port, and ID, in the form HOSTNAME:PORT/ID.
    RequestQueueLatency,
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
        }
    }
}
