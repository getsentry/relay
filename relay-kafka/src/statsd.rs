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
}

impl CounterMetric for KafkaCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::ProcessingProduceError => "processing.produce.error",
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

pub enum KafkaGauges {
    /// The number of messages waiting to be acknowledged.
    ///
    /// See <https://docs.confluent.io/platform/7.5/clients/librdkafka/html/rdkafka_8h.html#ad4b3b7659cf9a79d3353810d6b625bb7>.
    ///
    /// This metric is tagged with:
    /// - `topic`
    InFlightCount,
}

impl GaugeMetric for KafkaGauges {
    fn name(&self) -> &'static str {
        match self {
            KafkaGauges::InFlightCount => "kafka.in_flight_count",
        }
    }
}
