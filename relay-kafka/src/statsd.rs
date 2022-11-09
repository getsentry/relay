use relay_statsd::{CounterMetric, HistogramMetric};

pub enum KafkaCounters {
    /// Number of producer errors occurred after an envelope was already enqueued for sending to
    /// Kafka.
    ///
    /// These errors include, for example, _"MessageTooLarge"_ errors when the broker does not
    /// accept the requests over a certain size, which is usually due to invalid or inconsistent
    /// broker/producer configurations.
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
