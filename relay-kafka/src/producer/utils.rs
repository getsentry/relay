use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::ClientContext;

use relay_log::LogError;
use relay_statsd::{metric, CounterMetric, HistogramMetric};

/// Kafka producer context that logs producer errors.
#[derive(Debug)]
pub struct CaptureErrorContext;

impl ClientContext for CaptureErrorContext {}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();

    /// This method is called after attempting to send a message to Kafka.
    /// It's called asynchronously for every message, so we want to handle errors explicitly here.
    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        if let Err((error, _message)) = result {
            relay_log::error!(
                "failed to produce message to Kafka (delivery callback): {}",
                LogError(error)
            );

            metric!(counter(KafkaCounters::ProcessingProduceError) += 1);
        }
    }
}

/// The wrapper type around the kafka [`rdkafka::producer::ThreadedProducer`] with our own
/// [`CaptureErrorContext`] context.
pub type ThreadedProducer = rdkafka::producer::ThreadedProducer<CaptureErrorContext>;

enum KafkaCounters {
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

pub(crate) enum KafkaHistograms {
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
