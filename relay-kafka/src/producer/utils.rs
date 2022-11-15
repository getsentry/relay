use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::ClientContext;

use relay_log::LogError;
use relay_statsd::metric;

use crate::statsd::KafkaCounters;

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
