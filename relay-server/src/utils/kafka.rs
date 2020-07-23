use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::ClientContext;

pub struct CaptureErrorContext;

impl ClientContext for CaptureErrorContext {}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        if let Err((e, _message)) = result {
            log::error!("producer error: {}", e);

            // TODO send a metric
            // metric!(counter(RelayCounters::EventProtocol) += 1);
        }
    }
}

pub type ThreadedProducer = rdkafka::producer::ThreadedProducer<CaptureErrorContext>;
