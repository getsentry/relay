use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::ClientContext;

use relay_common::{metric, LogError};

use crate::metrics::RelayCounters;

pub struct CaptureErrorContext;

impl ClientContext for CaptureErrorContext {}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        if let Err((error, _message)) = result {
            log::error!("callback producer error: {}", LogError(error));

            metric!(counter(RelayCounters::ProcessingProduceError) += 1);
        }
    }
}

pub type ThreadedProducer = rdkafka::producer::ThreadedProducer<CaptureErrorContext>;
