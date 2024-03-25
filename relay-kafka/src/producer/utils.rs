use std::error::Error;

use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::{ClientContext, Message};
use relay_statsd::metric;

use crate::statsd::{KafkaCounters, KafkaGauges};

/// Kafka producer context that logs producer errors.
#[derive(Debug)]
pub struct CaptureErrorContext;

impl ClientContext for CaptureErrorContext {
    /// Report client statistics as statsd metrics.
    ///
    /// This method is only called if `statistics.interval.ms` is configured.
    fn stats(&self, statistics: rdkafka::Statistics) {
        relay_statsd::metric!(gauge(KafkaGauges::MessageCount) = statistics.msg_cnt);
        relay_statsd::metric!(gauge(KafkaGauges::MessageCountMax) = statistics.msg_max);
        relay_statsd::metric!(gauge(KafkaGauges::MessageSize) = statistics.msg_size);
        relay_statsd::metric!(gauge(KafkaGauges::MessageSizeMax) = statistics.msg_size_max);

        for broker in statistics.brokers.values() {
            relay_statsd::metric!(
                gauge(KafkaGauges::OutboundBufferRequests) = broker.outbuf_cnt as u64,
                broker_name = &broker.name
            );
            relay_statsd::metric!(
                gauge(KafkaGauges::OutboundBufferMessages) = broker.outbuf_msg_cnt as u64,
                broker_name = &broker.name
            );
            if let Some(connects) = broker.connects {
                relay_statsd::metric!(
                    gauge(KafkaGauges::Connects) = connects as u64,
                    broker_name = &broker.name
                );
            }
            if let Some(disconnects) = broker.disconnects {
                relay_statsd::metric!(
                    gauge(KafkaGauges::Disconnects) = disconnects as u64,
                    broker_name = &broker.name
                );
            }
        }
    }
}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();

    /// This method is called after attempting to send a message to Kafka.
    /// It's called asynchronously for every message, so we want to handle errors explicitly here.
    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        if let Err((error, message)) = result {
            relay_log::error!(
                error = error as &dyn Error,
                payload_len = message.payload_len(),
                tags.topic = message.topic(),
                "failed to produce message to Kafka (delivery callback)",
            );

            metric!(
                counter(KafkaCounters::ProcessingProduceError) += 1,
                topic = message.topic()
            );
        }
    }
}

/// The wrapper type around the kafka [`rdkafka::producer::ThreadedProducer`] with our own
/// [`CaptureErrorContext`] context.
pub type ThreadedProducer = rdkafka::producer::ThreadedProducer<CaptureErrorContext>;
