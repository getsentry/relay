use std::error::Error;

use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::{ClientContext, Message};
use relay_statsd::metric;

use crate::statsd::{KafkaCounters, KafkaGauges};

/// Kafka client and producer context that logs statistics and producer errors.
#[derive(Debug)]
pub struct Context;

impl ClientContext for Context {
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
            if let Some(int_latency) = broker.int_latency {
                relay_statsd::metric!(
                    gauge(KafkaGauges::InternalLatency) = int_latency.max as u64,
                    broker_name = &broker.name
                );
            }
            if let Some(outbuf_latency) = broker.outbuf_latency {
                relay_statsd::metric!(
                    gauge(KafkaGauges::OutboundBufferLatency) = outbuf_latency.max as u64,
                    broker_name = &broker.name
                );
            }
        }
    }
}

impl ProducerContext for Context {
    type DeliveryOpaque = ();

    /// This method is called after attempting to send a message to Kafka.
    /// It's called asynchronously for every message, so we want to handle errors explicitly here.
    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        // TODO: any `Accepted` outcomes (e.g. spans) should be logged here instead of on the caller side,
        // such that we do not over-report in the error case.

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
/// [`Context`].
pub type ThreadedProducer = rdkafka::producer::ThreadedProducer<Context>;
