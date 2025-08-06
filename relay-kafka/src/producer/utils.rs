use std::error::Error;

use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::{ClientContext, Message};
use relay_statsd::metric;

use crate::statsd::{KafkaCounters, KafkaGauges};

/// A thin wrapper around [`OwnedHeaders`].
///
/// Unlike [`OwnedHeaders`], this will not allocate on creation.
/// Allocations are tuned for the use-case in a [`super::Producer`].
pub struct KafkaHeaders(Option<OwnedHeaders>);

impl KafkaHeaders {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn insert<V>(&mut self, header: Header<'_, &V>)
    where
        V: ToBytes + ?Sized,
    {
        self.extend(Some(header));
    }

    pub fn into_inner(self) -> Option<OwnedHeaders> {
        self.0
    }
}

impl<'a, 'b, V> Extend<Header<'a, &'b V>> for KafkaHeaders
where
    V: ToBytes + ?Sized,
{
    fn extend<T: IntoIterator<Item = Header<'a, &'b V>>>(&mut self, iter: T) {
        let mut iter = iter.into_iter();

        // Probe if the iterator is empty, if it is empty, no need to do anything.
        let Some(first) = iter.next() else {
            return;
        };

        let mut headers = self.0.take().unwrap_or_else(|| {
            // Get a size hint from the iterator, +2 for the already removed
            // first element and reserving space for 1 extra header which is conditionally
            // added by the `Producer` in this crate.
            //
            // This means we might allocate a little bit too much, but we never have to resize
            // and allocate a second time, a good trade-off.
            let size = iter.size_hint().0 + 2;
            OwnedHeaders::new_with_capacity(size)
        });
        headers = headers.insert(first);
        for remaining in iter {
            headers = headers.insert(remaining);
        }

        self.0 = Some(headers);
    }
}

impl<'a, 'b, V> FromIterator<Header<'a, &'b V>> for KafkaHeaders
where
    V: ToBytes + ?Sized,
{
    fn from_iter<I: IntoIterator<Item = Header<'a, &'b V>>>(iter: I) -> Self {
        let mut c = Self::new();
        c.extend(iter);
        c
    }
}

/// Kafka client and producer context that logs statistics and producer errors.
#[derive(Debug)]
pub struct Context {
    /// Producer name for deployment identification
    producer_name: String,
}

impl Context {
    pub fn new(producer_name: String) -> Self {
        Self { producer_name }
    }
}

impl ClientContext for Context {
    /// Report client statistics as statsd metrics.
    ///
    /// This method is only called if `statistics.interval.ms` is configured.
    fn stats(&self, statistics: rdkafka::Statistics) {
        let producer_name = &self.producer_name;

        relay_statsd::metric!(
            gauge(KafkaGauges::MessageCount) = statistics.msg_cnt,
            producer_name = producer_name
        );
        relay_statsd::metric!(
            gauge(KafkaGauges::MessageCountMax) = statistics.msg_max,
            producer_name = producer_name
        );
        relay_statsd::metric!(
            gauge(KafkaGauges::MessageSize) = statistics.msg_size,
            producer_name = producer_name
        );
        relay_statsd::metric!(
            gauge(KafkaGauges::MessageSizeMax) = statistics.msg_size_max,
            producer_name = producer_name
        );

        for (_, broker) in statistics.brokers {
            relay_statsd::metric!(
                gauge(KafkaGauges::OutboundBufferRequests) = broker.outbuf_cnt as u64,
                broker_name = &broker.name,
                producer_name = producer_name
            );
            relay_statsd::metric!(
                gauge(KafkaGauges::OutboundBufferMessages) = broker.outbuf_msg_cnt as u64,
                broker_name = &broker.name,
                producer_name = producer_name
            );
            if let Some(connects) = broker.connects {
                relay_statsd::metric!(
                    gauge(KafkaGauges::Connects) = connects as u64,
                    broker_name = &broker.name,
                    producer_name = producer_name
                );
            }
            if let Some(disconnects) = broker.disconnects {
                relay_statsd::metric!(
                    gauge(KafkaGauges::Disconnects) = disconnects as u64,
                    broker_name = &broker.name,
                    producer_name = producer_name
                );
            }
            if let Some(int_latency) = broker.int_latency {
                relay_statsd::metric!(
                    gauge(KafkaGauges::ProducerQueueLatency) = int_latency.max as u64,
                    broker_name = &broker.name,
                    producer_name = producer_name
                );
            }
            if let Some(outbuf_latency) = broker.outbuf_latency {
                relay_statsd::metric!(
                    gauge(KafkaGauges::RequestQueueLatency) = outbuf_latency.max as u64,
                    broker_name = &broker.name,
                    producer_name = producer_name
                );
            }
            if let Some(rtt) = broker.rtt {
                relay_statsd::metric!(
                    gauge(KafkaGauges::BrokerRtt) = rtt.avg as u64,
                    broker_name = &broker.name,
                    producer_name = producer_name
                );
            }
            relay_statsd::metric!(
                gauge(KafkaGauges::BrokerTx) = broker.tx as u64,
                broker_name = &broker.name,
                producer_name = producer_name
            );
            relay_statsd::metric!(
                gauge(KafkaGauges::BrokerTxMsgs) = broker.txmsgs as u64,
                broker_name = &broker.name,
                producer_name = producer_name
            );
            relay_statsd::metric!(
                gauge(KafkaGauges::BrokerTxBytes) = broker.txbytes as u64,
                broker_name = &broker.name,
                producer_name = producer_name
            );
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
                topic = message.topic(),
                producer_name = &self.producer_name
            );
        }
    }
}

/// The wrapper type around the kafka [`rdkafka::producer::ThreadedProducer`] with our own
/// [`Context`].
pub type ThreadedProducer = rdkafka::producer::ThreadedProducer<Context>;
