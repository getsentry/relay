use std::error::Error;

use rdkafka::message::{Header, Headers as _, OwnedHeaders, ToBytes};
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::{ClientContext, Message};
use relay_statsd::metric;
use sentry_arroyo::backends::kafka::producer::ProducerContext as ArroyoProducerContext;

use crate::statsd::KafkaCounters;

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
///
/// Cloned once per client a producer builds, so that the
/// callbacks keep reporting across a config reload.
#[derive(Clone)]
pub struct Context {
    /// Producer name for deployment identification
    producer_name: String,
    /// Delegate statistics to Arroyo.
    arroyo_statistics: ArroyoProducerContext,
}

impl Context {
    pub fn new(producer_name: String) -> Self {
        Self {
            arroyo_statistics: ArroyoProducerContext::new(producer_name.clone()),
            producer_name,
        }
    }

    pub fn producer_name(&self) -> &str {
        &self.producer_name
    }
}

impl ClientContext for Context {
    /// Report client statistics as statsd metrics.
    ///
    /// This method is only called if `statistics.interval.ms` is configured.
    fn stats(&self, statistics: rdkafka::Statistics) {
        self.arroyo_statistics.stats(statistics);
    }
}

impl ProducerContext for Context {
    type DeliveryOpaque = ();

    /// This method is called after attempting to send a message to Kafka.
    /// It's called asynchronously for every message, so we want to handle errors explicitly here.
    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        // TODO: any `Accepted` outcomes (e.g. spans) should be logged here instead of on the caller side,
        // such that we do not over-report in the error case.

        match result {
            Ok(message) => {
                metric!(
                    counter(KafkaCounters::ProduceStatusSuccess) += 1,
                    topic = message.topic(),
                    producer_name = self.producer_name.as_str(),
                );
                metric!(
                    counter(KafkaCounters::ProcessingMessageProduced) += 1,
                    topic = message.topic(),
                    producer_name = self.producer_name.as_str(),
                );
                metric!(
                    counter(KafkaCounters::ProducedBytes) += message_size(message),
                    topic = message.topic(),
                    producer_name = self.producer_name.as_str(),
                );
            }
            Err((error, message)) => {
                relay_log::error!(
                    error = error as &dyn Error,
                    payload_len = message.payload_len(),
                    tags.topic = message.topic(),
                    "failed to produce message to Kafka (delivery callback)",
                );

                metric!(
                    counter(KafkaCounters::ProduceStatusError) += 1,
                    topic = message.topic(),
                    producer_name = self.producer_name.as_str(),
                );
            }
        }
    }
}

/// Uncompressed size of a message: key, payload, and header keys and values.
fn message_size(message: &impl Message) -> u64 {
    let headers_size: usize = message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| header.key.len() + header.value.map_or(0, <[u8]>::len))
                .sum()
        })
        .unwrap_or(0);
    let key_size = message.key().map_or(0, <[u8]>::len);
    let payload_size = message.payload().map_or(0, <[u8]>::len);
    (key_size + payload_size + headers_size) as u64
}

#[cfg(test)]
mod tests {
    use rdkafka::ClientContext;
    use rdkafka::statistics::{Broker, Statistics};
    use relay_statsd::with_capturing_test_client;

    use rdkafka::Timestamp;
    use rdkafka::message::{Header, OwnedHeaders, OwnedMessage};

    use super::{Context, message_size};

    #[test]
    fn test_statistics_backend() {
        let context = Context::new("test-producer".to_owned());
        let statistics = Statistics {
            msg_cnt: 42,
            brokers: [(
                "broker-id".to_owned(),
                Broker {
                    name: "broker-name".to_owned(),
                    state: "UP".to_owned(),
                    outbuf_cnt: 7,
                    ..Default::default()
                },
            )]
            .into(),
            ..Default::default()
        };

        let metrics = with_capturing_test_client(|| context.stats(statistics));
        let prefix = "arroyo.producer.librdkafka.";
        let broker_metric = "broker_outbuf_requests";

        assert!(metrics.contains(&format!(
            "{prefix}message_count:42|g|#producer_name:test-producer"
        )));
        assert!(metrics.contains(&format!("{prefix}{broker_metric}:7|g|#")));
        assert!(metrics.iter().all(|metric| metric.starts_with(prefix)));
    }

    #[test]
    fn test_message_size() {
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "version",
                value: Some("1"),
            })
            .insert(Header {
                key: "empty",
                value: None::<&str>,
            });
        let message = OwnedMessage::new(
            Some(b"payload".to_vec()),
            Some(42_u128.to_be_bytes().to_vec()),
            "test".to_owned(),
            Timestamp::NotAvailable,
            0,
            0,
            Some(headers),
        );

        // key 16 + payload 7 + headers (7 + 1) + (5 + 0)
        assert_eq!(message_size(&message), 36);

        let empty = OwnedMessage::new(
            None,
            None,
            "test".to_owned(),
            Timestamp::NotAvailable,
            0,
            0,
            None,
        );
        assert_eq!(message_size(&empty), 0);
    }
}
