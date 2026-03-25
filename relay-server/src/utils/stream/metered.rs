use std::task::Poll;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};

use crate::statsd::RelayTimers;

/// A stream wrapper that counts the time spent polling / waiting for the next poll.
pub struct MeteredStream<S> {
    name: &'static str,
    inner: S,
    pending_since: Option<Instant>,
    last_item_consumed: Option<Instant>,
    producer_latency: Duration,
    consumer_latency: Duration,
}

impl<S> MeteredStream<S>
where
    S: Stream,
{
    /// Create a new metered stream from an existing stream.
    pub fn new(name: &'static str, inner: S) -> Self {
        Self {
            name,
            inner,
            pending_since: None,
            last_item_consumed: None,
            producer_latency: Duration::ZERO,
            consumer_latency: Duration::ZERO,
        }
    }
}

impl<S: Unpin> Unpin for MeteredStream<S> {}

impl<S> Stream for MeteredStream<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = self.inner.poll_next_unpin(cx);
        let now = Instant::now();

        if let Some(time) = self.last_item_consumed.take() {
            // The consumer is back for more.
            self.consumer_latency += now.duration_since(time);
        }

        match &result {
            Poll::Ready(Some(_)) => {
                if let Some(time) = self.pending_since.take() {
                    self.producer_latency += now.duration_since(time);
                }
                self.last_item_consumed.replace(now);
            }
            Poll::Ready(None) => {} // nothing to do
            Poll::Pending => {
                self.pending_since.get_or_insert(now);
            }
        };

        result
    }
}

impl<S> Drop for MeteredStream<S> {
    fn drop(&mut self) {
        relay_statsd::metric!(
            timer(RelayTimers::StreamProducerLatency) = self.producer_latency,
            name = self.name
        );
        relay_statsd::metric!(
            timer(RelayTimers::StreamConsumerLatency) = self.consumer_latency,
            name = self.name
        );
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;
    use futures::stream;
    use tokio::time::Duration;

    use super::*;

    fn parse_metric_value(s: &str) -> f64 {
        regex::Regex::new(r":([0-9.]+)\|")
            .unwrap()
            .captures(s)
            .unwrap()[1]
            .parse()
            .unwrap()
    }

    #[test]
    fn test_slow_producer() {
        let captures = relay_statsd::with_capturing_test_client(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let inner = stream::iter([1u8, 2, 3])
                    .then(|x| async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        x
                    })
                    .boxed();
                MeteredStream::new("test", inner).collect::<Vec<_>>().await;
            });
        });

        let producer = captures.iter().find(|s| s.contains("producer")).unwrap();
        let consumer = captures.iter().find(|s| s.contains("consumer")).unwrap();

        assert!(producer.contains("name:test"));
        assert!(consumer.contains("name:test"));
        // Producer was slow (we slept), consumer was immediate.
        assert!(parse_metric_value(producer) > 30.0); // 3 x 10 milliseconds
        assert!(parse_metric_value(consumer) < 1.0); // < 1 millisecond
    }

    #[test]
    fn test_slow_consumer() {
        let captures = relay_statsd::with_capturing_test_client(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let inner = stream::iter([1u8, 2, 3]).boxed();
                let mut stream = MeteredStream::new("test", inner);
                while let Some(_) = stream.next().await {
                    // Simulate a slow consumer (e.g. a backpressured sink).
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        });

        let producer = captures.iter().find(|s| s.contains("producer")).unwrap();
        let consumer = captures.iter().find(|s| s.contains("consumer")).unwrap();

        // Consumer was slow (we slept between polls), producer was immediate.
        assert!(parse_metric_value(producer) < 1.0); // < 1 millisecond
        assert!(parse_metric_value(consumer) > 20.0); // 2 x 10 milliseconds (last item has no successor)
    }
}
