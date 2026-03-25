use std::task::Poll;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};

use crate::statsd::RelayTimers;

/// A stream wrapper that emits a metric per item for producer and consumer latency.
pub struct MeteredStream<S> {
    inner: S,
    name: &'static str,
    pending_since: Option<Instant>,
    last_item_consumed: Option<Instant>,
}

impl<S> MeteredStream<S>
where
    S: Stream,
{
    /// Create a new metered stream from an existing stream.
    pub fn new(inner: S, name: &'static str) -> Self {
        Self {
            inner,
            name,
            pending_since: None,
            last_item_consumed: None,
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
            relay_statsd::metric!(
                timer(RelayTimers::StreamConsumerLatency) = now.duration_since(time),
                name = self.name
            );
        }

        match &result {
            Poll::Ready(Some(_)) => {
                let producer_latency = match self.pending_since.take() {
                    Some(t) => now.duration_since(t),
                    None => Duration::ZERO,
                };
                relay_statsd::metric!(
                    timer(RelayTimers::StreamProducerLatency) = producer_latency,
                    name = self.name
                );
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
                MeteredStream::new(inner, "test").collect::<Vec<_>>().await;
            });
        });

        let producers: Vec<_> = captures.iter().filter(|s| s.contains("producer")).collect();
        let consumers: Vec<_> = captures.iter().filter(|s| s.contains("consumer")).collect();

        assert_eq!(producers.len(), 3);
        assert_eq!(consumers.len(), 3);
        // Producer was slow (we slept), consumer was immediate.
        assert!(producers.iter().all(|s| parse_metric_value(s) > 10.0));
        assert!(consumers.iter().all(|s| parse_metric_value(s) < 1.0));
    }

    #[test]
    fn test_slow_consumer() {
        let captures = relay_statsd::with_capturing_test_client(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let inner = stream::iter([1u8, 2, 3]).boxed();
                let mut stream = MeteredStream::new(inner, "test");
                while let Some(_) = stream.next().await {
                    // Simulate a slow consumer.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        });

        let producers: Vec<_> = captures.iter().filter(|s| s.contains("producer")).collect();
        let consumers: Vec<_> = captures.iter().filter(|s| s.contains("consumer")).collect();

        assert_eq!(producers.len(), 3);
        assert_eq!(consumers.len(), 3);
        // Consumer was slow (we slept between polls), producer was immediate.
        assert!(producers.iter().all(|s| parse_metric_value(s) < 1.0));
        assert!(consumers.iter().all(|s| parse_metric_value(s) > 10.0));
    }
}
