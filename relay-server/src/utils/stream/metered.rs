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
