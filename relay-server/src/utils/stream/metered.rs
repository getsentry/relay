use std::task::Poll;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};

/// Times spent waiting for the sending end and the receiving end of the stream.
#[derive(Clone, Copy, Default)]
pub struct Latencies {
    /// Total time spent waiting for the sender.
    pub sender: Duration,
    /// Total time spent waiting for the receiver.
    pub receiver: Duration,
}

/// A stream wrapper that counts the time spent polling / waiting for the next poll.
pub struct MeteredStream<S, F>
where
    F: Fn(Latencies),
{
    inner: S,
    pending_since: Option<Instant>,
    last_item_produced: Option<Instant>,
    latencies: Latencies,
    callback: F,
}

impl<S, F> MeteredStream<S, F>
where
    S: Stream,
    F: Fn(Latencies),
{
    /// Create a new metered stream from an existing stream.
    pub fn new(inner: S, callback: F) -> Self {
        Self {
            inner,
            pending_since: None,
            last_item_produced: None,
            latencies: Latencies::default(),
            callback,
        }
    }
}

impl<S: Unpin, F> Unpin for MeteredStream<S, F> where F: Fn(Latencies) {}

impl<S, F> Stream for MeteredStream<S, F>
where
    S: Stream + Unpin,
    F: Fn(Latencies),
{
    type Item = S::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = self.inner.poll_next_unpin(cx);

        if let Some(time) = self.last_item_produced.take() {
            self.latencies.receiver += time.elapsed();
        }

        match &result {
            Poll::Ready(Some(_)) => {
                if let Some(time) = self.pending_since.take() {
                    self.latencies.sender += time.elapsed();
                }
            }
            Poll::Ready(None) => {} // nothing to do
            Poll::Pending => {
                self.pending_since.get_or_insert_with(Instant::now);
            }
        };

        result
    }
}

impl<S, F> Drop for MeteredStream<S, F>
where
    F: Fn(Latencies),
{
    fn drop(&mut self) {
        (self.callback)(self.latencies)
    }
}
