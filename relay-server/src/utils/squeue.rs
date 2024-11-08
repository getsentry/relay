use std::collections::BinaryHeap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::stream::FusedStream;
use tokio::time::Instant;

use futures::Stream;

/// A scheduled queue that can be polled for when the next item is ready.
#[derive(Debug)]
pub struct ScheduledQueue<T> {
    inner: BinaryHeap<Item<T>>,
    waker: Option<Waker>,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl<T> ScheduledQueue<T> {
    /// Creates a new, empty [`ScheduledQueue`].
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
            waker: None,
            sleep: Box::pin(tokio::time::sleep(Duration::MAX)),
        }
    }

    /// Returns the current size of the queue.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no items in the queue.
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Schedules a new item to be yielded at `when`.
    pub fn schedule(&mut self, when: Instant, value: T) {
        self.inner.push(Item { when, value });
        if let Some(ref waker) = self.waker {
            waker.wake_by_ref();
        }
    }
}

impl<T> Unpin for ScheduledQueue<T> {}

impl<T> Stream for ScheduledQueue<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker = Some(cx.waker().clone());

        let sleep = self.sleep.as_mut().poll(cx);

        // No item means, we're pending until scheduled (through adding an item).
        if let Some(next) = self.inner.peek() {
            let when = next.when;

            // TODO: maybe optimize here for `when` being in the past but the sleep
            // is still in the future. This can happen when an item with a shorter
            // deadline, than the item at the front of the queue, is inserted.
            //
            // With the current behaviour we will reset the sleep deadline (in the else branch)
            // to a value which will immediately be ready.
            // Only after the sleep yields ready, we yield the item.
            //
            // This is one more wakeup and sleep poll than necessary.
            //
            // The current design is much simpler and less prone to mistakes since
            // the sleep is always synchronized to the first item in the queue.
            if matches!(sleep, Poll::Ready(_)) && when <= Instant::now() {
                // We already expired the first item, yield it.
                let current = self.inner.pop().unwrap();

                let next_deadline = self
                    .inner
                    .peek()
                    .map(|item| item.when)
                    .unwrap_or_else(far_future);
                self.sleep.as_mut().reset(next_deadline);
                // Immediately wake up to check the next item and to poll the new sleep deadline.
                cx.waker().wake_by_ref();

                return Poll::Ready(Some(current.value));
            } else if self.sleep.deadline() != when {
                // Somehow the deadline does not match the first item anymore, adjust the deadline.
                // This may happen when there is a new item in the queue and we got woken to
                // update the deadline or in the rare case where the first item got removed
                // while we were getting woken from the sleep.
                self.sleep.as_mut().reset(when);
                // Immediately wake to await the new deadline.
                cx.waker().wake_by_ref();
            }
        }

        // Next wake up already triggered or will be triggered by sleep.
        Poll::Pending
    }
}

impl<T> FusedStream for ScheduledQueue<T> {
    fn is_terminated(&self) -> bool {
        // The stream never returns `Poll::Ready(None)`.
        false
    }
}

impl<T> Default for ScheduledQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

struct Item<T> {
    when: Instant,
    value: T,
}

impl<T> PartialEq for Item<T> {
    fn eq(&self, other: &Self) -> bool {
        other.when == self.when
    }
}
impl<T> Eq for Item<T> {}

impl<T> PartialOrd for Item<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Item<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.when.cmp(&other.when).reverse()
    }
}

impl<T> fmt::Debug for Item<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

/// Creates an instant in the far future which is not expected to be hit.
///
/// This follows an internal implementation detail for [`tokio::time::sleep`].
fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(86400 * 365 * 30)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_squeue() {
        let mut s = ScheduledQueue::new();

        let start = Instant::now();

        s.schedule(start + Duration::from_millis(100), 4);
        s.schedule(start + Duration::from_millis(150), 5);

        s.schedule(start + Duration::from_nanos(2), 2);
        s.schedule(start + Duration::from_nanos(1), 1);

        for i in 1..6 {
            let value = s.next().await.unwrap();
            assert_eq!(value, i);

            if i == 2 {
                // schedule now!
                s.schedule(start, 3);
            }
        }

        assert!(s.is_empty());
    }
}
