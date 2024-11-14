use futures::StreamExt as _;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::FusedStream;
use tokio::time::Instant;

use futures::Stream;

/// A scheduled queue that can be polled for when the next item is ready.
pub struct ScheduledQueue<T> {
    inner: Inner<BinaryHeap<Item<T>>>,
}

impl<T> ScheduledQueue<T> {
    /// Creates a new, empty [`ScheduledQueue`].
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
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
    }
}

impl<T: fmt::Debug> fmt::Debug for ScheduledQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let now = Instant::now();
        let mut f = f.debug_list();
        for Item { when, value } in self.inner.iter() {
            f.entry(&(when.saturating_duration_since(now), value));
        }
        f.finish()
    }
}

impl<T> Stream for ScheduledQueue<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl<T> FusedStream for ScheduledQueue<T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<T> Default for ScheduledQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A scheduled queue that can be polled for when the next item is ready.
///
/// Unlike [`ScheduledQueue`] every unique `T` can only be scheduled once,
/// scheduling a value again moves the deadline instead.
pub struct UniqueScheduledQueue<T>
where
    T: std::hash::Hash + Eq,
{
    inner: Inner<PriorityQueue<T, Reverse<Instant>>>,
}

impl<T: std::hash::Hash + Eq> UniqueScheduledQueue<T> {
    /// Creates a new, empty [`UniqueScheduledQueue`].
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    /// Returns the current size of the queue.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no items in the queue.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Schedules an item to be yielded at `when`.
    ///
    /// If the item was net yet scheduled, it is inserted into the queue,
    /// otherwise the previous schedule is moved to the new deadline.
    pub fn schedule(&mut self, when: Instant, value: T) {
        self.inner.push(value, Reverse(when));
    }

    /// Removes a value from the queue.
    pub fn remove(&mut self, value: &T) {
        self.inner.remove(value);
    }
}

impl<T: fmt::Debug + std::hash::Hash + Eq> fmt::Debug for UniqueScheduledQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let now = Instant::now();
        let mut f = f.debug_list();
        for (value, Reverse(when)) in self.inner.iter() {
            f.entry(&(when.saturating_duration_since(now), value));
        }
        f.finish()
    }
}

impl<T: std::hash::Hash + Eq> Stream for UniqueScheduledQueue<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl<T: std::hash::Hash + Eq> FusedStream for UniqueScheduledQueue<T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}
//
impl<T: std::hash::Hash + Eq> Default for UniqueScheduledQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

trait Queue {
    type Value;

    /// Peeks into the queue returning the deadline of the first item.
    fn peek_when(&self) -> Option<Instant>;
    /// Removes the first element from the queue returning its value.
    ///
    /// A successful [`Self::peek_when`] followed by a [`Self::pop_value`] must not return `None`.
    fn pop_value(&mut self) -> Option<Self::Value>;
}

impl<T> Queue for BinaryHeap<Item<T>> {
    type Value = T;

    fn peek_when(&self) -> Option<Instant> {
        self.peek().map(|item| item.when)
    }

    fn pop_value(&mut self) -> Option<Self::Value> {
        self.pop().map(|item| item.value)
    }
}

impl<T: std::hash::Hash + Eq> Queue for priority_queue::PriorityQueue<T, Reverse<Instant>> {
    type Value = T;

    fn peek_when(&self) -> Option<Instant> {
        self.peek().map(|(_, Reverse(when))| *when)
    }

    fn pop_value(&mut self) -> Option<Self::Value> {
        self.pop().map(|(value, _)| value)
    }
}

#[derive(Debug)]
struct Inner<Q> {
    inner: Q,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl<Q> std::ops::Deref for Inner<Q> {
    type Target = Q;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Q> std::ops::DerefMut for Inner<Q> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Q: Default> Default for Inner<Q> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            sleep: Box::pin(tokio::time::sleep(Duration::MAX)),
        }
    }
}

impl<Q> Unpin for Inner<Q> {}

impl<Q: Queue> Stream for Inner<Q> {
    type Item = Q::Value;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(when) = self.inner.peek_when() {
            // The head of the queue changed, reset the deadline.
            if self.sleep.deadline() != when {
                self.sleep.as_mut().reset(when);
            }

            // Poll and wait for the next item to be ready.
            if self.sleep.as_mut().poll(cx).is_ready() {
                // Item is ready, yield it.
                let value = self.inner.pop_value().expect("pop after peek");
                return Poll::Ready(Some(value));
            }
        }

        Poll::Pending
    }
}

impl<Q: Queue> FusedStream for Inner<Q> {
    fn is_terminated(&self) -> bool {
        // The stream never returns `Poll::Ready(None)`.
        false
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_scheduled_queue() {
        let mut s = ScheduledQueue::new();

        let start = Instant::now();

        s.schedule(start + Duration::from_millis(100), 4);
        s.schedule(start + Duration::from_millis(150), 5);

        s.schedule(start + Duration::from_nanos(3), 2);
        s.schedule(start + Duration::from_nanos(2), 2);
        s.schedule(start + Duration::from_nanos(1), 1);

        assert_eq!(s.len(), 5);
        assert_eq!(s.next().await, Some(1));
        assert_eq!(s.next().await, Some(2));
        assert_eq!(s.next().await, Some(2));

        // Schedule immediately!
        s.schedule(start, 3);

        assert_eq!(s.len(), 3);
        assert_eq!(s.next().await, Some(3));
        assert_eq!(s.next().await, Some(4));
        assert_eq!(s.next().await, Some(5));

        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_unique_scheduled_queue() {
        let mut s = UniqueScheduledQueue::new();

        let start = Instant::now();

        s.schedule(start, "xxx");
        s.schedule(start + Duration::from_nanos(1), "a");
        s.schedule(start + Duration::from_nanos(2), "b");
        s.schedule(start + Duration::from_millis(100), "c");
        s.schedule(start + Duration::from_millis(150), "d");
        s.schedule(start + Duration::from_millis(200), "e");

        assert_eq!(s.len(), 6);
        s.remove(&"xxx");
        assert_eq!(s.len(), 5);

        assert_eq!(s.next().await, Some("a"));
        assert_eq!(s.len(), 4);

        // Move `b` to the end.
        s.schedule(start + Duration::from_secs(1), "b");
        // Move `d` before `c`.
        s.schedule(start + Duration::from_millis(99), "d");
        // Immediately schedule a new element.
        s.schedule(start, "x");

        assert_eq!(s.len(), 5);
        assert_eq!(s.next().await, Some("x"));
        assert_eq!(s.next().await, Some("d"));
        assert_eq!(s.next().await, Some("c"));
        assert_eq!(s.next().await, Some("e"));
        assert_eq!(s.next().await, Some("b"));

        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }
}
