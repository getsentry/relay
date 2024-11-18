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
    queue: BinaryHeap<Item<T>>,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl<T> ScheduledQueue<T> {
    /// Schedules a new item to be yielded at `when`.
    pub fn schedule(&mut self, when: Instant, value: T) {
        self.queue.push(Item { when, value });
    }

    fn peek_when(&self) -> Option<Instant> {
        self.queue.peek().map(|item| item.when)
    }

    fn pop_value(&mut self) -> Option<T> {
        self.queue.pop().map(|item| item.value)
    }

    fn iter(&self) -> impl Iterator<Item = (Instant, &T)> + '_ {
        self.queue.iter().map(|item| (item.when, &item.value))
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
    queue: PriorityQueue<T, Reverse<Instant>>,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl<T: std::hash::Hash + Eq> UniqueScheduledQueue<T> {
    /// Schedules an item to be yielded at `when`.
    ///
    /// If the item was net yet scheduled, it is inserted into the queue,
    /// otherwise the previous schedule is moved to the new deadline.
    pub fn schedule(&mut self, when: Instant, value: T) {
        self.queue.push(value, Reverse(when));
    }

    /// Removes a value from the queue.
    pub fn remove(&mut self, value: &T) {
        self.queue.remove(value);
    }

    fn peek_when(&self) -> Option<Instant> {
        self.queue.peek().map(|(_, Reverse(when))| *when)
    }

    fn pop_value(&mut self) -> Option<T> {
        self.queue.pop().map(|(value, _)| value)
    }

    fn iter(&self) -> impl Iterator<Item = (Instant, &T)> + '_ {
        self.queue
            .iter()
            .map(|(value, Reverse(when))| (*when, value))
    }
}

macro_rules! impl_queue {
    ($name:ident, $($where:tt)*) => {
        impl<T: $($where)*> $name<T> {
            /// Creates a new, empty [`Self`].
            pub fn new() -> Self {
                Self {
                    queue: Default::default(),
                    sleep: Box::pin(tokio::time::sleep(Duration::MAX)),
                }
            }

            /// Returns the current size of the queue.
            #[allow(dead_code)]
            pub fn len(&self) -> usize {
                self.queue.len()
            }

            /// Returns true if there are no items in the queue.
            #[allow(dead_code)]
            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }
        }

        impl<T: $($where)*> Default for $name<T> {
            fn default() -> Self {
                Self::new()
            }
        }

        impl<T: $($where)*> Unpin for $name<T> {}

        impl<T: $($where)*> FusedStream for $name<T> {
            fn is_terminated(&self) -> bool {
                // The stream never returns `Poll::Ready(None)`.
                false
            }
        }

        impl<T: $($where)*> Stream for $name<T> {
            type Item = T;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if let Some(when) = self.peek_when() {
                    // The head of the queue changed, reset the deadline.
                    if self.sleep.deadline() != when {
                        self.sleep.as_mut().reset(when);
                    }

                    // Poll and wait for the next item to be ready.
                    if self.sleep.as_mut().poll(cx).is_ready() {
                        // Item is ready, yield it.
                        let value = self.pop_value().expect("pop after peek");
                        return Poll::Ready(Some(value));
                    }
                }

                Poll::Pending
            }
        }

        impl<T: $($where)*>  fmt::Debug for $name<T> where T: fmt::Debug {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let now = Instant::now();
                let mut f = f.debug_list();
                for (when, value) in self.iter() {
                    f.entry(&(when.saturating_duration_since(now), value));
                }
                f.finish()
            }
        }
    };
}

impl_queue!(ScheduledQueue, Sized);
impl_queue!(UniqueScheduledQueue, std::hash::Hash + Eq);

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
