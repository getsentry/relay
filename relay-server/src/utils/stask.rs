use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::utils::ScheduledQueue;
use futures::{
    stream::{FusedStream, FuturesUnordered},
    Stream, StreamExt,
};
use tokio::time::Instant;

/// A set of tasks/futures that can be scheduled for execution.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct ScheduledTasks<T> {
    queue: ScheduledQueue<T>,
    tasks: FuturesUnordered<T>,
}

impl<T> ScheduledTasks<T> {
    /// Creates a new, empty [`ScheduledTasks`].
    pub fn new() -> Self {
        Self {
            queue: ScheduledQueue::new(),
            tasks: FuturesUnordered::new(),
        }
    }

    /// Returns the total amount of scheduled and active tasks.
    pub fn len(&self) -> usize {
        self.queue.len() + self.tasks.len()
    }

    /// Schedules a new `task`.
    ///
    /// A `None` `when` value indicates the task should be scheduled immediately.
    pub fn schedule(&mut self, when: Option<Instant>, task: T) {
        match when {
            Some(when) => self.queue.schedule(when, task),
            None => self.tasks.push(task),
        }
    }
}

impl<T> Default for ScheduledTasks<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Unpin for ScheduledTasks<T> {}

impl<T> Stream for ScheduledTasks<T>
where
    T: Future,
{
    type Item = T::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Queue is fused, but also never terminating, it is okay to match on `Some` directly here.
        if let Poll::Ready(Some(next)) = self.queue.poll_next_unpin(cx) {
            self.tasks.push(next);
        }

        match self.tasks.poll_next_unpin(cx) {
            // An item is ready, yield it.
            Poll::Ready(Some(next)) => Poll::Ready(Some(next)),
            // There are no more tasks in the queue, this is now pending waiting for another task.
            // It is fine to just remember this as pending, `FuturesUnordered` is fused.
            Poll::Ready(None) => Poll::Pending,
            // No task ready, keep waiting.
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> FusedStream for ScheduledTasks<T>
where
    T: Future,
{
    fn is_terminated(&self) -> bool {
        // The stream never returns `Poll::Ready(None)`.
        false
    }
}
