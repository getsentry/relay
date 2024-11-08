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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future::{ready, Ready};

    use super::*;

    #[tokio::test]
    async fn test_stask_empty() {
        let mut stask = ScheduledTasks::<Ready<()>>::new();

        assert_eq!(stask.len(), 0);
        let mut next = stask.next();
        for _ in 0..10 {
            assert_eq!(futures::poll!(&mut next), Poll::Pending);
        }
        assert_eq!(stask.len(), 0);
    }

    #[tokio::test]
    async fn test_stask_immediate_task() {
        let mut stask = ScheduledTasks::new();

        stask.schedule(None, ready(()));
        assert_eq!(stask.len(), 1);

        let mut next = stask.next();
        assert_eq!(futures::poll!(&mut next), Poll::Ready(Some(())));
        assert_eq!(stask.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_stask_scheduled_task() {
        let mut stask = ScheduledTasks::new();

        stask.schedule(Some(Instant::now() + Duration::from_secs(3)), ready(()));
        assert_eq!(stask.len(), 1);

        let mut next = stask.next();
        assert_eq!(futures::poll!(&mut next), Poll::Pending);
        tokio::time::sleep(Duration::from_millis(2800)).await;
        assert_eq!(futures::poll!(&mut next), Poll::Pending);
        tokio::time::sleep(Duration::from_millis(201)).await;
        assert_eq!(futures::poll!(&mut next), Poll::Ready(Some(())));

        assert_eq!(stask.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_stask_scheduled_task_next_cancelled() {
        let mut stask = ScheduledTasks::new();

        stask.schedule(Some(Instant::now() + Duration::from_secs(3)), ready(()));
        assert_eq!(stask.len(), 1);

        let mut next = stask.next();
        assert_eq!(futures::poll!(&mut next), Poll::Pending);
        tokio::time::sleep(Duration::from_millis(2800)).await;
        assert_eq!(futures::poll!(&mut next), Poll::Pending);
        drop(next);

        assert_eq!(stask.len(), 1);
        tokio::time::sleep(Duration::from_millis(201)).await;
        assert_eq!(futures::poll!(stask.next()), Poll::Ready(Some(())));

        assert_eq!(stask.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_stask_mixed_tasks() {
        let mut stask = ScheduledTasks::new();

        let now = Instant::now();

        stask.schedule(None, ready(0));
        stask.schedule(Some(now + Duration::from_secs(2)), ready(2));
        stask.schedule(Some(now + Duration::from_secs(1)), ready(1));
        stask.schedule(Some(now + Duration::from_secs(3)), ready(3));
        assert_eq!(stask.len(), 4);

        assert_eq!(stask.next().await, Some(0));
        assert_eq!(stask.next().await, Some(1));
        stask.schedule(None, ready(90));
        assert_eq!(stask.next().await, Some(90));
        stask.schedule(Some(now), ready(91)); // Now in the past.
        assert_eq!(stask.next().await, Some(91));
        assert_eq!(stask.next().await, Some(2));
        stask.schedule(Some(now + Duration::from_secs(4)), ready(4));
        assert_eq!(stask.len(), 2);
        assert_eq!(stask.next().await, Some(3));
        assert_eq!(stask.next().await, Some(4));

        assert_eq!(stask.len(), 0);
        assert!(Instant::now() < now + Duration::from_millis(4001));
        assert_eq!(futures::poll!(stask.next()), Poll::Pending);

        stask.schedule(Some(Instant::now()), ready(92));
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(1), stask.next())
                .await
                .unwrap(),
            Some(92)
        );

        assert_eq!(futures::poll!(stask.next()), Poll::Pending);
        assert_eq!(stask.len(), 0);
    }
}
