use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::CatchUnwind;
use futures::stream::{FusedStream, FuturesUnordered, Stream};
use futures::FutureExt;
use pin_project_lite::pin_project;
use tokio::task::Unconstrained;

use crate::{PanicHandler, ThreadMetrics};

pin_project! {
    /// Manages concurrent execution of asynchronous tasks.
    ///
    /// This internal structure collects and drives futures concurrently, invoking a panic handler (if provided)
    /// when a task encounters a panic.
    struct Tasks<F> {
        #[pin]
        futures: FuturesUnordered<Unconstrained<CatchUnwind<AssertUnwindSafe<F>>>>,
        panic_handler: Option<Arc<PanicHandler>>,
    }
}

impl<F> Tasks<F> {
    /// Creates a new task manager.
    ///
    /// This internal constructor initializes a new collection for tracking asynchronous tasks.
    fn new(panic_handler: Option<Arc<PanicHandler>>) -> Self {
        Self {
            futures: FuturesUnordered::new(),
            panic_handler,
        }
    }

    /// Returns the number of tasks currently scheduled for execution.
    fn len(&self) -> usize {
        self.futures.len()
    }

    /// Returns whether there are no tasks scheduled.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<F> Tasks<F>
where
    F: Future<Output = ()>,
{
    /// Adds a future to the collection for concurrent execution.
    fn push(&mut self, future: F) {
        let future = AssertUnwindSafe(future).catch_unwind();
        self.futures.push(tokio::task::unconstrained(future));
    }

    /// Drives the execution of collected tasks until a pending state is encountered.
    ///
    /// If a future panics and a panic handler is provided, the handler is invoked.
    /// Otherwise, the panic is propagated.
    ///
    /// # Panics
    ///
    /// Panics are either handled by the custom handler or propagated if no handler is specified.
    fn poll_tasks_until_pending(self: Pin<&mut Self>, cx: &mut Context<'_>) {
        let mut this = self.project();

        loop {
            // If the unordered pool of futures is terminated, we stop polling.
            if this.futures.is_terminated() {
                return;
            }

            // If we don't get a Ready(Some(_)), it means we are now polling a pending future or the
            // stream has ended, in that case we return.
            let Poll::Ready(Some(result)) = this.futures.as_mut().poll_next(cx) else {
                return;
            };

            // If there is an error, it means that the future has panicked, we want to notify this.
            match (this.panic_handler.as_ref(), result) {
                // Panic handler and error, we swallow the panic and invoke the callback.
                (Some(panic_handler), Err(error)) => {
                    panic_handler(error);
                }
                // No panic handler and error, we propagate the panic.
                (None, Err(error)) => {
                    std::panic::resume_unwind(error);
                }
                // Otherwise, we do nothing.
                (_, Ok(())) => {}
            }
        }
    }
}

pin_project! {
    /// [`Multiplexed`] is a future that concurrently schedules asynchronous tasks from a stream while ensuring that
    /// the number of concurrently executing tasks does not exceed a specified limit.
    ///
    /// This multiplexer is primarily used by the [`AsyncPool`] to manage task execution on worker threads.
    pub struct Multiplexed<S, F> {
        pool_name: &'static str,
        max_concurrency: usize,
        #[pin]
        rx: S,
        #[pin]
        tasks: Tasks<F>,
        metrics: Arc<ThreadMetrics>
    }
}

impl<S, F> Multiplexed<S, F>
where
    S: Stream<Item = F>,
{
    /// Creates a new [`Multiplexed`] instance with a defined concurrency limit and a stream of tasks.
    ///
    /// Tasks from the stream will be scheduled for execution concurrently, and an optional panic handler
    /// can be provided to manage errors during task execution.
    pub fn new(
        pool_name: &'static str,
        max_concurrency: usize,
        rx: S,
        panic_handler: Option<Arc<PanicHandler>>,
        metrics: Arc<ThreadMetrics>,
    ) -> Self {
        Self {
            pool_name,
            max_concurrency,
            rx,
            tasks: Tasks::new(panic_handler),
            metrics,
        }
    }
}

impl<S, F> Future for Multiplexed<S, F>
where
    S: FusedStream<Item = F>,
    F: Future<Output = ()>,
{
    type Output = ();

    /// Polls the [`Multiplexed`] future to drive task execution.
    ///
    /// This method repeatedly schedules new tasks from the stream while enforcing the concurrency limit.
    /// It completes when the stream is exhausted and no active tasks remain.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            // We report before polling since we might have only blocking tasks meaning that the
            // measure after the `poll_tasks_until_pending` will return 0, since all futures will
            // be completed.
            let before_len = this.tasks.len() as u64;
            this.metrics
                .active_tasks
                .store(before_len, Ordering::Relaxed);

            this.tasks.as_mut().poll_tasks_until_pending(cx);

            // We also want to report after polling since we might have finished polling some futures
            // and some not.
            let after_len = this.tasks.len() as u64;
            this.metrics
                .active_tasks
                .store(after_len, Ordering::Relaxed);

            // We calculate how many tasks have been driven to completion.
            if let Some(finished_tasks) = before_len.checked_sub(after_len) {
                this.metrics
                    .active_tasks
                    .store(finished_tasks, Ordering::Relaxed);
            }

            // If we can't get anymore tasks, and we don't have anything else to process, we report
            // ready. Otherwise, if we have something to process, we report pending.
            if this.tasks.is_empty() && this.rx.is_terminated() {
                return Poll::Ready(());
            } else if this.rx.is_terminated() {
                return Poll::Pending;
            }

            // If we could accept tasks, but we don't have space we report pending.
            if this.tasks.len() >= *this.max_concurrency {
                return Poll::Pending;
            }

            // At this point, we are free to start driving another future.
            match this.rx.as_mut().poll_next(cx) {
                Poll::Ready(Some(task)) => {
                    this.tasks.push(task);
                }
                // The stream is exhausted and there are no remaining tasks.
                Poll::Ready(None) if this.tasks.is_empty() => return Poll::Ready(()),
                // The stream is exhausted but tasks remain active. Now we need to make sure we
                // stop polling the stream and just process tasks.
                Poll::Ready(None) => return Poll::Pending,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{future::BoxFuture, FutureExt};
    use std::future;
    use std::sync::atomic::AtomicBool;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use std::time::Duration;

    use super::*;

    fn future_with(block: impl FnOnce() + Send + 'static) -> BoxFuture<'static, ()> {
        let fut = async {
            // Yield to allow a pending state during polling.
            tokio::task::yield_now().await;
            block();
        };

        fut.boxed()
    }

    fn mock_metrics() -> Arc<ThreadMetrics> {
        Arc::new(ThreadMetrics::default())
    }

    #[test]
    fn test_multiplexer_with_no_futures() {
        let (_, rx) = flume::bounded::<BoxFuture<'static, _>>(10);
        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            1,
            rx.into_stream(),
            None,
            mock_metrics(),
        ));
    }

    #[test]
    fn test_multiplexer_with_panic_handler_panicking_future() {
        let panic_handler_called = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = flume::bounded(10);

        let count_clone = count.clone();
        tx.send(future_with(move || {
            count_clone.fetch_add(1, Ordering::SeqCst);
            panic!("panicked");
        }))
        .unwrap();

        drop(tx);

        let panic_handler_called_clone = panic_handler_called.clone();
        let panic_handler = move |_| {
            panic_handler_called_clone.store(true, Ordering::SeqCst);
        };
        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            1,
            rx.into_stream(),
            Some(Arc::new(panic_handler)),
            mock_metrics(),
        ));

        // The count is expected to have been incremented and the handler called.
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(panic_handler_called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_multiplexer_with_no_panic_handler_panicking_future() {
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = flume::bounded(10);

        let count_clone = count.clone();
        tx.send(future_with(move || {
            count_clone.fetch_add(1, Ordering::SeqCst);
            panic!("panicked");
        }))
        .unwrap();

        drop(tx);

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            futures::executor::block_on(Multiplexed::new(
                "my_pool",
                1,
                rx.into_stream(),
                None,
                mock_metrics(),
            ))
        }));

        // The count is expected to have been incremented and the handler called.
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiplexer_with_one_concurrency_and_one_future() {
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = flume::bounded(10);

        let count_clone = count.clone();
        tx.send(future_with(move || {
            count_clone.fetch_add(1, Ordering::SeqCst);
        }))
        .unwrap();

        drop(tx);

        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            1,
            rx.into_stream(),
            None,
            mock_metrics(),
        ));

        // The count is expected to have been incremented.
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multiplexer_with_one_concurrency_and_multiple_futures() {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = flume::bounded(10);

        for i in 0..5 {
            let entries_clone = entries.clone();
            tx.send(future_with(move || {
                entries_clone.lock().unwrap().push(i);
            }))
            .unwrap();
        }

        drop(tx);

        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            1,
            rx.into_stream(),
            None,
            mock_metrics(),
        ));

        // The order of completion is expected to match the order of submission.
        assert_eq!(*entries.lock().unwrap(), (0..5).collect::<Vec<_>>());
    }

    #[test]
    fn test_multiplexer_with_multiple_concurrency_and_one_future() {
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = flume::bounded(10);

        let count_clone = count.clone();
        tx.send(future_with(move || {
            count_clone.fetch_add(1, Ordering::SeqCst);
        }))
        .unwrap();

        drop(tx);

        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            5,
            rx.into_stream(),
            None,
            mock_metrics(),
        ));

        // The count is expected to have been incremented.
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multiplexer_with_multiple_concurrency_and_multiple_futures() {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = flume::bounded(10);

        for i in 0..5 {
            let entries_clone = entries.clone();
            tx.send(future_with(move || {
                entries_clone.lock().unwrap().push(i);
            }))
            .unwrap();
        }

        drop(tx);

        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            5,
            rx.into_stream(),
            None,
            mock_metrics(),
        ));

        // The order of completion is expected to be the same as the order of submission.
        assert_eq!(*entries.lock().unwrap(), (0..5).collect::<Vec<_>>());
    }

    #[test]
    fn test_multiplexer_with_multiple_concurrency_and_less_multiple_futures() {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = flume::bounded(10);

        // We send 3 futures with a concurrency of 5, to make sure that if the stream returns
        // `Poll::Ready(None)` the system will stop polling from the stream and continue driving
        // the remaining futures.
        for i in 0..3 {
            let entries_clone = entries.clone();
            tx.send(future_with(move || {
                entries_clone.lock().unwrap().push(i);
            }))
            .unwrap();
        }

        drop(tx);

        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            5,
            rx.into_stream(),
            None,
            mock_metrics(),
        ));

        // The order of completion is expected to be the same as the order of submission.
        assert_eq!(*entries.lock().unwrap(), (0..3).collect::<Vec<_>>());
    }

    #[test]
    fn test_multiplexer_with_multiple_concurrency_and_multiple_futures_from_multiple_threads() {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = flume::bounded(10);

        let mut handles = vec![];
        for i in 0..5 {
            let entries_clone = entries.clone();
            let tx_clone = tx.clone();
            handles.push(std::thread::spawn(move || {
                tx_clone
                    .send(future_with(move || {
                        entries_clone.lock().unwrap().push(i);
                    }))
                    .unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        drop(tx);

        futures::executor::block_on(Multiplexed::new(
            "my_pool",
            5,
            rx.into_stream(),
            None,
            mock_metrics(),
        ));

        // The order of completion may vary; verify that all expected elements are present.
        let mut entries = entries.lock().unwrap();
        entries.sort();
        assert_eq!(*entries, (0..5).collect::<Vec<_>>());
    }

    #[test]
    fn test_catch_unwind_future_handles_panics() {
        let future = AssertUnwindSafe(async {
            panic!("panicked");
        })
        .catch_unwind();

        // The future should complete without propagating the panic but propagating the error.
        assert!(futures::executor::block_on(future).is_err());

        // Verify that non-panicking tasks complete normally.
        let future = AssertUnwindSafe(async {
            // A normal future that completes.
        })
        .catch_unwind();

        // The future should successfully complete.
        assert!(futures::executor::block_on(future).is_ok());
    }

    #[tokio::test]
    async fn test_multiplexer_emits_metrics() {
        let (tx, rx) = flume::bounded::<BoxFuture<'static, _>>(10);
        let metrics = mock_metrics();

        tx.send(future::pending().boxed()).unwrap();

        drop(tx);

        // We spawn the future, which will be indefinitely pending since it's never woken up.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(Multiplexed::new(
            "my_pool",
            1,
            rx.into_stream(),
            None,
            metrics.clone(),
        ));

        // We sleep to let the pending be processed by the `Multiplexed` so that the metric is then
        // correctly emitted.
        tokio::time::sleep(Duration::from_millis(1)).await;

        // We expect that now we have 1 active task, the one that is indefinitely pending.
        assert_eq!(metrics.active_tasks.load(Ordering::Relaxed), 1);
    }
}
