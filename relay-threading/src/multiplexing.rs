use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::CatchUnwind;
use futures::stream::{FusedStream, FuturesUnordered, Stream};
use futures::FutureExt;
use pin_project_lite::pin_project;
use tokio::task::Unconstrained;

pin_project! {
    /// Manages a collection of asynchronous tasks that are executed concurrently.
    ///
    /// This helper is designed for use within the [`Multiplexed`] executor to schedule and drive tasks
    /// while respecting a set concurrency limit.
    struct Tasks<F> {
        #[pin]
        futures: FuturesUnordered<Unconstrained<CatchUnwind<AssertUnwindSafe<F>>>>,
        panic_handler: Option<Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync>>,
    }
}

impl<F> Tasks<F> {
    /// Initializes a new [`Tasks`] collection for managing asynchronous tasks.
    ///
    /// This collection is used internally by [`Multiplexed`] to schedule task execution.
    fn new(panic_handler: Option<Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync>>) -> Self {
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
    /// Adds a new asynchronous task to the collection.
    ///
    /// Use this method to submit additional tasks for concurrent execution.
    fn push(&mut self, future: F) {
        let future = AssertUnwindSafe(future).catch_unwind();
        self.futures.push(tokio::task::unconstrained(future));
    }

    /// Drives the scheduled tasks until one remains pending.
    ///
    /// This method advances the execution of managed tasks until it encounters a task
    /// that is not immediately ready, ensuring that completed tasks are processed and the
    /// multiplexer can schedule new ones.
    ///
    /// For any task that panics, the `panic_handler` callback will be called.
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
    /// A task multiplexer that concurrently executes asynchronous tasks while limiting the maximum
    /// number of tasks running simultaneously.
    ///
    /// The [`Multiplexed`] executor retrieves tasks from a stream and schedules them according to the
    /// specified concurrency limit. The multiplexer completes once all tasks have been executed and
    /// no further tasks are available.
    pub struct Multiplexed<S, F> {
        max_concurrency: usize,
        #[pin]
        rx: S,
        #[pin]
        tasks: Tasks<F>,
    }
}
impl<S, F> Multiplexed<S, F>
where
    S: Stream<Item = F>,
{
    /// Constructs a new [`Multiplexed`] executor with a concurrency limit and a stream of tasks.
    ///
    /// This multiplexer should be awaited until completion, at which point all submitted tasks
    /// will have been executed.
    pub fn new(
        max_concurrency: usize,
        rx: S,
        panic_handler: Option<Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync>>,
    ) -> Self {
        Self {
            max_concurrency,
            rx,
            tasks: Tasks::new(panic_handler),
        }
    }
}

impl<S, F> Future for Multiplexed<S, F>
where
    S: FusedStream<Item = F>,
    F: Future<Output = ()>,
{
    type Output = ();

    /// Drives the execution of the multiplexer by advancing scheduled tasks and fetching new ones.
    ///
    /// This method polls the collection of tasks and retrieves additional tasks from the stream until
    /// the concurrency limit is reached or no more tasks are available. It yields `Poll::Pending` when
    /// there is ongoing work and returns `Poll::Ready(())` only once the task stream is exhausted
    /// and no active tasks remain.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            this.tasks.as_mut().poll_tasks_until_pending(cx);

            if this.tasks.len() >= *this.max_concurrency {
                return Poll::Pending;
            }

            match this.rx.as_mut().poll_next(cx) {
                Poll::Ready(Some(task)) => this.tasks.push(task),
                // The stream is exhausted and there are no remaining tasks.
                Poll::Ready(None) if this.tasks.is_empty() => return Poll::Ready(()),
                // The stream is exhausted but tasks remain active.
                Poll::Ready(None) => return Poll::Pending,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use futures::{future::BoxFuture, FutureExt};

    use super::*;

    fn future_with(block: impl FnOnce() + Send + 'static) -> BoxFuture<'static, ()> {
        let fut = async {
            // Yield to allow a pending state during polling.
            tokio::task::yield_now().await;
            block();
        };

        fut.boxed()
    }

    #[test]
    fn test_multiplexer_with_no_futures() {
        let (_, rx) = flume::bounded::<BoxFuture<'static, _>>(10);
        futures::executor::block_on(Multiplexed::new(1, rx.into_stream(), None));
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
            1,
            rx.into_stream(),
            Some(Arc::new(panic_handler)),
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
            futures::executor::block_on(Multiplexed::new(1, rx.into_stream(), None))
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

        futures::executor::block_on(Multiplexed::new(1, rx.into_stream(), None));

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

        futures::executor::block_on(Multiplexed::new(1, rx.into_stream(), None));

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

        futures::executor::block_on(Multiplexed::new(5, rx.into_stream(), None));

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

        futures::executor::block_on(Multiplexed::new(5, rx.into_stream(), None));

        // The order of completion is expected to be the same as the order of submission.
        assert_eq!(*entries.lock().unwrap(), (0..5).collect::<Vec<_>>());
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

        futures::executor::block_on(Multiplexed::new(5, rx.into_stream(), None));

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
}
