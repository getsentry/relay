use std::sync::Arc;

use futures::future::BoxFuture;
use relay_log::sentry::SentryFutureExt;
use relay_system::{MessageHandler, Receiver, Service};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::AsyncPool;

/// An async thread pool used for scheduling processing tasks in a service.
pub type ServicePool = AsyncPool<BoxFuture<'static, ()>>;

/// A service that delegates its work to an [`ServicePool`].
///
/// Waits for all delegated tasks to finish when its message channel closes.
pub struct AsyncPoolService<T> {
    handler: T,
    pool: ServicePool,
}

impl<T> AsyncPoolService<T> {
    /// Creates a new service from a handler and a pool.
    pub fn new(handler: T, pool: ServicePool) -> Self {
        Self { handler, pool }
    }
}

impl<T: MessageHandler + Clone + Send + 'static> Service for AsyncPoolService<T> {
    type Interface = T::Interface;

    async fn run(self, mut rx: Receiver<Self::Interface>) {
        let task_tracker = TaskTracker::new();

        while let Some(message) = rx.recv().await {
            let handler = self.handler.clone();
            // Create a new hub to prevent sentry scopes from bleeding to other tasks.
            let hub = relay_log::Hub::new_from_top(relay_log::Hub::current());

            let permit = task_tracker.track_one().await;
            self.pool
                .spawn_async(Box::pin(
                    async move {
                        let _permit = permit;
                        handler.handle_message(message).await;
                    }
                    .bind_hub(hub),
                ))
                .await;
        }
        // Drop the pool to remove any stalled permits. Caveat: Other pool owners might still
        // keep it alive.
        drop(self.pool);

        // Wait for all tasks to be done
        task_tracker.all_done().await;
    }

    fn name() -> &'static str {
        T::name()
    }
}

struct TaskTracker {
    size: usize,
    semaphore: Arc<Semaphore>,
}

impl TaskTracker {
    fn new() -> Self {
        // Initialize semaphore with the highest number that can be acquired at once.
        let size = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
        let semaphore = Arc::new(Semaphore::new(size));

        Self { size, semaphore }
    }

    async fn track_one(&self) -> OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should be open")
    }

    async fn all_done(&self) {
        let _ = self
            .semaphore
            .acquire_many(self.size as u32)
            .await
            .expect("semaphore should be open");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::FutureExt;
    use relay_system::{FromMessage, Interface, NoResponse, channel};
    use tokio::runtime::Handle;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use crate::AsyncPoolBuilder;

    use super::*;

    #[derive(Clone)]
    struct TestHandler;
    impl MessageHandler for TestHandler {
        type Interface = TestMessage;

        async fn handle_message(&self, message: Self::Interface) {
            message.0.await;
        }
    }

    struct TestMessage(BoxFuture<'static, ()>);

    impl Interface for TestMessage {}

    impl FromMessage<Self> for TestMessage {
        type Response = NoResponse;

        fn from_message(message: Self, _: ()) -> Self {
            message
        }
    }

    #[tokio::test]
    async fn service_waits_for_queued_and_running_tasks() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(1)
            .max_concurrency(1)
            .build()
            .unwrap();
        let (addr, rx) = channel("test");
        let gate = Arc::new(Semaphore::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let (started, mut starts) = mpsc::unbounded_channel();

        for _ in 0..2 {
            let gate = gate.clone();
            let completed = completed.clone();
            let started = started.clone();
            addr.send(TestMessage(Box::pin(async move {
                started.send(()).unwrap();
                gate.acquire().await.unwrap().forget();
                completed.fetch_add(1, Ordering::SeqCst);
            })));
        }
        drop(addr);

        let service = AsyncPoolService::new(TestHandler, pool).run(rx);
        tokio::pin!(service);
        // Both messages fit in the pool queue, so this polls through to the drain.
        assert!(service.as_mut().now_or_never().is_none());
        timeout(Duration::from_secs(5), starts.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(service.as_mut().now_or_never().is_none());

        gate.add_permits(1);
        timeout(Duration::from_secs(5), starts.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(service.as_mut().now_or_never().is_none());

        gate.add_permits(1);
        timeout(Duration::from_secs(5), service).await.unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn service_finishes_without_tasks() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(1)
            .build()
            .unwrap();
        let (addr, rx) = channel("test");
        drop(addr);

        timeout(
            Duration::from_secs(5),
            AsyncPoolService::new(TestHandler, pool).run(rx),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn service_releases_permits_on_panic() {
        // Test both a recovered task panic and a panic that terminates the worker,
        // dropping the queued task before it has ever been polled.
        for recover_task in [true, false] {
            let mut builder = AsyncPoolBuilder::new(Handle::current())
                .num_threads(1)
                .max_concurrency(1)
                .thread_panic_handler(|_| {});
            if recover_task {
                builder = builder.task_panic_handler(|_| {});
            }
            let pool = builder.build().unwrap();
            let (addr, rx) = channel("test");
            let (trigger, panic) = tokio::sync::oneshot::channel();
            addr.send(TestMessage(Box::pin(async move {
                panic.await.unwrap();
                panic!("test task panic");
            })));
            addr.send(TestMessage(Box::pin(async {})));
            drop(addr);

            let service = AsyncPoolService::new(TestHandler, pool).run(rx);
            tokio::pin!(service);
            assert!(service.as_mut().now_or_never().is_none());
            trigger.send(()).unwrap();
            timeout(Duration::from_secs(5), service).await.unwrap();
        }
    }

    #[test]
    fn service_uses_inner_name() {
        assert_eq!(
            AsyncPoolService::<TestHandler>::name(),
            "relay_threading::service::tests::TestHandler"
        );
    }

    #[test]
    fn service_uses_inner_name_for_arc() {
        assert_eq!(
            AsyncPoolService::<Arc<TestHandler>>::name(),
            "relay_threading::service::tests::TestHandler"
        );
    }
}
