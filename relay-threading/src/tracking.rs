use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::AsyncPool;

/// View of a shared async pool that tracks its spawned futures so its work can be awaited.
///
/// Multiple [`TrackingPool`]s can share the same [`AsyncPool`],
/// but every [`TrackingPool`] monitors only its own subset of tasks.
///
/// This is useful when multiple services share the same pool.
pub struct TrackingPool<F> {
    pool: AsyncPool<Tracked<F>>,
    size: usize,
    semaphore: Arc<Semaphore>,
}

impl<F> TrackingPool<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    /// Creates a new tracking view of the given shared pool.
    pub fn new(pool: AsyncPool<Tracked<F>>) -> Self {
        // Initialize semaphore with the highest number that can be acquired at once.
        let size = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
        let semaphore = Arc::new(Semaphore::new(size));

        Self {
            pool,
            size,
            semaphore,
        }
    }

    /// Spawns a [`Tracked`] future on the underlying [`AsyncPool`].
    ///
    /// [`Self::finish`] will not return until all tasks spawned through this method have finished.
    pub async fn spawn_async(&self, future: F) {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should be open");

        self.pool
            .spawn_async(Tracked {
                future,
                _permit: permit,
            })
            .await
    }

    /// Releases ownership of the shared pool and waits until all tasks are done.
    pub async fn finish(self) {
        drop(self.pool);

        // NOTE: If all workers panic, a different owner of the shared pool might still keep
        // permits alive. This is unlikely because all task panics are handled.

        let _ = self
            .semaphore
            .acquire_many(self.size as u32)
            .await
            .expect("semaphore should be open");
    }
}

pin_project_lite::pin_project! {
    pub struct Tracked<F> {
        #[pin]
        future: F,
        _permit: OwnedSemaphorePermit,
    }
}

impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().future.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::FutureExt;
    use futures::future::BoxFuture;
    use tokio::runtime::Handle;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use crate::AsyncPoolBuilder;

    use super::*;

    #[tokio::test]
    async fn finish_waits_for_queued_and_running_tasks() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(1)
            .max_concurrency(1)
            .build()
            .unwrap();
        let pool = TrackingPool::new(pool);
        let gate = Arc::new(Semaphore::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let (started, mut starts) = mpsc::unbounded_channel();

        for _ in 0..2 {
            let gate = gate.clone();
            let completed = completed.clone();
            let started = started.clone();
            pool.spawn_async(async move {
                started.send(()).unwrap();
                gate.acquire().await.unwrap().forget();
                completed.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        }

        let finish = pool.finish();
        tokio::pin!(finish);
        assert!(finish.as_mut().now_or_never().is_none());
        timeout(Duration::from_secs(5), starts.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(finish.as_mut().now_or_never().is_none());

        gate.add_permits(1);
        timeout(Duration::from_secs(5), starts.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(finish.as_mut().now_or_never().is_none());

        gate.add_permits(1);
        timeout(Duration::from_secs(5), finish).await.unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn finish_without_tasks() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(1)
            .build()
            .unwrap();
        let pool = TrackingPool::<BoxFuture<'static, ()>>::new(pool);

        timeout(Duration::from_secs(5), pool.finish())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn releases_permits_on_panic() {
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
            let pool = TrackingPool::<BoxFuture<'static, ()>>::new(builder.build().unwrap());
            let (trigger, panic) = tokio::sync::oneshot::channel();
            pool.spawn_async(Box::pin(async move {
                panic.await.unwrap();
                panic!("test task panic");
            }))
            .await;
            pool.spawn_async(Box::pin(async {})).await;

            let finish = pool.finish();
            tokio::pin!(finish);
            assert!(finish.as_mut().now_or_never().is_none());
            trigger.send(()).unwrap();
            timeout(Duration::from_secs(5), finish).await.unwrap();
        }
    }
}
