use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::Service;
use crate::service::simple::SimpleService;
use crate::statsd::SystemGauges;

/// A service that handles messages concurrently.
///
/// When the service reaches its maximum concurrency, it either drops messages
/// or keeps them in the input queue.
///
/// ```
/// use relay_system::{Interface, SimpleService, LoadShed, ConcurrentService};
///
/// #[derive(Clone)]
/// struct MyService;
///
/// struct MyMessage;
/// impl Interface for MyMessage {}
///
/// impl SimpleService for MyService {
///     type Interface = MyMessage;
///     async fn handle_message(&self, message: MyMessage) {
///         // do your thing
///     }
/// }
///
/// // `Loadshed` implementation is required but can be empty.
/// impl LoadShed<MyMessage> for MyService {
///     fn handle_loadshed(&self, _: MyMessage) {
///         eprintln!("Dropped a message!");
///     }
/// }
///
/// let concurrent_service = ConcurrentService::new(MyService).with_concurrency_limit(5);
/// ```
pub struct ConcurrentService<S>
where
    S: SimpleService + Clone + Send + Sync,
{
    inner: S,
    max_concurrency: usize,
    max_backlog: usize,
    pending: FuturesUnordered<BoxFuture<'static, ()>>,
}

impl<S> ConcurrentService<S>
where
    S: SimpleService + Clone + Send + Sync,
{
    /// Creates a new concurrent service from a [`SimpleService`].
    ///
    /// The default strategy for congestion control is to keep messages in the input queue.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            max_concurrency: usize::MAX,
            max_backlog: usize::MAX,
            pending: FuturesUnordered::new(),
        }
    }

    /// Sets the maximum number of messages that can be handled concurrently.
    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.max_concurrency = limit;
        self
    }

    /// Limits the amount of messages that wait in the queue by loadshedding.
    ///
    /// Setting this limit will cause message loss.
    ///
    /// Note that cleanup of the queue may be deferred until the next pending
    /// future completes.
    pub fn with_backlog_limit(mut self, limit: usize) -> Self {
        self.max_backlog = limit;
        self
    }
}

impl<S> Service for ConcurrentService<S>
where
    S: SimpleService + LoadShed<S::Interface> + Clone + Send + Sync + 'static,
{
    type Interface = S::Interface;

    async fn run(mut self, mut rx: super::Receiver<Self::Interface>) {
        loop {
            relay_log::trace!("Concurrent service loop iteration");

            let has_capacity = self.pending.len() < self.max_concurrency;
            let should_consume = has_capacity || {
                let backlog = rx.queue_size.load(std::sync::atomic::Ordering::Relaxed);
                backlog > self.max_backlog as u64
            };

            tokio::select! {
                // Bias towards handling responses so that there's space for new incoming requests.
                biased;

                Some(_) = self.pending.next() => {},
                Some(message) = rx.recv(), if should_consume => {
                    if has_capacity {
                        let inner = self.inner.clone();
                        self.pending
                            .push(async move { inner.handle_message(message).await }.boxed());
                    } else {
                        self.inner.handle_loadshed(message);
                    }
                },
                else => break,
            }

            relay_statsd::metric!(
                gauge(SystemGauges::ServiceConcurrency) = self.pending.len() as u64,
                service = Self::name()
            );
        }
    }
}

/// A trait describing what to do with a message that was load-shed.
pub trait LoadShed<T> {
    /// Gets called for every message that gets dropped by loadshedding.
    fn handle_loadshed(&self, _message: T);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::{FromMessage, Interface, NoResponse};

    use super::*;

    #[derive(Clone)]
    struct CountingService {
        success: Arc<AtomicUsize>,
        fail: Arc<AtomicUsize>,
    }
    struct Incr;
    impl Interface for Incr {}
    impl FromMessage<()> for Incr {
        type Response = NoResponse;
        fn from_message(_message: (), _sender: ()) -> Self {
            Self
        }
    }
    impl SimpleService for CountingService {
        type Interface = Incr;

        async fn handle_message(&self, _message: Incr) {
            tokio::time::sleep(Duration::from_secs(2)).await;
            self.success.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl LoadShed<Incr> for CountingService {
        fn handle_loadshed(&self, _message: Incr) {
            self.fail.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn loadshed() {
        let inner = CountingService {
            success: Arc::new(AtomicUsize::new(0)),
            fail: Arc::new(AtomicUsize::new(0)),
        };
        let service = ConcurrentService::new(inner.clone())
            .with_concurrency_limit(5)
            .with_backlog_limit(0);
        let addr = service.start_detached();

        for _ in 0..10 {
            addr.send(());
        }

        assert_eq!(inner.success.load(Ordering::Relaxed), 0);
        assert_eq!(inner.fail.load(Ordering::Relaxed), 0);

        tokio::time::sleep(Duration::from_secs(10)).await;

        // Only half of the items have been processed
        assert_eq!(inner.success.load(Ordering::Relaxed), 5);
        assert_eq!(inner.fail.load(Ordering::Relaxed), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure() {
        let inner = CountingService {
            success: Arc::new(AtomicUsize::new(0)),
            fail: Arc::new(AtomicUsize::new(0)),
        };
        let service = ConcurrentService::new(inner.clone()).with_concurrency_limit(5);
        let addr = service.start_detached();

        for _ in 0..10 {
            addr.send(());
        }

        // After 3 seconds, only half the messages have been handled:
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(inner.success.load(Ordering::Relaxed), 5);
        assert_eq!(inner.fail.load(Ordering::Relaxed), 0);

        // After 5 seconds, everything's been handled:
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(inner.success.load(Ordering::Relaxed), 10);
        assert_eq!(inner.fail.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure_and_loadshed() {
        let inner = CountingService {
            success: Arc::new(AtomicUsize::new(0)),
            fail: Arc::new(AtomicUsize::new(0)),
        };
        let service = ConcurrentService::new(inner.clone())
            .with_concurrency_limit(5)
            .with_backlog_limit(5);
        let addr = service.start_detached();

        for _ in 0..13 {
            addr.send(());
        }

        // After 3 seconds, only 5 messages have been handled.
        // Three have been dropped due to loadshedding.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(inner.success.load(Ordering::Relaxed), 5);
        assert_eq!(inner.fail.load(Ordering::Relaxed), 3);

        // After 5 seconds, another 5 messages got handled:
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(inner.success.load(Ordering::Relaxed), 10);
        assert_eq!(inner.fail.load(Ordering::Relaxed), 3);
    }
}
