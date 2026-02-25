use std::usize;

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::Service;
use crate::service::simple::SimpleService;

/// A service that handles messages concurrently.
///
/// When the service reaches its maximum concurrency, it either drops messages
/// or keeps them in the input queue.
///
/// ```rust
/// struct MyService;
///
/// struct MyMessage;
/// impl Interface for MyMessage;
///
/// impl SimpleService for MyService {
///     type Interface = MyMessage;
///     async fn handle_message(message: MyMessage) {
///         // do your thing
///     }
/// }
///
/// // `Loadshed` implementation is required but can be empty.
/// impl LoadShed for MyService {}
///
/// let concurrent_service = ConcurrentService::new(MyService).with_concurrency_limit(5);
/// ```
pub struct ConcurrentService<S>
where
    S: SimpleService + Clone + Send + Sync,
{
    inner: S,
    congestion_control: CongestionControl,
    max_concurrency: usize,
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
            congestion_control: CongestionControl::Backpressure,
            max_concurrency: usize::MAX,
            pending: FuturesUnordered::new(),
        }
    }

    /// Sets the maximum number of messages that can be handled concurrently.
    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.max_concurrency = limit;
        self
    }

    /// Changes the congestion control strategy to load-shedding.
    ///
    /// This will drop messages.
    pub fn with_loadshedding(mut self) -> Self {
        self.congestion_control = CongestionControl::LoadShed;
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
            let should_consume = has_capacity || self.congestion_control.is_loadshed();

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
        }
    }
}

/// A trait describing what to do with a message that was load-shed.
///
/// The default implementation is to do nothing, so implementations may be empty,
/// especially when the congestion control mechanism is backpressure.
pub trait LoadShed<T> {
    /// Gets called for every message that gets dropped by loadshedding.
    fn handle_loadshed(&self, _message: T) {}
}

/// Represents whether the [`ConcurrentService`] should load-shed or apply
/// backpressure when it's maximum concurrency has been reached.
#[derive(Debug, Clone, Copy)]
pub enum CongestionControl {
    /// Leave incoming messages in the queue.
    Backpressure,
    /// Drop incoming messages if there is no capacity.
    LoadShed,
}

impl CongestionControl {
    fn is_loadshed(&self) -> bool {
        matches!(self, Self::LoadShed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::{FromMessage, Interface, NoResponse};

    use super::*;

    #[derive(Clone)]
    struct CountingService(Arc<AtomicUsize>);
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
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl LoadShed<Incr> for CountingService {}

    #[tokio::test(start_paused = true)]
    async fn loadshed() {
        let inner = CountingService(Arc::new(AtomicUsize::new(0)));
        let service = ConcurrentService::new(inner.clone())
            .with_concurrency_limit(5)
            .with_loadshedding();
        let addr = service.start_detached();

        for _ in 0..10 {
            addr.send(());
        }

        tokio::time::sleep(Duration::from_secs(10)).await;

        // Only half of the items have been processed
        assert_eq!(inner.0.load(Ordering::Relaxed), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure() {
        let inner = CountingService(Arc::new(AtomicUsize::new(0)));
        let service = ConcurrentService::new(inner.clone()).with_concurrency_limit(5);
        let addr = service.start_detached();

        for _ in 0..10 {
            addr.send(());
        }

        // After 3 seconds, only half the messages have been handled:
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(inner.0.load(Ordering::Relaxed), 5);

        // After 5 seconds, everything's been handled:
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(inner.0.load(Ordering::Relaxed), 10);
    }
}
