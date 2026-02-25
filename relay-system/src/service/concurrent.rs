use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::Service;
use crate::service::simple::SimpleService;

#[derive(Debug, Clone, Copy)]
enum CongestionStrategy {
    Backpressure,
    LoadShed,
}

impl CongestionStrategy {
    fn is_loadshed(&self) -> bool {
        matches!(self, Self::LoadShed)
    }
}

struct ConcurrentService<S>
where
    S: SimpleService + Clone + Send + Sync,
{
    inner: S,
    congestion_strategy: CongestionStrategy,
    max_concurrency: usize,
    pending: FuturesUnordered<BoxFuture<'static, ()>>,
}

impl<S> ConcurrentService<S>
where
    S: SimpleService + Clone + Send + Sync,
{
    fn new(inner: S, congestion_strategy: CongestionStrategy, max_concurrency: usize) -> Self {
        Self {
            inner,
            congestion_strategy,
            max_concurrency,
            pending: FuturesUnordered::new(),
        }
    }
}

impl<S> Service for ConcurrentService<S>
where
    S: SimpleService + Clone + Send + Sync + 'static,
{
    type Interface = S::Interface;

    async fn run(mut self, mut rx: super::Receiver<Self::Interface>) {
        loop {
            // if self.pending.len() >= self.max_concurrency {
            //     match self.congestion_strategy {
            //         CongestionStrategy::Backpressure => {
            //             // just wait for capacity.
            //         }
            //         CongestionStrategy::LoadShed => {
            //             while let Some(x) = rx.recv()
            //         },
            //     }
            //     // Wait for capacity:
            //     let _ = self.pending.next().await;
            // }
            // TODO: self.name()
            relay_log::trace!("Concurrent service loop iteration");

            let has_capacity = self.pending.len() < self.max_concurrency;
            let should_consume = has_capacity || self.congestion_strategy.is_loadshed();

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
                        relay_log::error!("Dropping");
                    }
                },
            //     // TODO: what if pending.next returns None?
                else => break,
            }
        }
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

    #[tokio::test(start_paused = true)]
    async fn loadshed() {
        let inner = CountingService(Arc::new(AtomicUsize::new(0)));
        let service = ConcurrentService::new(inner.clone(), CongestionStrategy::LoadShed, 5);
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
        let service = ConcurrentService::new(inner.clone(), CongestionStrategy::Backpressure, 5);
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
