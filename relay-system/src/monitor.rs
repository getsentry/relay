use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::time::{Duration, Instant};

/// Minimum interval when utilization is recalculated.
const UTILIZATION_UPDATE_THRESHOLD: Duration = Duration::from_secs(5);

pub trait TimedFutureReceiver {
    fn increment_poll_count(&self, count: u64) -> u64;

    fn increment_total_duration_ns(&self, duration: u64) -> u64;

    fn update_utilization(&self, utilization: u8) -> u8;
}

pin_project_lite::pin_project! {
    /// A future that tracks metrics.
    pub struct TimedFuture<F, R> {
        #[pin]
        inner: F,
        receiver: R,
        last_utilization_update: Instant,
        last_utilization_duration_ns: u64,
    }
}

impl<F, R> TimedFuture<F, R> {
    /// Wraps a future with the [`TimedFuture`].
    pub fn wrap(inner: F, receiver: R) -> Self {
        Self {
            inner,
            receiver,
            last_utilization_update: Instant::now(),
            last_utilization_duration_ns: 0,
        }
    }
}

impl<F, R: TimedFutureReceiver> Future for TimedFuture<F, R>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll_start = Instant::now();

        let this = self.project();
        this.receiver.increment_poll_count(1);

        let ret = this.inner.poll(cx);

        let poll_end = Instant::now();
        let poll_duration = poll_end - poll_start;
        let poll_duration_ns = poll_duration.as_nanos().try_into().unwrap_or(u64::MAX);

        let previous_total_duration = this.receiver.increment_total_duration_ns(poll_duration_ns);
        let total_duration_ns = previous_total_duration + poll_duration_ns;

        let utilization_duration = poll_end - *this.last_utilization_update;
        if utilization_duration >= UTILIZATION_UPDATE_THRESHOLD {
            // Time spent the future was busy since the last utilization calculation.
            let busy = total_duration_ns - *this.last_utilization_duration_ns;

            // The maximum possible time spent busy is the total time between the last measurement
            // and the current measurement. We can extract a percentage from this.
            let percentage = (busy * 100).div_ceil(utilization_duration.as_nanos().max(1) as u64);
            this.receiver.update_utilization(percentage.min(100) as u8);

            *this.last_utilization_duration_ns = total_duration_ns;
            *this.last_utilization_update = poll_end;
        }

        ret
    }
}

/// The raw metrics extracted from a [`TimedFuture`].
///
/// All access outside the [`TimedFuture`] must be *read* only.
#[derive(Debug)]
pub struct RawMetrics {
    /// Amount of times the future was polled.
    pub poll_count: AtomicU64,
    /// The total time the future spent in its poll function.
    pub total_duration_ns: AtomicU64,
    /// Estimated utilization percentage `[0-100]` as a function of time spent doing busy work
    /// vs. the time range of the measurement.
    pub utilization: AtomicU8,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Inner {
        poll_count: AtomicU64,
        total_duration_ns: AtomicU64,
        utilization: AtomicU8,
    }

    #[derive(Clone)]
    struct Metrics(Arc<Inner>);

    impl Metrics {
        fn new() -> Self {
            Self(Arc::new(Inner {
                poll_count: AtomicU64::new(0),
                total_duration_ns: AtomicU64::new(0),
                utilization: AtomicU8::new(0),
            }))
        }
    }

    impl TimedFutureReceiver for Metrics {
        fn increment_poll_count(&self, count: u64) -> u64 {
            self.0.poll_count.fetch_add(count, Ordering::Relaxed)
        }

        fn increment_total_duration_ns(&self, duration: u64) -> u64 {
            self.0
                .total_duration_ns
                .fetch_add(duration, Ordering::Relaxed)
        }

        fn update_utilization(&self, utilization: u8) -> u8 {
            let previous_utilization = self.0.utilization.load(Ordering::Relaxed);
            self.0.utilization.store(utilization, Ordering::Relaxed);
            previous_utilization
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_monitor() {
        let metrics = Metrics::new();
        let mut monitor = TimedFuture::wrap(
            Box::pin(async {
                loop {
                    tokio::time::advance(Duration::from_millis(500)).await;
                }
            }),
            metrics.clone(),
        );

        assert_eq!(metrics.0.poll_count.load(Ordering::Relaxed), 0);
        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.0.poll_count.load(Ordering::Relaxed), 1);
        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.0.poll_count.load(Ordering::Relaxed), 2);
        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.0.poll_count.load(Ordering::Relaxed), 3);

        assert_eq!(metrics.0.utilization.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.0.total_duration_ns.load(Ordering::Relaxed),
            1500000000
        );

        // Advance time just enough to perfectly hit the update threshold.
        tokio::time::advance(UTILIZATION_UPDATE_THRESHOLD - Duration::from_secs(2)).await;

        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.0.poll_count.load(Ordering::Relaxed), 4);
        assert_eq!(metrics.0.utilization.load(Ordering::Relaxed), 40);
        assert_eq!(
            metrics.0.total_duration_ns.load(Ordering::Relaxed),
            2000000000
        );
    }
}
