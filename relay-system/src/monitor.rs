use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::time::{Duration, Instant};

/// Minimum interval when utilization is recalculated.
const UTILIZATION_UPDATE_THRESHOLD: Duration = Duration::from_secs(5);

pin_project_lite::pin_project! {
    /// A future that tracks metrics.
    pub struct TimedFuture<F> {
        #[pin]
        inner: F,
        metrics: Arc<RawMetrics>,
        last_utilization_update: Instant,
        total_duration_ns: u64
    }
}

impl<F> TimedFuture<F> {
    /// Wraps a future with the [`TimedFuture`].
    pub fn wrap(inner: F) -> Self {
        Self {
            inner,
            metrics: Arc::new(RawMetrics::default()),
            last_utilization_update: Instant::now(),
            total_duration_ns: 0,
        }
    }

    /// Provides access to the raw metrics tracked in this monitor.
    pub fn metrics(&self) -> &Arc<RawMetrics> {
        &self.metrics
    }
}

impl<F> Future for TimedFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll_start = Instant::now();

        let this = self.project();
        this.metrics.poll_count.fetch_add(1, Ordering::Relaxed);

        let ret = this.inner.poll(cx);

        let poll_end = Instant::now();
        let poll_duration = poll_end - poll_start;
        let poll_duration_ns = poll_duration.as_nanos().try_into().unwrap_or(u64::MAX);

        this.metrics
            .total_duration_ns
            .fetch_add(poll_duration_ns, Ordering::Relaxed);
        *this.total_duration_ns += poll_duration_ns;

        let utilization_duration = poll_end - *this.last_utilization_update;
        if utilization_duration >= UTILIZATION_UPDATE_THRESHOLD {
            // The maximum possible time spent busy is the total time between the last measurement
            // and the current measurement. We can extract a percentage from this.
            let percentage = (*this.total_duration_ns * 100)
                .div_ceil(utilization_duration.as_nanos().max(1) as u64);
            this.metrics
                .utilization
                .store(percentage.min(100) as u8, Ordering::Relaxed);

            *this.total_duration_ns = 0;
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
    /// Amount of times the service was polled.
    pub poll_count: AtomicU64,
    /// The total time the service spent in its poll function.
    pub total_duration_ns: AtomicU64,
    /// Estimated utilization percentage `[0-100]`
    pub utilization: AtomicU8,
}

impl Default for RawMetrics {
    fn default() -> Self {
        Self {
            poll_count: AtomicU64::new(0),
            total_duration_ns: AtomicU64::new(0),
            utilization: AtomicU8::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_monitor() {
        let mut monitor = TimedFuture::wrap(Box::pin(async {
            loop {
                tokio::time::advance(Duration::from_millis(500)).await;
            }
        }));
        let metrics = Arc::clone(monitor.metrics());

        assert_eq!(metrics.poll_count.load(Ordering::Relaxed), 0);
        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.poll_count.load(Ordering::Relaxed), 1);
        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.poll_count.load(Ordering::Relaxed), 2);
        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.poll_count.load(Ordering::Relaxed), 3);

        assert_eq!(metrics.utilization.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.total_duration_ns.load(Ordering::Relaxed),
            1500000000
        );

        // Advance time just enough to perfectly hit the update threshold.
        tokio::time::advance(UTILIZATION_UPDATE_THRESHOLD - Duration::from_secs(2)).await;

        let _ = futures::poll!(&mut monitor);
        assert_eq!(metrics.poll_count.load(Ordering::Relaxed), 4);
        assert_eq!(metrics.utilization.load(Ordering::Relaxed), 40);
        assert_eq!(
            metrics.total_duration_ns.load(Ordering::Relaxed),
            2000000000
        );
    }
}
