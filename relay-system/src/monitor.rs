use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::time::{Duration, Instant};

/// Minimum interval when utilization is recalculated.
const UTILIZATION_UPDATE_THRESHOLD: Duration = Duration::from_secs(5);

/// A trait that defines a receiver that receives events emitted from a [`TimedFuture`].
///
/// This trait is designed to allow any external consumer to hook into the lifecycle of a
/// [`TimedFuture`].
///
/// An example usage could be to collect metrics, as implemented in [`RawMetrics`].
pub trait TimedFutureReceiver {
    /// Called before a poll is being performed by the [`TimedFuture`].
    fn before_poll(&self) {}

    /// Called after a poll was performed by the [`TimedFuture`].
    fn after_poll(&self, _poll_duration: u64) {}

    /// Called when the [`TimedFuture`] reaches the internal threshold for additional data.
    fn on_threshold_reached(&self, _utilization: u8) {}
}

pin_project_lite::pin_project! {
    /// A future that tracks metrics.
    pub struct TimedFuture<F, R> {
        #[pin]
        inner: F,
        receiver: R,
        last_utilization_update: Instant,
        total_duration_ns: u64
    }
}

impl<F, R> TimedFuture<F, R> {
    /// Wraps a future with the [`TimedFuture`].
    pub fn wrap(inner: F, receiver: R) -> Self {
        Self {
            inner,
            receiver,
            last_utilization_update: Instant::now(),
            total_duration_ns: 0,
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
        this.receiver.before_poll();

        let ret = this.inner.poll(cx);

        let poll_end = Instant::now();
        let poll_duration = poll_end - poll_start;
        let poll_duration_ns = poll_duration.as_nanos().try_into().unwrap_or(u64::MAX);

        this.receiver.after_poll(poll_duration_ns);
        *this.total_duration_ns += poll_duration_ns;

        let utilization_duration = poll_end - *this.last_utilization_update;
        if utilization_duration >= UTILIZATION_UPDATE_THRESHOLD {
            // The maximum possible time spent busy is the total time between the last measurement
            // and the current measurement. We can extract a percentage from this.
            let percentage = (*this.total_duration_ns * 100)
                .div_ceil(utilization_duration.as_nanos().max(1) as u64);
            this.receiver
                .on_threshold_reached(percentage.min(100) as u8);

            *this.total_duration_ns = 0;
            *this.last_utilization_update = poll_end;
        }

        ret
    }
}

#[derive(Debug)]
struct Inner {
    /// Amount of times the future was polled.
    poll_count: AtomicU64,
    /// The total time the future spent in its poll function.
    total_duration_ns: AtomicU64,
    /// Estimated utilization percentage `[0-100]` as a function of time spent doing busy work
    /// vs. the time range of the measurement.
    utilization: AtomicU8,
}

/// The raw metrics extracted from a [`TimedFuture`].
///
/// All access outside the [`TimedFuture`] must be *read* only.
#[derive(Debug, Clone)]
pub struct RawMetrics(Arc<Inner>);

impl RawMetrics {
    /// Returns the total number of times the future was polled.
    pub fn poll_count(&self) -> u64 {
        self.0.poll_count.load(Ordering::Relaxed)
    }

    /// Returns the total duration in nanoseconds in which the future was being polled.
    pub fn total_duration_ns(&self) -> u64 {
        self.0.total_duration_ns.load(Ordering::Relaxed)
    }

    /// Returns the estimated utilization of the future which is defined as the time spent doing
    /// work versus not doing work within the measurement range.
    pub fn utilization(&self) -> u8 {
        self.0.utilization.load(Ordering::Relaxed)
    }
}

impl TimedFutureReceiver for RawMetrics {
    fn before_poll(&self) {
        self.0.poll_count.fetch_add(1, Ordering::Relaxed);
    }

    fn after_poll(&self, poll_duration: u64) {
        self.0
            .total_duration_ns
            .fetch_add(poll_duration, Ordering::Relaxed);
    }

    fn on_threshold_reached(&self, utilization: u8) {
        self.0.utilization.store(utilization, Ordering::Relaxed);
    }
}

impl Default for RawMetrics {
    fn default() -> Self {
        Self(Arc::new(Inner {
            poll_count: AtomicU64::new(0),
            total_duration_ns: AtomicU64::new(0),
            utilization: AtomicU8::new(0),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_monitor() {
        let metrics = RawMetrics::default();
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
