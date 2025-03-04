use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Metrics for a single thread in an asynchronous pool.
///
/// This struct provides a way to track and query metrics specific to an individual thread, such as
/// the number of futures it has polled. It is designed for safe concurrent access across threads.
#[derive(Debug, Default)]
pub struct ThreadMetrics {
    /// Number of futures that are currently being polled concurrently by the thread in the pool.
    polled_futures: AtomicU64,
}

impl ThreadMetrics {
    /// Creates a new instance of [`ThreadMetrics`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of futures polled by the thread.
    ///
    /// This method provides a snapshot of the total futures processed by the thread at the time of
    /// the call. It is useful for monitoring thread activity and workload distribution.
    pub fn polled_futures(&self) -> u64 {
        self.polled_futures.load(Ordering::SeqCst)
    }

    /// Updates the `polled_futures` value.
    pub(crate) fn update_polled_futures(&self, polled_futures: u64) {
        self.polled_futures.store(polled_futures, Ordering::SeqCst);
    }
}

impl Clone for ThreadMetrics {
    fn clone(&self) -> Self {
        Self {
            polled_futures: AtomicU64::new(self.polled_futures.load(Ordering::SeqCst)),
        }
    }
}

/// Metrics for the asynchronous pool.
#[derive(Debug)]
pub struct AsyncPoolMetrics<'a> {
    max_expected_futures: u64,
    queue_size: u64,
    threads_metrics: &'a Vec<Arc<ThreadMetrics>>,
}

impl<'a> AsyncPoolMetrics<'a> {
    /// Creates a new instance of [`AsyncPoolMetrics`].
    pub(crate) fn new(
        max_expected_futures: u64,
        queue_size: u64,
        threads_metrics: &'a Vec<Arc<ThreadMetrics>>,
    ) -> Self {
        Self {
            max_expected_futures,
            queue_size,
            threads_metrics,
        }
    }

    /// Returns the amount of futures in the pool's queue.
    pub fn queue_size(&self) -> u64 {
        self.queue_size
    }

    /// Returns the utilization metric for the pool.
    pub fn utilization(&self) -> f32 {
        let total_polled_futures: u64 = self
            .threads_metrics
            .iter()
            .map(|m| m.polled_futures())
            .sum();

        (total_polled_futures as f32 / self.max_expected_futures as f32).clamp(0.0, 1.0)
    }
}
