use std::sync::atomic::{AtomicU64, Ordering};

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

/// Metrics for an asynchronous pool.
#[derive(Debug)]
pub struct AsyncPoolMetrics {
    pub queue_size: u64,
    pub utilization: f32,
}
