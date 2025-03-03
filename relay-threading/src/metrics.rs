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

/// Inner state of asynchronous pool metrics.
///
/// This struct is not intended for direct use by end users. It is wrapped by [`AsyncPoolMetrics`]
/// to provide a safe, ergonomic interface for tracking pool performance.
#[derive(Debug)]
struct Inner {
    /// The name of the pool from which the metrics originate.
    pool_name: &'static str,
    /// The maximum number of futures that are expected to run concurrently at any point in time.
    max_expected_futures: u64,
    /// Number of futures waiting to be executed by one of the threads of the pool.
    queue_size: AtomicU64,
    /// Vector containing all the metrics collected individually in each thread.
    thread_metrics: Vec<ThreadMetrics>,
}

/// Metrics for an asynchronous pool.
///
/// This struct provides a high-level interface for monitoring the performance and utilization of
/// an asynchronous pool. It tracks queued futures, per-thread activity, and overall utilization.
///
/// The metrics are stored in a thread-safe manner, allowing safe access from multiple threads.
///
/// Cloning this struct is inexpensive and shares the underlying data.
#[derive(Debug, Clone)]
pub struct AsyncPoolMetrics(Arc<Inner>);

impl AsyncPoolMetrics {
    /// Create a new instance [`AsyncPoolMetrics`].
    pub(crate) fn new(pool_name: &'static str, num_threads: usize, max_concurrency: usize) -> Self {
        let inner = Inner {
            pool_name,
            max_expected_futures: (num_threads * max_concurrency) as u64,
            queue_size: AtomicU64::new(0),
            thread_metrics: vec![ThreadMetrics::default(); num_threads],
        };

        Self(Arc::new(inner))
    }

    /// Returns the name of the pool that emits the metrics captured by the [`AsyncPoolMetrics`].
    pub fn pool_name(&self) -> &'static str {
        self.0.pool_name
    }

    /// Returns the number of futures currently queued for execution.
    ///
    /// This method provides a snapshot of the pool's backlog, which can help identify whether the
    /// pool is overloaded or underutilized. A high value may indicate that the pool needs more
    /// threads or higher concurrency limits.
    pub fn queue_size(&self) -> u64 {
        self.0.queue_size.load(Ordering::SeqCst)
    }

    /// Returns the utilization of the pool as a fraction between 0.0 and 1.0.
    ///
    /// Utilization represents how close the pool is to its maximum capacity, based on the number
    /// of futures being polled across all threads compared to the expected maximum. A value near
    /// 1.0 indicates full utilization, while a value near 0.0 suggests idle resources.
    pub fn utilization(&self) -> f32 {
        let total_polled_futures: u64 = self
            .0
            .thread_metrics
            .iter()
            .map(|m| m.polled_futures())
            .sum();

        (total_polled_futures as f32 / self.0.max_expected_futures as f32).clamp(0.0, 1.0)
    }

    /// Updates the `queued_futures` value.
    pub(crate) fn update_queued_futures(&self, queued_futures: u64) {
        self.0.queue_size.store(queued_futures, Ordering::SeqCst);
    }

    /// Returns the [`ThreadMetrics`] of the given thread, identified by thread id.
    ///
    /// If no thread exists with that id, `None` will be returned.
    pub(crate) fn thread_metrics(&self, thread_id: usize) -> Option<&ThreadMetrics> {
        self.0.thread_metrics.get(thread_id)
    }
}
