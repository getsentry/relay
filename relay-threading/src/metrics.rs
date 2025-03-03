use relay_statsd::GaugeMetric;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Gauge metrics emitted by the asynchronous pool.
pub enum AsyncPoolGauges {
    /// Number of futures queued up for execution in the asynchronous pool.
    AsyncPoolQueueSize,
    /// Number of futures being driven in each thread of the asynchronous pool.
    AsyncPoolFuturesPerThread,
}

impl GaugeMetric for AsyncPoolGauges {
    fn name(&self) -> &'static str {
        match self {
            Self::AsyncPoolQueueSize => "async_pool.queue_size",
            Self::AsyncPoolFuturesPerThread => "async_pool.futures_per_thread",
        }
    }
}

#[derive(Debug, Default)]
pub struct ThreadMetrics {
    polled_futures: AtomicU64,
}

impl ThreadMetrics {
    pub fn polled_futures(&self) -> u64 {
        self.polled_futures.load(Ordering::SeqCst)
    }

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

#[derive(Debug)]
pub struct Inner {
    /// The number of futures we can poll within each thread times the number of threads.
    ///
    /// This value is used to compute the utilization metric, which should be as close as possible
    /// to 100%, meaning all threads are busy to their maximum.
    max_expected_futures: u64,
    queued_futures: AtomicU64,
    thread_metrics: Vec<ThreadMetrics>,
}

#[derive(Debug, Clone)]
pub struct AsyncPoolMetrics(Arc<Inner>);

impl AsyncPoolMetrics {
    pub(crate) fn new(num_threads: usize, max_concurrency: usize) -> Self {
        let inner = Inner {
            max_expected_futures: (num_threads * max_concurrency) as u64,
            queued_futures: AtomicU64::new(0),
            thread_metrics: vec![ThreadMetrics::default(); num_threads],
        };

        Self(Arc::new(inner))
    }

    pub fn queued_futures(&self) -> u64 {
        self.0.queued_futures.load(Ordering::SeqCst)
    }

    pub fn utilization(&self) -> f32 {
        let total_polled_futures: u64 = self
            .0
            .thread_metrics
            .iter()
            .map(|m| m.polled_futures())
            .sum();
        total_polled_futures as f32 / self.0.max_expected_futures as f32
    }

    pub(crate) fn update_queued_futures(&self, queued_futures: u64) {
        self.0
            .queued_futures
            .store(queued_futures, Ordering::SeqCst);
    }

    pub(crate) fn thread_metrics(&self, thread_id: usize) -> Option<&ThreadMetrics> {
        self.0.thread_metrics.get(thread_id)
    }
}
