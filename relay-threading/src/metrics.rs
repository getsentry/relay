use relay_statsd::GaugeMetric;

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
