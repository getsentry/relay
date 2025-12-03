use relay_statsd::{CounterMetric, TimerMetric};

pub enum QuotaTimers {
    /// Timer for the duration of the cache vacuum.
    CacheVacuumDuration,
}

impl TimerMetric for QuotaTimers {
    fn name(&self) -> &'static str {
        match self {
            Self::CacheVacuumDuration => "quota.cache.vacuum.duration",
        }
    }
}

pub enum QuotaCounters {
    /// Metric collecting the total error of the cache.
    ///
    /// The error is the total quantity the cache accepted over the actual quota limit.
    ///
    /// This metric is tagged with:
    ///  - `category`: The item category being rate limited.
    CacheError,
}

impl CounterMetric for QuotaCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::CacheError => "quota.cache.error",
        }
    }
}
