use relay_statsd::TimerMetric;

pub enum CardinalityLimiterTimers {
    /// Timer for the entire process of checking cardinality limits.
    CardinalityLimiter,
    /// Timer for the duration of the Redis call.
    ///
    /// This metric is tagged with:
    ///  - `id`: The id of the enforced limit.
    ///  - `scopes`: The amount of scopes checked with a single Redis call.
    #[cfg(feature = "redis")]
    Redis,
    /// Timer tracking the amount of time spent removing expired values
    /// from the cardinality cache.
    #[cfg(feature = "redis")]
    CacheVacuum,
}

impl TimerMetric for CardinalityLimiterTimers {
    fn name(&self) -> &'static str {
        match self {
            CardinalityLimiterTimers::CardinalityLimiter => "cardinality.limiter.duration",
            #[cfg(feature = "redis")]
            CardinalityLimiterTimers::Redis => "cardinality.limiter.redis.duration",
            #[cfg(feature = "redis")]
            CardinalityLimiterTimers::CacheVacuum => {
                "cardinality.limiter.redis.cache_vacuum.duration"
            }
        }
    }
}
