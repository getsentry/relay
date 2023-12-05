use relay_statsd::CounterMetric;

/// Counter metrics for the Relay Cardinality Limiter.
pub enum CardinalityLimiterCounters {
    /// Incremented for every rejected item by the cardinality limiter.
    ///
    /// This metric is tagged with:
    ///  - `namespace`: The namespace of the metric.
    #[cfg(feature = "redis")]
    Rejected,
    /// Incremented for every loaded Redis set.
    ///
    /// This metric is tagged with:
    ///  - `namespace`: The namespace of the metric.
    #[cfg(feature = "redis")]
    RedisRead,
    /// Incremented for every hash requested to be cardinality checked.
    ///
    /// This metric is tagged with:
    ///  - `namespace`: The namespace of the metric.
    #[cfg(feature = "redis")]
    RedisHashCheck,
    /// Incremented for every hash added to the internal Redis sets.
    ///
    /// Note: there are multiple Redis commands executed for every update.
    ///
    /// This metric is tagged with:
    ///  - `namespace`: The namespace of the metric.
    #[cfg(feature = "redis")]
    RedisHashUpdate,
}

impl CounterMetric for CardinalityLimiterCounters {
    fn name(&self) -> &'static str {
        match *self {
            #[cfg(feature = "redis")]
            Self::Rejected => "cardinality.limiter.rejected",
            #[cfg(feature = "redis")]
            Self::RedisRead => "cardinality.limiter.redis.read",
            #[cfg(feature = "redis")]
            Self::RedisHashCheck => "cardinality.limiter.redis.hash.check",
            #[cfg(feature = "redis")]
            Self::RedisHashUpdate => "cardinality.limiter.redis.hash.update",
        }
    }
}
