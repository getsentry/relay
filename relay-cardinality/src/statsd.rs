use relay_statsd::{CounterMetric, HistogramMetric, TimerMetric};

/// Counter metrics for the Relay Cardinality Limiter.
pub enum CardinalityLimiterCounters {
    /// Incremented for every rejected item by the cardinality limiter.
    ///
    /// This metric is tagged with:
    ///  - `scope`: The scope of check operation.
    #[cfg(feature = "redis")]
    Rejected,
    /// Incremented for every redis cardinality check.
    ///
    /// This metric is tagged with:
    ///  - `scope`: The scope of check operation.
    #[cfg(feature = "redis")]
    RedisRead,
    /// Incremented for every hash requested to be cardinality checked.
    ///
    /// This metric is tagged with:
    ///  - `scope`: The scope of check operation.
    #[cfg(feature = "redis")]
    RedisHashCheck,
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
        }
    }
}

pub enum CardinalityLimiterTimers {
    /// Timer for the entire process of checking cardinality limits.
    CardinalityLimiter,
}

impl TimerMetric for CardinalityLimiterTimers {
    fn name(&self) -> &'static str {
        match self {
            CardinalityLimiterTimers::CardinalityLimiter => "cardinality.limiter.duration",
        }
    }
}

pub enum CardinalityLimiterHistograms {
    /// Redis stored set cardinality.
    ///
    /// This metric is tagged with:
    ///  - `scope`: The scope of check operation.
    #[cfg(feature = "redis")]
    RedisSetCardinality,
}

impl HistogramMetric for CardinalityLimiterHistograms {
    fn name(&self) -> &'static str {
        match *self {
            #[cfg(feature = "redis")]
            Self::RedisSetCardinality => "cardinality.limiter.redis.set_cardinality",
        }
    }
}
