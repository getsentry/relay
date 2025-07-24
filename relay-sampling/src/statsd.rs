use relay_statsd::TimerMetric;

pub enum SamplingTimers {
    /// Amount of time it took to increment the Redis reservoir.
    RedisReservoir,
}

impl TimerMetric for SamplingTimers {
    fn name(&self) -> &'static str {
        match self {
            Self::RedisReservoir => "sampling.redis.reservoir",
        }
    }
}
