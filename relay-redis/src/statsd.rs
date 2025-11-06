use relay_statsd::CounterMetric;

pub enum RedisCounters {
    /// Incremented every time a Redis command or pipeline is run.
    ///
    /// This metric is tagged with:
    /// - `result`: The outcome (`ok`, `error`, `timeout`).
    /// - `client`: The name of the Redis client sending the command.
    CommandExecuted,
}

impl CounterMetric for RedisCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::CommandExecuted => "redis.command_executed",
        }
    }
}
