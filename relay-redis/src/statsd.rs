use relay_statsd::TimerMetric;

pub enum RedisTimers {
    /// The time from sending a Redis command or pipeline to receiving
    /// a response.
    ///
    /// This metric is tagged with:
    /// - `result`: The outcome (`ok`, `error`, `timeout`).
    /// - `client`: The name of the Redis client sending the command.
    /// - `cmd`: The name of the command that was sent (or `"pipeline"` for pipelines).
    CommandExecuted,
}

impl TimerMetric for RedisTimers {
    fn name(&self) -> &'static str {
        match self {
            Self::CommandExecuted => "redis.command_executed",
        }
    }
}
