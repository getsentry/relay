use relay_statsd::{CounterMetric, TimerMetric};

pub enum RedisCounters {
    /// Counter incremented every time a connection is created.
    ///
    /// This metric is tagged with:
    /// - `result`: Whether the connection was successfully created.
    /// - `client`: The name of the Redis client sending the command.
    CreateConnection,
    /// Counter incremented every time a connection was recycled due to an error.
    ///
    /// This metric is tagged with:
    /// - `client`: The name of the Redis client sending the command.
    RecycleConnection,
}

impl CounterMetric for RedisCounters {
    fn name(&self) -> &'static str {
        match self {
            Self::CreateConnection => "redis.connection.create",
            Self::RecycleConnection => "redis.connection.recycle",
        }
    }
}

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
