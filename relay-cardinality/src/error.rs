use relay_redis::RedisError;

/// Result type for the cardinality module, using [`Error`] as the default error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error for the cardinality module.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Something went wrong with Redis.
    #[error("failed to talk to redis: {0}")]
    RedisError(#[from] RedisError),
}
