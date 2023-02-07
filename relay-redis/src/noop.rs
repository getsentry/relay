use thiserror::Error;

use crate::config::RedisConfig;

/// This is an unconstructable type to make `Option<RedisPool>` zero-sized.
#[derive(Clone, Debug)]
pub struct RedisPool;

/// An error returned from `RedisPool`.
#[derive(Debug, Error)]
#[error("unreachable")]
pub struct RedisError;

impl RedisPool {
    /// Creates a `RedisPool` from the given configuration.
    ///
    /// Always returns `Ok(None)`.
    pub fn new(_config: &RedisConfig) -> Result<Self, RedisError> {
        Ok(Self)
    }
}
