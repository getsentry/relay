use thiserror::Error;

use crate::config::RedisConfigOptions;

/// This is an unconstructable type to make `Option<RedisPool>` zero-sized.
#[derive(Clone, Debug)]
pub struct RedisPool;

/// An error returned from `RedisPool`.
#[derive(Debug, Error)]
#[error("unreachable")]
pub struct RedisError;

impl RedisPool {
    /// Creates a `RedisPool` in cluster configuration.
    ///
    /// Always returns `Ok(Self)`.
    pub fn cluster<'a>(
        _servers: impl IntoIterator<Item = &'a str>,
        _opts: RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        Ok(Self)
    }

    /// Creates a `RedisPool` in single-node configuration.
    ///
    /// Always returns `Ok(Self)`.
    pub fn single(_server: &str, _opts: RedisConfigOptions) -> Result<Self, RedisError> {
        Ok(Self)
    }
}
