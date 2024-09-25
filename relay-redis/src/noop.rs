use thiserror::Error;

use crate::config::RedisConfigOptions;

/// This is an unconstructable type to make `Option<RedisPool>` zero-sized.
#[derive(Clone, Debug)]
pub struct RedisPool;

/// An error returned from `RedisPool`.
#[derive(Debug, Error)]
#[error("unreachable")]
pub enum RedisError {
    /// Failure to configure Redis.
    Configuration,
}

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

    /// Creates a [`RedisPool`] in multi write configuration.
    ///
    /// Always returns `Ok(Self)`.
    pub fn multi_write<'a>(
        _primary: &str,
        _secondaries: impl IntoIterator<Item = &'a str>,
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

/// The various [`RedisPool`]s used within Relay.
#[derive(Debug, Clone)]
pub struct RedisPools {
    /// The pool used for project configurations
    pub project_configs: RedisPool,
    /// The pool used for cardinality limits.
    pub cardinality: RedisPool,
    /// The pool used for rate limiting/quotas.
    pub quotas: RedisPool,
    /// The pool used for metrics metadata.
    pub misc: RedisPool,
}
