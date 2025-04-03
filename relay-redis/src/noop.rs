use thiserror::Error;

use crate::config::RedisConfigOptions;

/// This is an unconstructable type to make [`Option<RedisClient>`] zero-sized.
#[derive(Clone, Debug)]
pub struct RedisClient;

/// An error returned from [`RedisClient`].
#[derive(Debug, Error)]
#[error("unreachable")]
pub enum RedisError {
    /// Failure to configure Redis.
    Configuration,
}

impl RedisClient {
    /// Creates [`RedisClient`] in cluster configuration.
    ///
    /// Always returns [`Ok(Self)`].
    pub fn cluster<'a>(
        _servers: impl IntoIterator<Item = &'a str>,
        _opts: RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        Ok(Self)
    }

    /// Creates a [`RedisClient`] in multi write configuration.
    ///
    /// Always returns [`Ok(Self)`].
    pub fn multi_write(
        _primary: RedisClient,
        _secondaries: Vec<RedisClient>,
    ) -> Result<Self, RedisError> {
        Ok(Self)
    }

    /// Creates a [`RedisClient`] in single-node configuration.
    ///
    /// Always returns [`Ok(Self)`].
    pub fn single(_server: &str, _opts: RedisConfigOptions) -> Result<Self, RedisError> {
        Ok(Self)
    }
}

/// The various [`RedisClient`]s used within Relay.
#[derive(Debug, Clone)]
pub struct RedisClients {
    /// The client used for project configurations
    pub project_configs: RedisClient,
    /// The client used for cardinality limits.
    pub cardinality: RedisClient,
    /// The client used for rate limiting/quotas.
    pub quotas: RedisClient,
}

/// Noop type of the [`AsyncRedisClient`].
#[derive(Debug, Clone)]
pub struct AsyncRedisClient;

impl AsyncRedisClient {
    /// Creates a [`AsyncRedisClient`] in cluster configration.
    ///
    /// Always returns `Ok(Self)`
    pub async fn cluster<'a>(
        _servers: impl IntoIterator<Item = &'a str>,
        _opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        Ok(Self)
    }

    /// Creates a [`AsyncRedisClient`] in single-node configuration.
    ///
    /// Always returns `Ok(Self)`
    pub async fn single(_server: &str, _opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        Ok(Self)
    }
}
