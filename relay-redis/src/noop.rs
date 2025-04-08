use thiserror::Error;

use crate::config::RedisConfigOptions;

/// An error returned from [`RedisClient`].
#[derive(Debug, Error)]
#[error("unreachable")]
pub enum RedisError {}

/// The various [`RedisClient`]s used within Relay.
#[derive(Debug, Clone)]
pub struct RedisClients {
    /// The client used for project configurations
    pub project_configs: AsyncRedisClient,
    /// The client used for cardinality limits.
    pub cardinality: AsyncRedisClient,
    /// The client used for rate limiting/quotas.
    pub quotas: AsyncRedisClient,
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
