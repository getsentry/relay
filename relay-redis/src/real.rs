use deadpool::managed::BuildError;
use deadpool_redis::cluster::{
    Config as ClusterConfig, Connection as ClusterConnection, Pool as ClusterPool,
    PoolError as ClusterPoolError,
};
use deadpool_redis::redis::{Cmd, Pipeline, RedisFuture, Value};
use deadpool_redis::{
    Config as SingleConfig, ConfigError, Connection as SingleConnection, Pool as SinglePool,
    PoolError as SinglePoolError,
};
use std::fmt;
use std::fmt::{Debug, Formatter};
use thiserror::Error;

use crate::config::RedisConfigOptions;

pub use redis;

/// An error returned from `RedisPool`.
#[derive(Debug, Error)]
pub enum RedisError {
    /// Failure to configure Redis.
    #[error("failed to configure redis")]
    Configuration,

    /// Failure in r2d2 pool.
    #[error("failed to pool redis connection")]
    Pool(#[source] r2d2::Error),

    /// Failure in Redis communication.
    #[error("failed to communicate with redis")]
    Redis(#[source] redis::RedisError),

    #[error("failed to create redis pool: {0}")]
    CreatePool(#[from] BuildError),

    #[error("failed to configure redis: {0}")]
    ConfigError(#[from] ConfigError),

    #[error("failed")]
    SingleRedis(#[from] SinglePoolError),

    #[error("failed ")]
    ClusterRedis(#[from] ClusterPoolError),

    /// Multi write is not supported for the specified part.
    #[error("multi write is not supported for {0}")]
    MultiWriteNotSupported(&'static str),
}

/// The various [`AsyncRedisClient`]s used by Relay.
#[derive(Debug, Clone)]
pub struct RedisClients {
    /// The pool used for project configurations
    pub project_configs: AsyncRedisPool,
    /// The pool used for cardinality limits.
    pub cardinality: AsyncRedisPool,
    /// The pool used for rate limiting/quotas.
    pub quotas: AsyncRedisPool,
}

/// Stats about how the Redis client is performing.
#[derive(Debug)]
pub struct RedisClientStats {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
}

/// A wrapper type for async redis connections.
#[derive(Clone)]
pub enum AsyncRedisPool {
    /// Contains a connection pool to a redis cluster.
    Cluster(ClusterPool),
    /// Contains a connection pool to a single redis instance.
    Single(SinglePool),
}

impl AsyncRedisPool {
    // TODO:
    //  - Add all options to the builders.

    pub fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let servers = servers
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let builder = ClusterConfig::from_urls(servers)
            .builder()?
            .max_size(opts.max_connections as usize);
        let pool = builder.build()?;
        Ok(AsyncRedisPool::Cluster(pool))
    }

    pub fn single<'a>(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        let builder = SingleConfig::from_url(server)
            .builder()?
            .max_size(opts.max_connections as usize);
        let pool = builder.build()?;
        Ok(AsyncRedisPool::Single(pool))
    }

    pub async fn get_connection(&self) -> Result<AsyncRedisConnection, RedisError> {
        let connection = match self {
            Self::Cluster(pool) => AsyncRedisConnection::Cluster(pool.get().await?),
            Self::Single(pool) => AsyncRedisConnection::Single(pool.get().await?),
        };

        Ok(connection)
    }

    /// Return [`RedisClientStats`] for [`AsyncRedisClient`].
    ///
    /// It will always return 0 for `idle_connections` and 1 for `connections` since we
    /// are re-using the same connection.
    pub fn stats(&self) -> RedisClientStats {
        RedisClientStats {
            idle_connections: 0,
            connections: 1,
        }
    }
}

impl fmt::Debug for AsyncRedisPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AsyncRedisPool::Cluster(_) => write!(f, "AsyncRedisPool::Cluster"),
            AsyncRedisPool::Single(_) => write!(f, "AsyncRedisPool::Single"),
        }
    }
}

/// A wrapper type for async redis connections.
pub enum AsyncRedisConnection {
    Cluster(ClusterConnection),
    Single(SingleConnection),
}

impl AsyncRedisConnection {}

impl Debug for AsyncRedisConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Cluster(_) => "Cluster",
            Self::Single(_) => "Single",
        };
        f.debug_tuple(name).finish()
    }
}

impl deadpool_redis::redis::aio::ConnectionLike for AsyncRedisConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Self::Cluster(conn) => conn.req_packed_command(cmd),
            Self::Single(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Self::Cluster(conn) => conn.req_packed_commands(cmd, offset, count),
            Self::Single(conn) => conn.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Cluster(conn) => conn.get_db(),
            Self::Single(conn) => conn.get_db(),
        }
    }
}
