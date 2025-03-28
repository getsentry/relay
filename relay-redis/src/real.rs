use deadpool::managed::{BuildError, Pool, PoolError};
use deadpool_redis::redis::{Cmd, Pipeline, RedisFuture, Value};
use deadpool_redis::ConfigError;
use std::time::Duration;
use thiserror::Error;

use crate::config::RedisConfigOptions;
use crate::pool::{
    ClusterPool, CustomClusterConnection, CustomClusterManager, CustomSingleConnection,
    CustomSingleManager, SinglePool,
};

pub use deadpool_redis::redis;

/// An error type that represents various failure modes when interacting with Redis.
#[derive(Debug, Error)]
pub enum RedisError {
    /// An error that occurs during Redis configuration.
    #[error("failed to configure redis")]
    Configuration,

    /// An error that occurs during communication with Redis.
    #[error("failed to communicate with redis: {0}")]
    Redis(#[source] redis::RedisError),

    /// An error that occurs when interacting with the Redis connection pool.
    #[error("failed to interact with the redis pool: {0}")]
    Pool(#[source] PoolError<redis::RedisError>),

    /// An error that occurs when creating a Redis connection pool.
    #[error("failed to create redis pool: {0}")]
    CreatePool(#[from] BuildError),

    /// An error that occurs when configuring Redis.
    #[error("failed to configure redis: {0}")]
    ConfigError(#[from] ConfigError),

    /// An error that occurs when attempting multi-write operations on unsupported components.
    #[error("multi write is not supported for {0}")]
    MultiWriteNotSupported(&'static str),
}

/// A collection of Redis clients used by Relay for different purposes.
#[derive(Debug, Clone)]
pub struct RedisClients {
    /// The pool used for project configurations
    pub project_configs: AsyncRedisPool,
    /// The pool used for cardinality limits.
    pub cardinality: AsyncRedisPool,
    /// The pool used for rate limiting/quotas.
    pub quotas: AsyncRedisPool,
}

/// Statistics about the Redis client's connection pool state.
#[derive(Debug)]
pub struct RedisClientStats {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
}

/// A connection pool that can manage either a single Redis instance or a Redis cluster.
#[derive(Clone)]
pub enum AsyncRedisPool {
    /// Contains a connection pool to a Redis cluster.
    Cluster(ClusterPool),
    /// Contains a connection pool to a single Redis instance.
    Single(SinglePool),
}

impl AsyncRedisPool {
    /// Creates a new connection pool for a Redis cluster.
    ///
    /// This method initializes a connection pool that can communicate with multiple Redis nodes
    /// in a cluster configuration. The pool is configured with the specified servers and options.
    pub fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let servers = servers
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // We use our custom cluster manager which performs recycling in a different way from the
        // default manager.
        let manager = CustomClusterManager::new(servers, false).map_err(RedisError::Redis)?;

        // TODO: correctly configure the connection.
        let pool = Pool::builder(manager)
            .max_size(opts.max_connections as usize)
            .wait_timeout(Some(Duration::from_secs(1)))
            .create_timeout(Some(Duration::from_secs(1)))
            .recycle_timeout(Some(Duration::from_secs(1)))
            .build()?;

        Ok(AsyncRedisPool::Cluster(pool))
    }

    /// Creates a new connection pool for a single Redis instance.
    ///
    /// This method initializes a connection pool that communicates with a single Redis server.
    /// The pool is configured with the specified server URL and options.
    pub fn single(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        // We use our custom single manager which performs recycling in a different way from the
        // default manager.
        let manager = CustomSingleManager::new(server).map_err(RedisError::Redis)?;

        // TODO: correctly configure the connection.
        let pool = Pool::builder(manager)
            .max_size(opts.max_connections as usize)
            .wait_timeout(Some(Duration::from_secs(1)))
            .create_timeout(Some(Duration::from_secs(1)))
            .recycle_timeout(Some(Duration::from_secs(1)))
            .build()?;

        Ok(AsyncRedisPool::Single(pool))
    }

    /// Acquires a connection from the pool.
    ///
    /// Returns a new [`AsyncRedisConnection`] that can be used to execute Redis commands.
    /// The connection is automatically returned to the pool when dropped.
    pub async fn get_connection(&self) -> Result<AsyncRedisConnection, RedisError> {
        let connection = match self {
            Self::Cluster(pool) => {
                AsyncRedisConnection::Cluster(pool.get().await.map_err(RedisError::Pool)?)
            }
            Self::Single(pool) => {
                AsyncRedisConnection::Single(pool.get().await.map_err(RedisError::Pool)?)
            }
        };

        Ok(connection)
    }

    /// Returns statistics about the current state of the connection pool.
    pub fn stats(&self) -> RedisClientStats {
        let status = match self {
            Self::Cluster(pool) => pool.status(),
            Self::Single(pool) => pool.status(),
        };

        RedisClientStats {
            idle_connections: status.available as u32,
            connections: status.size as u32,
        }
    }
}

impl std::fmt::Debug for AsyncRedisPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsyncRedisPool::Cluster(_) => write!(f, "AsyncRedisPool::Cluster"),
            AsyncRedisPool::Single(_) => write!(f, "AsyncRedisPool::Single"),
        }
    }
}

/// A connection to either a single Redis instance or a Redis cluster.
pub enum AsyncRedisConnection {
    /// A connection to a Redis cluster.
    Cluster(CustomClusterConnection),
    /// A connection to a single Redis instance.
    Single(CustomSingleConnection),
}

impl AsyncRedisConnection {}

impl std::fmt::Debug for AsyncRedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Cluster(_) => "Cluster",
            Self::Single(_) => "Single",
        };
        f.debug_tuple(name).finish()
    }
}

impl redis::aio::ConnectionLike for AsyncRedisConnection {
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
