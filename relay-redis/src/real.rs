use deadpool::managed::{BuildError, Manager, Object, Pool, PoolError};
use deadpool_redis::redis::{Cmd, Pipeline, RedisFuture, Value};
use deadpool_redis::{ConfigError, Runtime};
use std::time::Duration;
use thiserror::Error;

use crate::config::RedisConfigOptions;
use crate::pool::{
    ClusterPool, CustomClusterConnection, CustomClusterManager, CustomSingleConnection,
    CustomSingleManager, SinglePool,
};

pub use deadpool_redis::redis;

/// An error type that represents various failure modes when interacting with Redis.
///
/// This enum provides a unified error type for Redis-related operations, handling both
/// configuration issues and runtime errors that may occur during Redis interactions.
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

/// A collection of Redis pools used by Relay for different purposes.
///
/// This struct manages separate Redis connection pools for different functionalities
/// within the Relay system, such as project configurations, cardinality limits,
/// and rate limiting.
#[derive(Debug, Clone)]
pub struct RedisPools {
    /// The pool used for project configurations
    pub project_configs: AsyncRedisPool,
    /// The pool used for cardinality limits.
    pub cardinality: AsyncRedisPool,
    /// The pool used for rate limiting/quotas.
    pub quotas: AsyncRedisPool,
}

/// Statistics about the Redis pool's connection pool state.
///
/// Provides information about the current state of Redis connection pools,
/// including the number of active and idle connections.
#[derive(Debug)]
pub struct RedisPoolStats {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
}

/// A connection pool that can manage either a single Redis instance or a Redis cluster.
///
/// This enum provides a unified interface for Redis operations, supporting both
/// single-instance and cluster configurations.
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
    ///
    /// The pool uses a custom cluster manager that implements a specific connection recycling
    /// strategy, ensuring optimal performance and reliability in cluster environments.
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
        let manager = CustomClusterManager::new(servers, false, opts.refresh_interval)
            .map_err(RedisError::Redis)?;

        let pool = Self::build_pool(manager, opts)?;

        Ok(AsyncRedisPool::Cluster(pool))
    }

    /// Creates a new connection pool for a single Redis instance.
    ///
    /// This method initializes a connection pool that communicates with a single Redis server.
    /// The pool is configured with the specified server URL and options.
    ///
    /// The pool uses a custom single manager that implements a specific connection recycling
    /// strategy, ensuring optimal performance and reliability in single-instance environments.
    pub fn single(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        // We use our custom single manager which performs recycling in a different way from the
        // default manager.
        let manager =
            CustomSingleManager::new(server, opts.refresh_interval).map_err(RedisError::Redis)?;

        let pool = Self::build_pool(manager, opts)?;

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
    ///
    /// Provides information about the number of active and idle connections in the pool,
    /// which can be useful for monitoring and debugging purposes.
    pub fn stats(&self) -> RedisPoolStats {
        let status = match self {
            Self::Cluster(pool) => pool.status(),
            Self::Single(pool) => pool.status(),
        };

        RedisPoolStats {
            idle_connections: status.available as u32,
            connections: status.size as u32,
        }
    }

    /// Builds a [`Pool`] given a type implementing [`Manager`] and [`RedisConfigOptions`].
    fn build_pool<M: Manager, W: From<Object<M>>>(
        manager: M,
        opts: &RedisConfigOptions,
    ) -> Result<Pool<M, W>, BuildError> {
        Pool::builder(manager)
            .max_size(opts.max_connections as usize)
            .wait_timeout(Some(Duration::from_secs(opts.wait_timeout)))
            .create_timeout(Some(Duration::from_secs(opts.create_timeout)))
            .recycle_timeout(Some(Duration::from_secs(opts.recycle_timeout)))
            .runtime(Runtime::Tokio1)
            .build()
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
///
/// This enum provides a unified interface for Redis operations, abstracting away the
/// differences between single-instance and cluster connections. It implements the
/// [`redis::aio::ConnectionLike`] trait, allowing it to be used with Redis commands
/// regardless of the underlying connection type.
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
