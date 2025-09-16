use deadpool::managed::{BuildError, Manager, Metrics, Object, Pool, PoolError};
use deadpool_redis::{ConfigError, Runtime};
use redis::{Cmd, Pipeline, RedisFuture, Value};
use std::time::Duration;
use thiserror::Error;

use crate::config::RedisConfigOptions;
use crate::pool;

pub use redis;

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
}

/// A collection of Redis clients used by Relay for different purposes.
///
/// This struct manages separate Redis connection clients for different functionalities
/// within the Relay system, such as project configurations, cardinality limits,
/// and rate limiting.
#[derive(Debug, Clone)]
pub struct RedisClients {
    /// The client used for project configurations
    pub project_configs: AsyncRedisClient,
    /// The client used for cardinality limits.
    pub cardinality: AsyncRedisClient,
    /// The client used for rate limiting/quotas.
    pub quotas: AsyncRedisClient,
}

/// Statistics about the Redis client's connection client state.
///
/// Provides information about the current state of Redis connection clients,
/// including the number of active and idle connections.
#[derive(Debug)]
pub struct RedisClientStats {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
    /// The maximum number of connections in the pool.
    pub max_connections: u32,
    /// The number of futures that are currently waiting to get a connection from the pool.
    ///
    /// This number increases when there are not enough connections in the pool.
    pub waiting_for_connection: u32,
}

/// A connection client that can manage either a single Redis instance or a Redis cluster.
///
/// This enum provides a unified interface for Redis operations, supporting both
/// single-instance and cluster configurations.
#[derive(Clone)]
pub enum AsyncRedisClient {
    /// Contains a connection pool to a Redis cluster.
    Cluster(pool::CustomClusterPool),
    /// Contains a connection pool to a single Redis instance.
    Single(pool::CustomSinglePool),
    /// Contains a connection pool to a Redis-master instance.
    Sentinel(pool::CustomSentinelPool),
}

impl AsyncRedisClient {
    /// Creates a new connection client for a Redis cluster.
    ///
    /// This method initializes a connection client that can communicate with multiple Redis nodes
    /// in a cluster configuration. The client is configured with the specified servers and options.
    ///
    /// The client uses a custom cluster manager that implements a specific connection recycling
    /// strategy, ensuring optimal performance and reliability in cluster environments.
    pub fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let servers = servers
            .into_iter()
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();

        // We use our custom cluster manager which performs recycling in a different way from the
        // default manager.
        let manager = pool::CustomClusterManager::new(servers, false, opts.recycle_check_frequency)
            .map_err(RedisError::Redis)?;

        let pool = Self::build_pool(manager, opts)?;

        Ok(AsyncRedisClient::Cluster(pool))
    }

    /// Creates a new connection client for a single Redis instance.
    ///
    /// This method initializes a connection client that communicates with a single Redis server.
    /// The client is configured with the specified server URL and options.
    ///
    /// The client uses a custom single manager that implements a specific connection recycling
    /// strategy, ensuring optimal performance and reliability in single-instance environments.
    pub fn single(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        // We use our custom single manager which performs recycling in a different way from the
        // default manager.
        let manager = pool::CustomSingleManager::new(server, opts.recycle_check_frequency)
            .map_err(RedisError::Redis)?;

        let pool = Self::build_pool(manager, opts)?;

        Ok(AsyncRedisClient::Single(pool))
    }

    /// Creates a new connection client for a Redis-master instance.
    ///
    /// This method initializes a connection client that can communicate with multiple Sentinel nodes
    /// to retrieve connection to a current Redis-master instance.
    /// The client is configured with the specified sentinel servers, master name and options.
    ///
    /// The client uses a custom Sentinel manager that implements a specific connection recycling
    /// strategy, ensuring optimal performance and reliability in single-instance environments.
    pub fn sentinel<'a>(
        sentinels: impl IntoIterator<Item = &'a str>,
        master_name: &str,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let sentinels = sentinels
            .into_iter()
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();
        // We use our custom single manager which performs recycling in a different way from the
        // default manager.
        let manager = pool::CustomSentinelManager::new(
            sentinels,
            master_name.to_owned(),
            opts.recycle_check_frequency,
        )
        .map_err(RedisError::Redis)?;

        let pool = Self::build_pool(manager, opts)?;

        Ok(AsyncRedisClient::Sentinel(pool))
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
            Self::Sentinel(pool) => {
                AsyncRedisConnection::Sentinel(pool.get().await.map_err(RedisError::Pool)?)
            }
        };

        Ok(connection)
    }

    /// Returns statistics about the current state of the connection pool.
    ///
    /// Provides information about the number of active and idle connections in the pool,
    /// which can be useful for monitoring and debugging purposes.
    pub fn stats(&self) -> RedisClientStats {
        let status = match self {
            Self::Cluster(pool) => pool.status(),
            Self::Single(pool) => pool.status(),
            Self::Sentinel(pool) => pool.status(),
        };

        RedisClientStats {
            idle_connections: status.available as u32,
            connections: status.size as u32,
            max_connections: status.max_size as u32,
            waiting_for_connection: status.waiting as u32,
        }
    }

    /// Runs the `predicate` on the pool blocking it.
    ///
    /// If the `predicate` returns `false` the object will be removed from pool.
    pub fn retain(&self, mut predicate: impl FnMut(Metrics) -> bool) {
        match self {
            Self::Cluster(pool) => {
                pool.retain(|_, metrics| predicate(metrics));
            }
            Self::Single(pool) => {
                pool.retain(|_, metrics| predicate(metrics));
            }
            Self::Sentinel(pool) => {
                pool.retain(|_, metrics| predicate(metrics));
            }
        }
    }

    /// Builds a [`Pool`] given a type implementing [`Manager`] and [`RedisConfigOptions`].
    fn build_pool<M: Manager + 'static, W: From<Object<M>> + 'static>(
        manager: M,
        opts: &RedisConfigOptions,
    ) -> Result<Pool<M, W>, BuildError> {
        let result = Pool::builder(manager)
            .max_size(opts.max_connections as usize)
            .create_timeout(opts.create_timeout.map(Duration::from_secs))
            .recycle_timeout(opts.recycle_timeout.map(Duration::from_secs))
            .wait_timeout(opts.wait_timeout.map(Duration::from_secs))
            .runtime(Runtime::Tokio1)
            .build();

        let idle_timeout = opts.idle_timeout;
        let refresh_interval = opts.idle_timeout / 2;
        if let Ok(pool) = result.clone() {
            relay_system::spawn!(async move {
                loop {
                    pool.retain(|_, metrics| {
                        metrics.last_used() < Duration::from_secs(idle_timeout)
                    });
                    tokio::time::sleep(Duration::from_secs(refresh_interval)).await;
                }
            });
        }

        result
    }
}

impl std::fmt::Debug for AsyncRedisClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsyncRedisClient::Cluster(_) => write!(f, "AsyncRedisPool::Cluster"),
            AsyncRedisClient::Single(_) => write!(f, "AsyncRedisPool::Single"),
            AsyncRedisClient::Sentinel(_) => write!(f, "AsyncRedisPool::Sentinel"),
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
    Cluster(pool::CustomClusterConnection),
    /// A connection to a single Redis instance.
    Single(pool::CustomSingleConnection),
    /// A connection to a single Redis-master instance
    Sentinel(pool::CustomSentinelConnection),
}

impl std::fmt::Debug for AsyncRedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Cluster(_) => "Cluster",
            Self::Single(_) => "Single",
            Self::Sentinel(_) => "Sentinel",
        };
        f.debug_tuple(name).finish()
    }
}

impl redis::aio::ConnectionLike for AsyncRedisConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Self::Cluster(conn) => conn.req_packed_command(cmd),
            Self::Single(conn) => conn.req_packed_command(cmd),
            Self::Sentinel(conn) => conn.req_packed_command(cmd),
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
            Self::Sentinel(conn) => conn.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Cluster(conn) => conn.get_db(),
            Self::Single(conn) => conn.get_db(),
            Self::Sentinel(conn) => conn.get_db(),
        }
    }
}
