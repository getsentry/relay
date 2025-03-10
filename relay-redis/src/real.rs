use std::fmt;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::{Client, Cmd, Pipeline, RedisFuture, Value};
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

    /// Multi write is not supported for the specified part.
    #[error("multi write is not supported for {0}")]
    MultiWriteNotSupported(&'static str),
}

/// The various [`AsyncRedisClient`](s) used by Relay.
#[derive(Debug, Clone)]
pub struct RedisClients {
    /// The pool used for project configurations
    pub project_configs: AsyncRedisClient,
    /// The pool used for cardinality limits.
    pub cardinality: AsyncRedisClient,
    /// The pool used for rate limiting/quotas.
    pub quotas: AsyncRedisClient,
}

/// Stats about how the Redis client is performing.
#[derive(Debug)]
pub struct RedisClientStats {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
}

/// Asynchronous redis client that wraps an [`AsyncRedisConnection`].
#[derive(Debug, Clone)]
pub struct AsyncRedisClient {
    connection: AsyncRedisConnection,
}

impl AsyncRedisClient {
    /// Creates a new [`AsyncRedisClient`] from an [`AsyncRedisConnection`].
    fn new(connection: AsyncRedisConnection) -> Self {
        Self { connection }
    }

    /// Creates a new [`AsyncRedisClient`] in cluster mode.
    pub async fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        AsyncRedisConnection::cluster(servers, opts)
            .await
            .map(AsyncRedisClient::new)
    }

    /// Creates a new [`AsyncRedisClient`] in single mode.
    pub async fn single(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        AsyncRedisConnection::single(server, opts)
            .await
            .map(AsyncRedisClient::new)
    }

    /// Returns a shared [`AsyncRedisConnection`].
    pub fn get_connection(&self) -> AsyncRedisConnection {
        self.connection.clone()
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

/// A wrapper type for async redis connections.
#[derive(Clone)]
pub enum AsyncRedisConnection {
    /// Variant for [`ClusterConnection`].
    Cluster(ClusterConnection),
    /// Variant for [`ConnectionManager`] using [`redis::aio::MultiplexedConnection`].
    Single(ConnectionManager),
}

impl AsyncRedisConnection {
    /// Creates an [`AsyncRedisConnection`] in cluster mode.
    pub async fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        // connection timeout is set in `base_pool_builder` on the pool level
        let client = ClusterClientBuilder::new(servers)
            .response_timeout(Duration::from_secs(opts.read_timeout))
            .connection_timeout(Duration::from_secs(opts.connection_timeout))
            .build()
            .map_err(RedisError::Redis)?;
        let connection = client
            .get_async_connection()
            .await
            .map_err(RedisError::Redis)?;
        Ok(Self::Cluster(connection))
    }

    /// Create an [`AsyncRedisConnection`] in single mode.
    pub async fn single(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        let client = Client::open(server).map_err(RedisError::Redis)?;
        let config = ConnectionManagerConfig::new()
            .set_response_timeout(Duration::from_secs(opts.read_timeout))
            .set_connection_timeout(Duration::from_secs(opts.connection_timeout));
        let connection_manager = ConnectionManager::new_with_config(client, config)
            .await
            .map_err(RedisError::Redis)?;
        Ok(Self::Single(connection_manager))
    }
}

impl Debug for AsyncRedisConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
