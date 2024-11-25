use crate::config::RedisConfigOptions;
use bb8_redis::bb8;
use bb8_redis::bb8::RunError;
use r2d2::{Builder, ManageConnection, Pool, PooledConnection};
pub use redis;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Cmd, ConnectionLike, FromRedisValue, Script};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::thread::Scope;
use std::time::Duration;
use std::{fmt, thread};
use thiserror::Error;
use tokio::time::timeout;

/// An error returned from `RedisPool`.
#[derive(Debug, Error)]
pub enum RedisError {
    /// Failure to configure Redis.
    #[error("failed to configure redis")]
    Configuration,

    /// Failure in r2d2 pool.
    #[error("failed to pool redis connection")]
    Pool(#[source] r2d2::Error),

    /// Failure in bb8 pool.
    #[error("failed to pool async redis connections")]
    AsyncPool(#[source] RunError<redis::RedisError>),

    /// Timeout occurred.
    #[error("redis connection timeout")]
    AsyncTimeout(#[source] tokio::time::error::Elapsed),

    /// Failure in Redis communication.
    #[error("failed to communicate with redis")]
    Redis(#[source] redis::RedisError),
}

fn log_secondary_redis_error<T>(result: redis::RedisResult<T>) {
    if let Err(error) = result {
        relay_log::error!(
            error = &error as &dyn Error,
            "sending cmds to the secondary Redis instance failed",
        );
    }
}

fn spawn_secondary_thread<'scope, 'env: 'scope, T>(
    scope: &'scope Scope<'scope, 'env>,
    block: impl FnOnce() -> redis::RedisResult<T> + Send + 'scope,
) {
    let result = thread::Builder::new().spawn_scoped(scope, move || {
        log_secondary_redis_error(block());
    });
    if let Err(error) = result {
        relay_log::error!(
            error = &error as &dyn Error,
            "spawning the thread for the secondary Redis connection failed",
        );
    }
}

enum ConnectionInner<'a> {
    Cluster(&'a mut redis::cluster::ClusterConnection),
    MultiWrite {
        primary: Box<ConnectionInner<'a>>,
        secondaries: Vec<ConnectionInner<'a>>,
    },
    Single(&'a mut redis::Connection),
}

impl ConnectionLike for ConnectionInner<'_> {
    fn req_packed_command(&mut self, cmd: &[u8]) -> redis::RedisResult<redis::Value> {
        match self {
            ConnectionInner::Cluster(con) => con.req_packed_command(cmd),
            ConnectionInner::Single(con) => con.req_packed_command(cmd),
            ConnectionInner::MultiWrite {
                primary,
                secondaries,
            } => thread::scope(|s| {
                for connection in secondaries {
                    spawn_secondary_thread(s, || connection.req_packed_command(cmd))
                }
                primary.req_packed_command(cmd)
            }),
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        match self {
            ConnectionInner::Cluster(con) => con.req_packed_commands(cmd, offset, count),
            ConnectionInner::Single(con) => con.req_packed_commands(cmd, offset, count),
            ConnectionInner::MultiWrite {
                primary,
                secondaries,
            } => thread::scope(|s| {
                for connection in secondaries {
                    spawn_secondary_thread(s, || connection.req_packed_commands(cmd, offset, count))
                }
                primary.req_packed_commands(cmd, offset, count)
            }),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            ConnectionInner::Cluster(con) => con.get_db(),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                ..
            } => primary_connection.get_db(),
            ConnectionInner::Single(con) => con.get_db(),
        }
    }

    fn check_connection(&mut self) -> bool {
        match self {
            ConnectionInner::Cluster(con) => con.check_connection(),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                ..
            } => primary_connection.check_connection(),
            ConnectionInner::Single(con) => con.check_connection(),
        }
    }

    fn is_open(&self) -> bool {
        match self {
            ConnectionInner::Cluster(con) => con.is_open(),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                ..
            } => primary_connection.is_open(),
            ConnectionInner::Single(con) => con.is_open(),
        }
    }
}

/// A reference to a pooled connection from `PooledClient`.
pub struct Connection<'a> {
    inner: ConnectionInner<'a>,
}

impl ConnectionLike for Connection<'_> {
    fn req_packed_command(&mut self, cmd: &[u8]) -> redis::RedisResult<redis::Value> {
        self.inner.req_packed_command(cmd)
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        self.inner.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.inner.get_db()
    }

    fn check_connection(&mut self) -> bool {
        self.inner.check_connection()
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }
}

/// A pooled Redis client.
pub enum PooledClient {
    /// Pool that is connected to a Redis cluster.
    Cluster(
        Box<PooledConnection<redis::cluster::ClusterClient>>,
        RedisConfigOptions,
    ),
    /// Multiple pools that are used for multi-write.
    MultiWrite {
        /// Primary [`PooledClient`].
        primary: Box<PooledClient>,
        /// Array of secondary [`PooledClient`]s.
        secondaries: Vec<PooledClient>,
    },
    /// Pool that is connected to a single Redis instance.
    Single(Box<PooledConnection<redis::Client>>, RedisConfigOptions),
}

impl PooledClient {
    /// Returns a pooled connection to this client.
    ///
    /// When the connection is fetched from the pool, we also set the read and write timeouts to
    /// the configured values.
    pub fn connection(&mut self) -> Result<Connection<'_>, RedisError> {
        Ok(Connection {
            inner: self.connection_inner()?,
        })
    }

    /// Recursively computes the [`ConnectionInner`] from a [`PooledClient`].
    fn connection_inner(&mut self) -> Result<ConnectionInner<'_>, RedisError> {
        let inner = match self {
            PooledClient::Cluster(connection, opts) => {
                connection
                    .set_read_timeout(Some(Duration::from_secs(opts.read_timeout)))
                    .map_err(RedisError::Redis)?;
                connection
                    .set_write_timeout(Some(Duration::from_secs(opts.write_timeout)))
                    .map_err(RedisError::Redis)?;

                ConnectionInner::Cluster(connection)
            }
            PooledClient::MultiWrite {
                primary: primary_client,
                secondaries: secondary_clients,
            } => {
                let primary_connection = primary_client.connection_inner()?;
                let mut secondary_connections = Vec::with_capacity(secondary_clients.len());
                for secondary_client in secondary_clients.iter_mut() {
                    let connection = secondary_client.connection_inner()?;
                    secondary_connections.push(connection);
                }

                ConnectionInner::MultiWrite {
                    primary: Box::new(primary_connection),
                    secondaries: secondary_connections,
                }
            }
            PooledClient::Single(connection, opts) => {
                connection
                    .set_read_timeout(Some(Duration::from_secs(opts.read_timeout)))
                    .map_err(RedisError::Redis)?;
                connection
                    .set_write_timeout(Some(Duration::from_secs(opts.write_timeout)))
                    .map_err(RedisError::Redis)?;

                ConnectionInner::Single(connection)
            }
        };

        Ok(inner)
    }
}

/// Abstraction over cluster vs non-cluster mode.
///
/// Even just writing a method that takes a command and executes it doesn't really work because
/// there's both `Cmd` and `ScriptInvocation` to take care of, and both have sync vs async
/// APIs.
///
/// Basically don't waste your time here, if you want to abstract over this, consider
/// upstreaming to the redis crate.
#[derive(Clone)]
pub enum RedisPool {
    /// Pool that is connected to a Redis cluster.
    Cluster(Pool<redis::cluster::ClusterClient>, RedisConfigOptions),
    /// Multiple pools that are used for multi-write.
    MultiWrite {
        /// Primary [`RedisPool`].
        primary: Box<RedisPool>,
        /// Array of secondary [`RedisPool`]s.
        secondaries: Vec<RedisPool>,
    },
    /// Pool that is connected to a single Redis instance.
    Single(Pool<redis::Client>, RedisConfigOptions),
}

impl fmt::Debug for RedisPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster(_, _) => f.debug_tuple("Cluster").finish(),
            Self::MultiWrite { .. } => f.debug_tuple("MultiWrite").finish(),
            Self::Single(_, _) => f.debug_tuple("Single").finish(),
        }
    }
}

impl RedisPool {
    /// Creates a [`RedisPool`] in cluster configuration.
    pub fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let pool = Self::base_pool_builder(&opts)
            .build(redis::cluster::ClusterClient::new(servers).map_err(RedisError::Redis)?)
            .map_err(RedisError::Pool)?;

        Ok(RedisPool::Cluster(pool, opts))
    }

    /// Creates a [`RedisPool`] in multi write configuration.
    pub fn multi_write(
        primary: RedisPool,
        secondaries: Vec<RedisPool>,
    ) -> Result<Self, RedisError> {
        Ok(RedisPool::MultiWrite {
            primary: Box::new(primary),
            secondaries,
        })
    }

    /// Creates a [`RedisPool`] in single-node configuration.
    pub fn single(server: &str, opts: RedisConfigOptions) -> Result<Self, RedisError> {
        let pool = Self::base_pool_builder(&opts)
            .build(redis::Client::open(server).map_err(RedisError::Redis)?)
            .map_err(RedisError::Pool)?;

        Ok(RedisPool::Single(pool, opts))
    }

    /// Returns a pooled connection to a client.
    pub fn client(&self) -> Result<PooledClient, RedisError> {
        let pool = match self {
            RedisPool::Cluster(pool, opts) => PooledClient::Cluster(
                Box::new(pool.get().map_err(RedisError::Pool)?),
                opts.clone(),
            ),
            RedisPool::MultiWrite {
                primary: primary_pool,
                secondaries: secondary_pools,
            } => {
                let primary_client = primary_pool.client()?;
                let mut secondary_clients = Vec::with_capacity(secondary_pools.len());
                for secondary_pool in secondary_pools.iter() {
                    let client = secondary_pool.client()?;
                    secondary_clients.push(client);
                }

                PooledClient::MultiWrite {
                    primary: Box::new(primary_client),
                    secondaries: secondary_clients,
                }
            }
            RedisPool::Single(pool, opts) => PooledClient::Single(
                Box::new(pool.get().map_err(RedisError::Pool)?),
                opts.clone(),
            ),
        };

        Ok(pool)
    }

    /// Returns information about the current state of the pool.
    pub fn stats(&self) -> Stats {
        let (connections, idle_connections) = self.state();
        Stats {
            connections,
            idle_connections,
        }
    }

    /// Returns the base builder for the pool with the options applied.
    fn base_pool_builder<M: ManageConnection>(opts: &RedisConfigOptions) -> Builder<M> {
        Pool::builder()
            .max_size(opts.max_connections)
            .min_idle(opts.min_idle)
            .test_on_check_out(false)
            .max_lifetime(Some(Duration::from_secs(opts.max_lifetime)))
            .idle_timeout(Some(Duration::from_secs(opts.idle_timeout)))
            .connection_timeout(Duration::from_secs(opts.connection_timeout))
    }

    /// Recursively computes the state of the [`RedisPool`].
    fn state(&self) -> (u32, u32) {
        match self {
            RedisPool::Cluster(p, _) => (p.state().connections, p.state().idle_connections),
            RedisPool::MultiWrite { primary: p, .. } => p.state(),
            RedisPool::Single(p, _) => (p.state().connections, p.state().idle_connections),
        }
    }
}

/// The various [`RedisPool`]s used within Relay.
#[derive(Debug, Clone)]
pub struct RedisPools {
    /// The pool used for project configurations
    pub project_configs: AsyncRedisPool,
    /// The pool used for cardinality limits.
    pub cardinality: RedisPool,
    /// The pool used for rate limiting/quotas.
    pub quotas: RedisPool,
}

/// Stats about how the [`RedisPool`] is performing.
pub struct Stats {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
}

/// [`bb8::ManageConnection`] for a async redis cluster.
pub struct RedisClusterConnectionManager {
    client: ClusterClient,
}

impl RedisClusterConnectionManager {
    /// Creates a new [`bb8::ManageConnection`] with the provided [`ClusterClient`]
    pub fn new(client: ClusterClient) -> Self {
        Self { client }
    }
}

impl bb8::ManageConnection for RedisClusterConnectionManager {
    type Connection = ClusterConnection;
    type Error = redis::RedisError;

    fn connect<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
    {
        let client = self.client.clone();
        Box::pin(async move { client.get_async_connection().await })
    }

    fn is_valid<'life0, 'life1, 'async_trait>(
        &'life0 self,
        conn: &'life1 mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move { redis::cmd("PING").query_async(conn).await })
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

/// An abstraction over Single and Cluster redis connections.
#[derive(Debug, Clone)]
pub enum AsyncRedisPool {
    /// Represents a connection pool to a clustered redis instance.
    Cluster(bb8::Pool<RedisClusterConnectionManager>, RedisConfigOptions),
    /// Represents a connection pool to a single redis server.
    Single(
        bb8::Pool<bb8_redis::RedisConnectionManager>,
        RedisConfigOptions,
    ),
}

impl AsyncRedisPool {
    /// Takes a command and executes it on redis.
    pub async fn query_async<T: FromRedisValue>(&self, cmd: Cmd) -> Result<T, RedisError> {
        match self {
            Self::Cluster(pool, options) => {
                let mut conn = pool.get().await.map_err(RedisError::AsyncPool)?;
                timeout(
                    Duration::from_secs(options.read_timeout),
                    cmd.query_async(&mut *conn),
                )
                .await
                .map_err(RedisError::AsyncTimeout)?
                .map_err(RedisError::Redis)
            }
            Self::Single(pool, options) => {
                let mut conn = pool.get().await.map_err(RedisError::AsyncPool)?;
                timeout(
                    Duration::from_secs(options.read_timeout),
                    cmd.query_async(&mut *conn),
                )
                .await
                .map_err(RedisError::AsyncTimeout)?
                .map_err(RedisError::Redis)
            }
        }
    }

    /// Forwards the [`Script`] to the pool and loads it asynchronously.
    /// Returns the SHA1 hash of it.
    pub async fn load_async(&self, script: &Script) -> Result<String, RedisError> {
        match self {
            Self::Cluster(pool, _options) => {
                let mut conn = pool.get().await.map_err(RedisError::AsyncPool)?;
                script
                    .prepare_invoke()
                    .load_async(&mut *conn)
                    .await
                    .map_err(RedisError::Redis)
            }
            Self::Single(pool, _options) => {
                let mut conn = pool.get().await.map_err(RedisError::AsyncPool)?;
                script
                    .prepare_invoke()
                    .load_async(&mut *conn)
                    .await
                    .map_err(RedisError::Redis)
            }
        }
    }

    /// Creates a new cluster based [`AsyncRedisPool`].
    pub async fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let client = ClusterClient::new(servers).map_err(RedisError::Redis)?;
        let manager = RedisClusterConnectionManager::new(client);
        let pool = Self::base_pool_builder(opts)
            .build(manager)
            .await
            .map_err(RedisError::Redis)?;
        Ok(AsyncRedisPool::Cluster(pool, opts.clone()))
    }

    /// Creates a new [`AsyncRedisPool`] backed by a single redis server.
    pub async fn single(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        let manager = bb8_redis::RedisConnectionManager::new(server).map_err(RedisError::Redis)?;
        let pool = Self::base_pool_builder(opts)
            .build(manager)
            .await
            .map_err(RedisError::Redis)?;
        Ok(AsyncRedisPool::Single(pool, opts.clone()))
    }

    /// Returns information about the current state of the pool.
    pub fn stats(&self) -> Stats {
        let state = match self {
            AsyncRedisPool::Cluster(pool, _) => pool.state(),
            AsyncRedisPool::Single(pool, _) => pool.state(),
        };
        Stats {
            connections: state.connections,
            idle_connections: state.idle_connections,
        }
    }

    fn base_pool_builder<M: bb8::ManageConnection>(opts: &RedisConfigOptions) -> bb8::Builder<M> {
        bb8::Pool::builder()
            .max_size(opts.max_connections)
            .min_idle(opts.min_idle)
            .test_on_check_out(false)
            .max_lifetime(Some(Duration::from_secs(opts.max_lifetime)))
            .idle_timeout(Some(Duration::from_secs(opts.idle_timeout)))
            .connection_timeout(Duration::from_secs(opts.connection_timeout))
    }
}
