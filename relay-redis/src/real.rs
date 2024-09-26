use std::fmt;
use std::process::id;
use std::time::Duration;

use r2d2::{Builder, ManageConnection, Pool, PooledConnection};
pub use redis;
use redis::ConnectionLike;
use thiserror::Error;

use crate::config::RedisConfigOptions;

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
            ConnectionInner::Cluster(ref mut con) => con.req_packed_command(cmd),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                secondaries: secondary_connections,
            } => {
                let primary_result = primary_connection.req_packed_command(cmd);
                for secondary_connection in secondary_connections.iter_mut() {
                    secondary_connection.req_packed_command(cmd)?;
                }

                primary_result
            }
            ConnectionInner::Single(ref mut con) => con.req_packed_command(cmd),
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        match self {
            ConnectionInner::Cluster(ref mut con) => con.req_packed_commands(cmd, offset, count),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                secondaries: secondary_connections,
            } => {
                let primary_result = primary_connection.req_packed_commands(cmd, offset, count);
                for secondary_connection in secondary_connections.iter_mut() {
                    secondary_connection.req_packed_commands(cmd, offset, count)?;
                }

                primary_result
            }
            ConnectionInner::Single(ref mut con) => con.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            ConnectionInner::Cluster(ref con) => con.get_db(),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                ..
            } => primary_connection.get_db(),
            ConnectionInner::Single(ref con) => con.get_db(),
        }
    }

    fn check_connection(&mut self) -> bool {
        match self {
            ConnectionInner::Cluster(ref mut con) => con.check_connection(),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                secondaries: secondary_connections,
            } => {
                primary_connection.check_connection()
                    && secondary_connections
                        .iter_mut()
                        .all(|c| c.check_connection())
            }
            ConnectionInner::Single(ref mut con) => con.check_connection(),
        }
    }

    fn is_open(&self) -> bool {
        match self {
            ConnectionInner::Cluster(ref con) => con.is_open(),
            ConnectionInner::MultiWrite {
                primary: primary_connection,
                secondaries: secondary_connections,
            } => primary_connection.is_open() && secondary_connections.iter().all(|c| c.is_open()),
            ConnectionInner::Single(ref con) => con.is_open(),
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

enum PooledClientInner {
    Cluster(Box<PooledConnection<redis::cluster::ClusterClient>>),
    MultiWrite(Box<PooledClientInner>, Vec<PooledClientInner>),
    Single(Box<PooledConnection<redis::Client>>),
}

/// A pooled Redis client.
pub struct PooledClient {
    opts: RedisConfigOptions,
    inner: PooledClientInner,
}

impl PooledClient {
    /// Returns a pooled connection to this client.
    ///
    /// When the connection is fetched from the pool, we also set the read and write timeouts to
    /// the configured values.
    pub fn connection(&mut self) -> Result<Connection<'_>, RedisError> {
        let inner = Self::connection_inner(&mut self.inner, &self.opts)?;
        Ok(Connection { inner })
    }

    /// Recursively computes the [`ConnectionInner`] from a [`PooledClientInner`].
    fn connection_inner<'a>(
        inner: &'a mut PooledClientInner,
        opts: &RedisConfigOptions,
    ) -> Result<ConnectionInner<'a>, RedisError> {
        let inner = match inner {
            PooledClientInner::Cluster(ref mut connection) => {
                connection
                    .set_read_timeout(Some(Duration::from_secs(opts.read_timeout)))
                    .map_err(RedisError::Redis)?;
                connection
                    .set_write_timeout(Some(Duration::from_secs(opts.write_timeout)))
                    .map_err(RedisError::Redis)?;

                ConnectionInner::Cluster(connection)
            }
            PooledClientInner::MultiWrite(primary_client, secondary_clients) => {
                let primary_connection = Self::connection_inner(primary_client.as_mut(), opts)?;
                let mut secondary_connections = Vec::with_capacity(secondary_clients.len());
                for secondary_client in secondary_clients.iter_mut() {
                    let connection = Self::connection_inner(secondary_client, opts)?;
                    secondary_connections.push(connection);
                }

                ConnectionInner::MultiWrite {
                    primary: Box::new(primary_connection),
                    secondaries: secondary_connections,
                }
            }
            PooledClientInner::Single(ref mut connection) => {
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

#[derive(Clone)]
enum RedisPoolInner {
    Cluster(Pool<redis::cluster::ClusterClient>),
    MultiWrite(Box<RedisPoolInner>, Vec<RedisPoolInner>),
    Single(Pool<redis::Client>),
}

impl fmt::Debug for RedisPoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster(_) => f.debug_tuple("Cluster").finish(),
            Self::MultiWrite(_, _) => f.debug_tuple("MultiWrite").finish(),
            Self::Single(_) => f.debug_tuple("Single").finish(),
        }
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
#[derive(Clone, Debug)]
pub struct RedisPool {
    opts: RedisConfigOptions,
    inner: RedisPoolInner,
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

        let inner = RedisPoolInner::Cluster(pool);
        Ok(RedisPool { opts, inner })
    }

    /// Creates a [`RedisPool`] in multi write configuration.
    pub fn multi_write<'a>(
        primary: &str,
        secondaries: impl IntoIterator<Item = &'a str>,
        opts: RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let primary_pool = RedisPoolInner::Single(Self::client_pool(primary, &opts)?);
        let secondary_pools = secondaries
            .into_iter()
            .map(|s| Self::client_pool(s, &opts).map(RedisPoolInner::Single))
            .collect::<Result<Vec<_>, _>>()?;

        let inner = RedisPoolInner::MultiWrite(Box::new(primary_pool), secondary_pools);
        Ok(RedisPool { opts, inner })
    }

    /// Creates a [`RedisPool`] in single-node configuration.
    pub fn single(server: &str, opts: RedisConfigOptions) -> Result<Self, RedisError> {
        let pool = Self::client_pool(server, &opts)?;

        let inner = RedisPoolInner::Single(pool);
        Ok(RedisPool { opts, inner })
    }

    /// Returns a pooled connection to a client.
    pub fn client(&self) -> Result<PooledClient, RedisError> {
        let inner = Self::client_inner(&self.inner)?;
        Ok(PooledClient {
            opts: self.opts.clone(),
            inner,
        })
    }

    /// Returns information about the current state of the pool.
    pub fn stats(&self) -> Stats {
        let (connections, idle_connections) = Self::state(&self.inner);
        Stats {
            connections,
            idle_connections,
        }
    }

    /// Returns a [`Pool`] with a [`redis::Client`].
    fn client_pool(
        server: &str,
        opts: &RedisConfigOptions,
    ) -> Result<Pool<redis::Client>, RedisError> {
        Self::base_pool_builder(opts)
            .build(redis::Client::open(server).map_err(RedisError::Redis)?)
            .map_err(RedisError::Pool)
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

    /// Recursively computes a [`PooledClientInner`].
    fn client_inner(inner: &RedisPoolInner) -> Result<PooledClientInner, RedisError> {
        let inner = match inner {
            RedisPoolInner::Cluster(ref pool) => {
                PooledClientInner::Cluster(Box::new(pool.get().map_err(RedisError::Pool)?))
            }
            RedisPoolInner::MultiWrite(ref primary_pool, ref secondary_pools) => {
                let primary_client = Self::client_inner(primary_pool)?;
                let mut secondary_clients = Vec::with_capacity(secondary_pools.len());
                for secondary_pool in secondary_pools.iter() {
                    let client = Self::client_inner(secondary_pool)?;
                    secondary_clients.push(client);
                }

                PooledClientInner::MultiWrite(Box::new(primary_client), secondary_clients)
            }
            RedisPoolInner::Single(ref pool) => {
                PooledClientInner::Single(Box::new(pool.get().map_err(RedisError::Pool)?))
            }
        };

        Ok(inner)
    }

    /// Recursively computes the state of the supplied [`RedisPoolInner`].
    fn state(inner: &RedisPoolInner) -> (u32, u32) {
        match inner {
            RedisPoolInner::Cluster(p) => (p.state().connections, p.state().idle_connections),
            RedisPoolInner::MultiWrite(p, s) => {
                let (primary_connections, primary_idle_connections) = Self::state(p);
                (primary_connections, primary_idle_connections)
            }
            RedisPoolInner::Single(p) => (p.state().connections, p.state().idle_connections),
        }
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

/// Stats about how the [`RedisPool`] is performing.
pub struct Stats {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
}
