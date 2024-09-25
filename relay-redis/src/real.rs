use std::fmt;
use std::time::Duration;

use r2d2::{Builder, ManageConnection, Pool, PooledConnection};
pub use redis;
use redis::ConnectionLike;
use thiserror::Error;

use crate::config::RedisConfigOptions;

/// An error returned from `RedisPool`.
#[derive(Debug, Error)]
pub enum RedisError {
    /// Failure in r2d2 pool.
    #[error("failed to pool redis connection")]
    Pool(#[source] r2d2::Error),

    /// Failure in Redis communication.
    #[error("failed to communicate with redis")]
    Redis(#[source] redis::RedisError),
}

enum ConnectionInner<'a> {
    Cluster(&'a mut redis::cluster::ClusterConnection),
    MultiWrite(Vec<ConnectionInner<'a>>),
    Single(&'a mut redis::Connection),
}

/// A reference to a pooled connection from `PooledClient`.
pub struct Connection<'a> {
    inner: ConnectionInner<'a>,
}

impl Connection<'_> {
    fn req_packed_command_inner(
        inner: &mut ConnectionInner<'_>,
        cmd: &[u8],
    ) -> redis::RedisResult<redis::Value> {
        match inner {
            ConnectionInner::Cluster(ref mut con) => con.req_packed_command(cmd),
            ConnectionInner::MultiWrite(ref mut connections) => {
                let mut first_result = None;
                for connection in connections.iter_mut() {
                    let result = Self::req_packed_command_inner(connection, cmd);
                    if first_result.is_none() {
                        first_result = Some(result)
                    }
                }

                first_result
                    .expect("The multi-write Redis connection should have at least one connection")
            }
            ConnectionInner::Single(ref mut con) => con.req_packed_command(cmd),
        }
    }

    fn req_packed_commands_inner(
        inner: &mut ConnectionInner<'_>,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        match inner {
            ConnectionInner::Cluster(ref mut con) => con.req_packed_commands(cmd, offset, count),
            ConnectionInner::MultiWrite(ref mut connections) => {
                let mut first_result = None;
                for connection in connections.iter_mut() {
                    let result = Self::req_packed_commands_inner(connection, cmd, offset, count);
                    if first_result.is_none() {
                        first_result = Some(result)
                    }
                }

                first_result
                    .expect("The multi-write Redis connection should have at least one connection")
            }
            ConnectionInner::Single(ref mut con) => con.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(inner: &ConnectionInner<'_>) -> i64 {
        match inner {
            ConnectionInner::Cluster(ref con) => con.get_db(),
            ConnectionInner::MultiWrite(ref connections) => Self::get_db(
                connections
                    .iter()
                    .next()
                    .expect("The multi-write Redis connection should have at least one connection"),
            ),
            ConnectionInner::Single(ref con) => con.get_db(),
        }
    }

    fn check_connection_inner(inner: &mut ConnectionInner<'_>) -> bool {
        match inner {
            ConnectionInner::Cluster(ref mut con) => con.check_connection(),
            ConnectionInner::MultiWrite(ref mut connections) => connections
                .iter_mut()
                .all(|c| Self::check_connection_inner(c)),
            ConnectionInner::Single(ref mut con) => con.check_connection(),
        }
    }

    fn is_open_inner(inner: &ConnectionInner<'_>) -> bool {
        match inner {
            ConnectionInner::Cluster(ref con) => con.is_open(),
            ConnectionInner::MultiWrite(ref connections) => {
                connections.iter().all(|c| Self::is_open_inner(c))
            }
            ConnectionInner::Single(ref con) => con.is_open(),
        }
    }
}

impl ConnectionLike for Connection<'_> {
    fn req_packed_command(&mut self, cmd: &[u8]) -> redis::RedisResult<redis::Value> {
        Self::req_packed_command_inner(&mut self.inner, cmd)
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        Self::req_packed_commands_inner(&mut self.inner, cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        Self::get_db(&self.inner)
    }

    fn check_connection(&mut self) -> bool {
        Self::check_connection_inner(&mut self.inner)
    }

    fn is_open(&self) -> bool {
        Self::is_open_inner(&self.inner)
    }
}

enum PooledClientInner {
    Cluster(Box<PooledConnection<redis::cluster::ClusterClient>>),
    MultiWrite(Vec<PooledClientInner>),
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
            PooledClientInner::MultiWrite(ref mut clients) => {
                let mut connections = Vec::with_capacity(clients.len());
                for client in clients.iter_mut() {
                    let connection = Self::connection_inner(client, opts)?;
                    connections.push(connection);
                }

                ConnectionInner::MultiWrite(connections)
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
    MultiWrite(Vec<RedisPoolInner>),
    Single(Pool<redis::Client>),
}

impl fmt::Debug for RedisPoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster(_) => f.debug_tuple("Cluster").finish(),
            Self::MultiWrite(_) => f.debug_tuple("MultiWrite").finish(),
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
        servers: impl IntoIterator<Item = &'a str>,
        opts: RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let pools = servers
            .into_iter()
            .map(|s| Self::client_pool(s, &opts))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(RedisPoolInner::Single)
            .collect();

        let inner = RedisPoolInner::MultiWrite(pools);
        Ok(RedisPool { opts, inner })
    }

    /// Creates a [`RedisPool`] in single-node configuration.
    pub fn single(server: &str, opts: RedisConfigOptions) -> Result<Self, RedisError> {
        let pool = Self::client_pool(server, &opts)?;

        let inner = RedisPoolInner::Single(pool);
        Ok(RedisPool { opts, inner })
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

    /// Returns a pooled connection to a client.
    pub fn client(&self) -> Result<PooledClient, RedisError> {
        let inner = Self::client_inner(&self.inner)?;
        Ok(PooledClient {
            opts: self.opts.clone(),
            inner,
        })
    }

    /// Recursively computes a [`PooledClientInner`].
    fn client_inner(inner: &RedisPoolInner) -> Result<PooledClientInner, RedisError> {
        let inner = match inner {
            RedisPoolInner::Cluster(ref pool) => {
                PooledClientInner::Cluster(Box::new(pool.get().map_err(RedisError::Pool)?))
            }
            RedisPoolInner::MultiWrite(ref pools) => {
                let mut clients = Vec::with_capacity(pools.len());
                for pool in pools.iter() {
                    let client = Self::client_inner(pool)?;
                    clients.push(client);
                }

                PooledClientInner::MultiWrite(clients)
            }
            RedisPoolInner::Single(ref pool) => {
                PooledClientInner::Single(Box::new(pool.get().map_err(RedisError::Pool)?))
            }
        };

        Ok(inner)
    }

    /// Returns information about the current state of the pool.
    pub fn stats(&self) -> Stats {
        let (connections, idle_connections) = Self::state(&self.inner);
        Stats {
            connections,
            idle_connections,
        }
    }

    /// Recursively computes the state of the supplied [`RedisPoolInner`].
    fn state(inner: &RedisPoolInner) -> (u32, u32) {
        match inner {
            RedisPoolInner::Cluster(p) => (p.state().connections, p.state().idle_connections),
            RedisPoolInner::MultiWrite(p) => p.iter().fold((0, 0), |(c, i), p| {
                let (connections, idle_connections) = Self::state(p);
                (c + connections, i + idle_connections)
            }),
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
