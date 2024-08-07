use std::fmt;
use std::time::Duration;

use r2d2::{Pool, PooledConnection};
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
    Single(&'a mut redis::Connection),
}

/// A reference to a pooled connection from `PooledClient`.
pub struct Connection<'a> {
    inner: ConnectionInner<'a>,
}

impl ConnectionLike for Connection<'_> {
    fn req_packed_command(&mut self, cmd: &[u8]) -> redis::RedisResult<redis::Value> {
        match self.inner {
            ConnectionInner::Cluster(ref mut con) => con.req_packed_command(cmd),
            ConnectionInner::Single(ref mut con) => con.req_packed_command(cmd),
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        match self.inner {
            ConnectionInner::Cluster(ref mut con) => con.req_packed_commands(cmd, offset, count),
            ConnectionInner::Single(ref mut con) => con.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self.inner {
            ConnectionInner::Cluster(ref con) => con.get_db(),
            ConnectionInner::Single(ref con) => con.get_db(),
        }
    }

    fn check_connection(&mut self) -> bool {
        match self.inner {
            ConnectionInner::Cluster(ref mut con) => con.check_connection(),
            ConnectionInner::Single(ref mut con) => con.check_connection(),
        }
    }

    fn is_open(&self) -> bool {
        match self.inner {
            ConnectionInner::Cluster(ref con) => con.is_open(),
            ConnectionInner::Single(ref con) => con.is_open(),
        }
    }
}

enum PooledClientInner {
    Cluster(Box<PooledConnection<redis::cluster::ClusterClient>>),
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
        let inner = match self.inner {
            PooledClientInner::Cluster(ref mut client) => {
                client
                    .set_read_timeout(Some(Duration::from_secs(self.opts.read_timeout)))
                    .map_err(RedisError::Redis)?;
                client
                    .set_write_timeout(Some(Duration::from_secs(self.opts.write_timeout)))
                    .map_err(RedisError::Redis)?;
                ConnectionInner::Cluster(client)
            }
            PooledClientInner::Single(ref mut client) => {
                client
                    .set_read_timeout(Some(Duration::from_secs(self.opts.read_timeout)))
                    .map_err(RedisError::Redis)?;
                client
                    .set_write_timeout(Some(Duration::from_secs(self.opts.write_timeout)))
                    .map_err(RedisError::Redis)?;
                ConnectionInner::Single(client)
            }
        };

        Ok(Connection { inner })
    }
}

#[derive(Clone)]
enum RedisPoolInner {
    Cluster(Pool<redis::cluster::ClusterClient>),
    Single(Pool<redis::Client>),
}

impl fmt::Debug for RedisPoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster(_) => f.debug_tuple("Cluster").finish(),
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
    /// Creates a `RedisPool` in cluster configuration.
    pub fn cluster<'a>(
        servers: impl IntoIterator<Item = &'a str>,
        opts: RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let pool = Pool::builder()
            .max_size(opts.max_connections)
            .min_idle(opts.min_idle)
            .test_on_check_out(false)
            .max_lifetime(Some(Duration::from_secs(opts.max_lifetime)))
            .idle_timeout(Some(Duration::from_secs(opts.idle_timeout)))
            .connection_timeout(Duration::from_secs(opts.connection_timeout))
            .build(redis::cluster::ClusterClient::new(servers).map_err(RedisError::Redis)?)
            .map_err(RedisError::Pool)?;

        let inner = RedisPoolInner::Cluster(pool);
        Ok(RedisPool { opts, inner })
    }

    /// Creates a `RedisPool` in single-node configuration.
    pub fn single(server: &str, opts: RedisConfigOptions) -> Result<Self, RedisError> {
        let pool = Pool::builder()
            .max_size(opts.max_connections)
            .min_idle(opts.min_idle)
            .test_on_check_out(false)
            .max_lifetime(Some(Duration::from_secs(opts.max_lifetime)))
            .idle_timeout(Some(Duration::from_secs(opts.idle_timeout)))
            .connection_timeout(Duration::from_secs(opts.connection_timeout))
            .build(redis::Client::open(server).map_err(RedisError::Redis)?)
            .map_err(RedisError::Pool)?;

        let inner = RedisPoolInner::Single(pool);
        Ok(RedisPool { opts, inner })
    }

    /// Returns a pooled connection to a client.
    pub fn client(&self) -> Result<PooledClient, RedisError> {
        let inner = match self.inner {
            RedisPoolInner::Cluster(ref pool) => {
                PooledClientInner::Cluster(Box::new(pool.get().map_err(RedisError::Pool)?))
            }
            RedisPoolInner::Single(ref pool) => {
                PooledClientInner::Single(Box::new(pool.get().map_err(RedisError::Pool)?))
            }
        };

        Ok(PooledClient {
            opts: self.opts.clone(),
            inner,
        })
    }

    /// Returns information about the current state of the pool.
    pub fn stats(&self) -> Stats {
        let s = match &self.inner {
            RedisPoolInner::Cluster(p) => p.state(),
            RedisPoolInner::Single(p) => p.state(),
        };

        Stats {
            connections: s.connections,
            idle_connections: s.idle_connections,
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
