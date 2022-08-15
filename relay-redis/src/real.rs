use failure::Fail;
use r2d2::{Pool, PooledConnection};
use redis::ConnectionLike;

use crate::config::{RedisConfig, RedisConfigOptions};

pub use redis;

/// An error returned from `RedisPool`.
#[derive(Debug, Fail)]
pub enum RedisError {
    /// Failure in r2d2 pool.
    #[fail(display = "failed to pool redis connection")]
    Pool(#[cause] r2d2::Error),

    /// Failure in Redis communication.
    #[fail(display = "failed to communicate with redis")]
    Redis(#[cause] redis::RedisError),
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
    Cluster(PooledConnection<redis::cluster::ClusterClient>),
    Single(PooledConnection<redis::Client>),
}

/// A pooled Redis client.
pub struct PooledClient {
    inner: PooledClientInner,
}

impl PooledClient {
    /// Returns a pooled connection to this client.
    pub fn connection(&mut self) -> Connection<'_> {
        let inner = match self.inner {
            PooledClientInner::Cluster(ref mut client) => ConnectionInner::Cluster(&mut **client),
            PooledClientInner::Single(ref mut client) => ConnectionInner::Single(&mut **client),
        };

        Connection { inner }
    }
}

#[derive(Clone)]
enum RedisPoolInner {
    Cluster(Pool<redis::cluster::ClusterClient>),
    Single(Pool<redis::Client>),
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
pub struct RedisPool {
    inner: RedisPoolInner,
}

impl RedisPool {
    /// Creates a `RedisPool` from configuration.
    pub fn new(config: &RedisConfig) -> Result<Self, RedisError> {
        match config {
            RedisConfig::Cluster {
                ref cluster_nodes,
                ref options,
            } => {
                let servers = cluster_nodes.iter().map(String::as_str).collect();
                Self::cluster(servers, options)
            }
            RedisConfig::Single(ref server) => Self::single(server, &RedisConfigOptions::default()),
            RedisConfig::SingleWithOpts {
                ref server,
                ref options,
            } => Self::single(server, options),
        }
    }

    /// Creates a `RedisPool` in cluster configuration.
    pub fn cluster(servers: Vec<&str>, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        let pool = Pool::builder()
            .max_size(opts.max_connections)
            .test_on_check_out(opts.test_on_check_out)
            .build(redis::cluster::ClusterClient::open(servers).map_err(RedisError::Redis)?)
            .map_err(RedisError::Pool)?;

        let inner = RedisPoolInner::Cluster(pool);
        Ok(RedisPool { inner })
    }

    /// Creates a `RedisPool` in single-node configuration.
    pub fn single(server: &str, opts: &RedisConfigOptions) -> Result<Self, RedisError> {
        let pool = Pool::builder()
            .max_size(opts.max_connections)
            .test_on_check_out(opts.test_on_check_out)
            .build(redis::Client::open(server).map_err(RedisError::Redis)?)
            .map_err(RedisError::Pool)?;

        let inner = RedisPoolInner::Single(pool);
        Ok(RedisPool { inner })
    }

    /// Returns a pooled connection to a client.
    pub fn client(&self) -> Result<PooledClient, RedisError> {
        let inner = match self.inner {
            RedisPoolInner::Cluster(ref pool) => {
                PooledClientInner::Cluster(pool.get().map_err(RedisError::Pool)?)
            }
            RedisPoolInner::Single(ref pool) => {
                PooledClientInner::Single(pool.get().map_err(RedisError::Pool)?)
            }
        };

        Ok(PooledClient { inner })
    }
}
