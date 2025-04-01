use serde::{Deserialize, Serialize};

/// Additional configuration options for a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RedisConfigOptions {
    /// Maximum number of connections managed by the pool.
    pub max_connections: u32,
    /// Minimum amount of idle connections kept alive in the pool.
    ///
    /// If not set it will default to [`Self::max_connections`].
    pub min_idle: Option<u32>,
    /// Sets the connection timeout used by the pool, in seconds.
    ///
    /// Calls to `Pool::get` will wait this long for a connection to become available before returning an error.
    pub connection_timeout: u64,
    /// Sets the maximum lifetime of connections in the pool, in seconds.
    pub max_lifetime: u64,
    /// Sets the idle timeout used by the pool, in seconds.
    pub idle_timeout: u64,
    /// Sets the read timeout out on the connection, in seconds.
    pub read_timeout: u64,
    /// Sets the write timeout on the connection, in seconds.
    pub write_timeout: u64,
    /// Sets the maximum time in seconds to wait when establishing a new Redis connection.
    ///
    /// If a connection cannot be established within this duration, it is considered a failure.
    /// Applies when the pool needs to grow or create fresh connections.
    pub create_timeout: u64,
    /// Sets the maximum time in seconds to validate an existing connection when it is recycled.
    ///
    /// Recycling involves checking whether an idle connection is still alive before reuse.
    /// If validation exceeds this timeout, the connection is discarded and a new fetch from the pool
    /// is attempted.
    pub recycle_timeout: u64,
    /// Sets the number of times after which the connection will check whether it is active when
    /// being recycled.
    ///
    /// An interval of 1, means that the connection will check whether it is active every time it
    /// is recycled.
    pub refresh_interval: usize,
}

impl Default for RedisConfigOptions {
    fn default() -> Self {
        Self {
            max_connections: 24,
            min_idle: None,
            connection_timeout: 5,
            max_lifetime: 300,
            idle_timeout: 60,
            read_timeout: 3,
            write_timeout: 3,
            create_timeout: 3,
            recycle_timeout: 2,
            refresh_interval: 100,
        }
    }
}
