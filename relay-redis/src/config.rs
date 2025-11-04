use serde::{Deserialize, Serialize};

/// Additional configuration options for a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RedisConfigOptions {
    /// Maximum number of connections managed by the pool.
    pub max_connections: u32,
    /// Sets the idle timeout used by the pool, in seconds.
    ///
    /// The idle timeout defines the maximum time a connection will be kept in the pool if unused.
    pub idle_timeout: u64,
    /// Sets the maximum time in seconds to wait when establishing a new Redis connection.
    ///
    /// If a connection cannot be established within this duration, it is considered a failure.
    /// Applies when the pool needs to grow or create fresh connections.
    pub create_timeout: Option<u64>,
    /// Sets the maximum time in seconds to validate an existing connection when it is recycled.
    ///
    /// Recycling involves checking whether an idle connection is still alive before reuse.
    /// If validation exceeds this timeout, the connection is discarded and a new fetch from the pool
    /// is attempted.
    pub recycle_timeout: Option<u64>,
    /// Sets the maximum time, in seconds, that a caller is allowed to wait
    /// when requesting a connection from the pool.
    ///
    /// If a connection does not become available within this period, the attempt
    /// will fail with a timeout error. This setting helps prevent indefinite
    /// blocking when the pool is exhausted.
    pub wait_timeout: Option<u64>,
    /// Sets the maximum time in seconds to wait for a result when sending a Redis command.
    ///
    /// If a command exceeds this timeout, the connection will be recycled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_timeout: Option<u64>,
    /// Sets the number of times after which the connection will check whether it is active when
    /// being recycled.
    ///
    /// A frequency of 1, means that the connection will check whether it is active every time it
    /// is recycled.
    pub recycle_check_frequency: usize,
}

impl Default for RedisConfigOptions {
    fn default() -> Self {
        Self {
            max_connections: 24,
            idle_timeout: 60,
            create_timeout: Some(3),
            recycle_timeout: Some(2),
            wait_timeout: None,
            response_timeout: Some(6),
            recycle_check_frequency: 100,
        }
    }
}
