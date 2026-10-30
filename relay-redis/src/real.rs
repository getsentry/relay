use deadpool::Runtime;
use deadpool::managed::{BuildError, Manager, Metrics, Object, Pool, PoolError};
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
    Redis(
        #[source]
        #[from]
        redis::RedisError,
    ),

    /// An error that occurs when interacting with the Redis connection pool.
    #[error("failed to interact with the redis pool: {0}")]
    Pool(#[source] PoolError<redis::RedisError>),

    /// An error that occurs when creating a Redis connection pool.
    #[error("failed to create redis pool: {0}")]
    CreatePool(#[from] BuildError),
}

/// A collection of Redis clients used by Relay for different purposes.
///
/// This type manages separate Redis connection clients for different functionalities
/// within the Relay system, such as project configurations and rate limiting.
#[derive(Debug, Clone)]
pub struct RedisClients {
    /// The client used for project configurations
    pub project_configs: AsyncRedisClient,
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
    /// Sends commands to the primary and write commands to the secondary.
    ///
    /// Only the primary's result is returned.
    Dual {
        /// Client whose results are returned.
        primary: Box<AsyncRedisClient>,
        /// Best-effort client, errors are only logged.
        secondary: Box<AsyncRedisClient>,
    },
}

/// Commands which are duplicated to the secondary.
///
/// Scripts are treated as writes. Read-only variants like `EVALSHA_RO` are not included.
const SECONDARY_COMMANDS: &[&str] = &[
    "EVAL", "EVALSHA", "SCRIPT", "SET", "SETEX", "DEL", "INCR", "INCRBY", "DECR", "DECRBY",
    "EXPIRE", "EXPIREAT", "HSET", "HDEL", "HINCRBY",
];

/// Returns `true` if `cmd` should be duplicated to the secondary.
fn is_secondary_command(cmd: &Cmd) -> bool {
    let Some(redis::Arg::Simple(name)) = cmd.args_iter().next() else {
        return false;
    };
    SECONDARY_COMMANDS
        .iter()
        .any(|c| c.as_bytes().eq_ignore_ascii_case(name))
}

/// Returns a pipeline with only the secondary commands, `None` if there are none.
fn secondary_pipeline(pipeline: &Pipeline) -> Option<Pipeline> {
    let mut filtered = Pipeline::new();
    if pipeline.is_transaction() {
        filtered.atomic();
    }
    for cmd in pipeline.cmd_iter().filter(|cmd| is_secondary_command(cmd)) {
        filtered.add_command(cmd.clone());
    }
    (!filtered.is_empty()).then_some(filtered)
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
        name: &'static str,
        servers: impl IntoIterator<Item = &'a str>,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        let servers = servers
            .into_iter()
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();

        // We use our custom cluster manager which performs recycling in a different way from the
        // default manager.
        let manager = pool::CustomClusterManager::new(name, servers, false, opts.clone())
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
    pub fn single(
        name: &'static str,
        server: &str,
        opts: &RedisConfigOptions,
    ) -> Result<Self, RedisError> {
        // We use our custom single manager which performs recycling in a different way from the
        // default manager.
        let manager = pool::CustomSingleManager::new(name, server, opts.clone())
            .map_err(RedisError::Redis)?;

        let pool = Self::build_pool(manager, opts)?;

        Ok(AsyncRedisClient::Single(pool))
    }

    /// Creates a client which sends commands to `primary` and write commands to `secondary`.
    ///
    /// Only the primary's result is returned, errors from the secondary are only logged.
    pub fn dual(primary: AsyncRedisClient, secondary: AsyncRedisClient) -> Self {
        AsyncRedisClient::Dual {
            primary: Box::new(primary),
            secondary: Box::new(secondary),
        }
    }

    /// Acquires a connection from the pool.
    ///
    /// Returns a new [`AsyncRedisConnection`] that can be used to execute Redis commands.
    /// The connection is automatically returned to the pool when dropped.
    pub async fn get_connection(&self) -> Result<AsyncRedisConnection, RedisError> {
        match self {
            Self::Cluster(pool) => pool
                .get()
                .await
                .map(AsyncRedisConnection::Cluster)
                .map_err(RedisError::Pool),
            Self::Single(pool) => pool
                .get()
                .await
                .map(AsyncRedisConnection::Single)
                .map_err(RedisError::Pool),
            Self::Dual { primary, secondary } => {
                let primary = Box::new(Box::pin(primary.get_connection()).await?);
                let secondary = match Box::pin(secondary.get_connection()).await {
                    Ok(connection) => Some(Box::new(connection)),
                    Err(error) => {
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            "failed to acquire a secondary redis connection",
                        );
                        None
                    }
                };

                Ok(AsyncRedisConnection::Dual { primary, secondary })
            }
        }
    }

    /// Returns statistics about the current state of the connection pool.
    ///
    /// Provides information about the number of active and idle connections in the pool,
    /// which can be useful for monitoring and debugging purposes.
    pub fn stats(&self) -> RedisClientStats {
        let status = match self {
            Self::Cluster(pool) => pool.status(),
            Self::Single(pool) => pool.status(),
            Self::Dual { primary, .. } => return primary.stats(),
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
            Self::Dual { primary, secondary } => {
                primary.retain(&mut predicate);
                secondary.retain(&mut predicate);
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
            AsyncRedisClient::Dual { .. } => write!(f, "AsyncRedisPool::Dual"),
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
    /// Sends commands to the primary and write commands to the secondary.
    Dual {
        /// Connection whose results are returned.
        primary: Box<AsyncRedisConnection>,
        /// Best-effort connection, `None` if it could not be acquired.
        secondary: Option<Box<AsyncRedisConnection>>,
    },
}

impl std::fmt::Debug for AsyncRedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Cluster(_) => "Cluster",
            Self::Single(_) => "Single",
            Self::Dual { .. } => "Dual",
        };
        f.debug_tuple(name).finish()
    }
}

/// Logs a failed command sent to the secondary.
fn log_secondary_error<T>(result: Option<redis::RedisResult<T>>) {
    if let Some(Err(error)) = result {
        relay_log::error!(
            error = &error as &dyn std::error::Error,
            "failed to send command to secondary redis",
        );
    }
}

impl redis::aio::ConnectionLike for AsyncRedisConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Self::Cluster(conn) => conn.req_packed_command(cmd),
            Self::Single(conn) => conn.req_packed_command(cmd),
            Self::Dual { primary, secondary } => Box::pin(async move {
                let secondary = async {
                    match secondary {
                        Some(secondary) if is_secondary_command(cmd) => {
                            Some(secondary.req_packed_command(cmd).await)
                        }
                        _ => None,
                    }
                };
                let (result, secondary_result) =
                    futures::future::join(primary.req_packed_command(cmd), secondary).await;
                log_secondary_error(secondary_result);
                result
            }),
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
            Self::Dual { primary, secondary } => Box::pin(async move {
                let secondary = async {
                    let secondary = secondary.as_mut()?;
                    let filtered = secondary_pipeline(cmd)?;
                    // Mirrors the offset and count of `Pipeline::query_async`.
                    let (offset, count) = match filtered.is_transaction() {
                        true => (filtered.len() + 1, 1),
                        false => (0, filtered.len()),
                    };
                    Some(
                        secondary
                            .req_packed_commands(&filtered, offset, count)
                            .await,
                    )
                };
                let (result, secondary_result) = futures::future::join(
                    primary.req_packed_commands(cmd, offset, count),
                    secondary,
                )
                .await;
                log_secondary_error(secondary_result);
                result
            }),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Cluster(conn) => conn.get_db(),
            Self::Single(conn) => conn.get_db(),
            Self::Dual { primary, .. } => primary.get_db(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_secondary_command() {
        assert!(is_secondary_command(&redis::cmd("EVALSHA")));
        assert!(is_secondary_command(&redis::cmd("evalsha")));
        assert!(is_secondary_command(&redis::cmd("SCRIPT")));
        assert!(is_secondary_command(&redis::cmd("SET")));

        assert!(!is_secondary_command(&redis::cmd("GET")));
        assert!(!is_secondary_command(&redis::cmd("MGET")));
        assert!(!is_secondary_command(&redis::cmd("EVALSHA_RO")));
    }

    #[test]
    fn test_secondary_pipeline() {
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("GET")
            .arg("a")
            .cmd("SET")
            .arg("b")
            .arg(1);
        let filtered = secondary_pipeline(&pipeline).unwrap();
        assert!(filtered.is_transaction());
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered.get_packed_pipeline(),
            redis::pipe()
                .atomic()
                .cmd("SET")
                .arg("b")
                .arg(1)
                .get_packed_pipeline()
        );

        let mut reads = redis::pipe();
        reads.cmd("GET").arg("a").cmd("MGET").arg("b");
        assert!(secondary_pipeline(&reads).is_none());
    }
}
