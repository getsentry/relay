use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult};
use futures::FutureExt;
use redis::AsyncConnectionConfig;
use redis::cluster::ClusterClientBuilder;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use crate::RedisConfigOptions;
use crate::redis;
use crate::redis::aio::MultiplexedConnection;
use crate::redis::cluster_async::ClusterConnection;
use crate::redis::{
    Cmd, IntoConnectionInfo, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};
use crate::statsd::RedisTimers;

use relay_statsd::metric;

/// A connection pool for Redis cluster deployments.
pub type CustomClusterPool = Pool<CustomClusterManager, CustomClusterConnection>;

/// A connection pool for single Redis instance deployments.
pub type CustomSinglePool = Pool<CustomSingleManager, CustomSingleConnection>;

/// A wrapper for a connection that can be tracked with metadata.
///
/// A connection is considered detached as soon as it is marked as detached and it can't be
/// un-detached.
///
/// This connection will automatically emit metrics after sending Redis commands or pipelines.
pub struct TrackedConnection<C> {
    connection: C,
    detach: bool,
    client_name: &'static str,
}

impl<C> TrackedConnection<C> {
    fn new(client_name: &'static str, connection: C) -> Self {
        Self {
            connection,
            detach: false,
            client_name,
        }
    }

    /// Returns `true` when the result of a Redis call should lead to the [`TrackedConnection`] being detached
    /// from the pool.
    ///
    /// An `Ok` result never leads to the connection being detached.
    /// A [`RedisError`] leads to the connection being detached if it is either unrecoverable or a timeout.
    /// In case of timeout, we would rather close a potentially dead connection and establish a new one.
    fn should_be_detached<T>(result: Result<T, &RedisError>) -> bool {
        result.is_err_and(|error| error.is_unrecoverable_error() || error.is_timeout())
    }
}

impl<C: redis::aio::ConnectionLike + Send> redis::aio::ConnectionLike for TrackedConnection<C> {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        async move {
            let start = Instant::now();
            let result = self.connection.req_packed_command(cmd).await;
            let elapsed = start.elapsed();

            self.detach |= Self::should_be_detached(result.as_ref());
            emit_metrics(result.as_ref(), elapsed, self.client_name, cmd_name(cmd));

            result
        }
        .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        async move {
            let start = Instant::now();
            let result = self
                .connection
                .req_packed_commands(cmd, offset, count)
                .await;
            let elapsed = start.elapsed();

            self.detach |= Self::should_be_detached(result.as_ref());
            emit_metrics(result.as_ref(), elapsed, self.client_name, "pipeline");

            result
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        self.connection.get_db()
    }
}

impl<C> Deref for TrackedConnection<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<C> DerefMut for TrackedConnection<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

/// A managed Redis cluster connection that implements [`redis::aio::ConnectionLike`].
///
/// This type provides access to the underlying Redis cluster connection while managing its lifecycle.
/// It is designed to be used with connection pools to efficiently handle Redis cluster operations.
pub struct CustomClusterConnection(Object<CustomClusterManager>);

impl redis::aio::ConnectionLike for CustomClusterConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.0.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        self.0.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.0.get_db()
    }
}

/// Manages Redis cluster connections and their lifecycle.
///
/// This manager handles the creation and recycling of Redis cluster connections,
/// ensuring proper connection health through periodic PING checks. It supports both
/// primary and replica nodes, with optional read-from-replicas functionality.
pub struct CustomClusterManager {
    name: &'static str,
    client: redis::cluster::ClusterClient,
    ping_number: AtomicUsize,
    recycle_check_frequency: usize,
}

impl CustomClusterManager {
    /// Creates a new cluster manager for the specified Redis nodes.
    ///
    /// The manager will attempt to connect to each node in the provided list and
    /// maintain connections to the Redis cluster.
    pub fn new<T: IntoConnectionInfo>(
        name: &'static str,
        params: Vec<T>,
        read_from_replicas: bool,
        options: RedisConfigOptions,
    ) -> RedisResult<Self> {
        let mut client = ClusterClientBuilder::new(params);

        if read_from_replicas {
            client = client.read_from_replicas();
        }
        if let Some(response_timeout) = options.response_timeout {
            client = client.response_timeout(Duration::from_secs(response_timeout));
        }

        Ok(Self {
            name,
            client: client.build()?,
            ping_number: AtomicUsize::new(0),
            recycle_check_frequency: options.recycle_check_frequency,
        })
    }
}

impl Manager for CustomClusterManager {
    type Type = TrackedConnection<ClusterConnection>;
    type Error = RedisError;

    async fn create(&self) -> Result<TrackedConnection<ClusterConnection>, RedisError> {
        self.client
            .get_async_connection()
            .await
            .map(|c| TrackedConnection::new(self.name, c))
    }

    async fn recycle(
        &self,
        conn: &mut TrackedConnection<ClusterConnection>,
        metrics: &Metrics,
    ) -> RecycleResult<RedisError> {
        // If the connection is marked to be detached, we return and error, signaling that this
        // connection must be detached from the pool.
        if conn.detach {
            return Err(RecycleError::Message(
                "the tracked connection was marked as detached".into(),
            ));
        }

        // If the interval has been reached, we optimistically assume the connection is active
        // without doing an actual `PING`.
        if !metrics
            .recycle_count
            .is_multiple_of(self.recycle_check_frequency)
        {
            return Ok(());
        }

        // Copied from deadpool_redis::cluster::Manager
        let ping_number = self.ping_number.fetch_add(1, Ordering::Relaxed).to_string();
        let n = redis::cmd("PING")
            .arg(&ping_number)
            .query_async::<String>(conn)
            .await?;
        if n == ping_number {
            Ok(())
        } else {
            Err(RecycleError::message("Invalid PING response"))
        }
    }
}

impl From<Object<CustomClusterManager>> for CustomClusterConnection {
    fn from(conn: Object<CustomClusterManager>) -> Self {
        Self(conn)
    }
}

/// A managed Redis connection that implements [`redis::aio::ConnectionLike`].
///
/// This type provides access to the underlying Redis connection while managing its lifecycle.
/// It is designed to be used with connection pools to efficiently handle Redis operations.
pub struct CustomSingleConnection(Object<CustomSingleManager>);

impl redis::aio::ConnectionLike for CustomSingleConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.0.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        self.0.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.0.get_db()
    }
}

/// Manages single Redis instance connections and their lifecycle.
///
/// This manager handles the creation and recycling of Redis connections,
/// ensuring proper connection health through periodic PING checks. It supports
/// multiplexed connections for efficient handling of multiple operations.
pub struct CustomSingleManager {
    name: &'static str,
    client: redis::Client,
    ping_number: AtomicUsize,
    connection_config: AsyncConnectionConfig,
    recycle_check_frequency: usize,
}

impl CustomSingleManager {
    /// Creates a new manager for a single Redis instance.
    ///
    /// The manager will establish and maintain connections to the specified Redis
    /// instance, handling connection lifecycle and health checks.
    pub fn new<T: IntoConnectionInfo>(
        name: &'static str,
        params: T,
        options: RedisConfigOptions,
    ) -> RedisResult<Self> {
        let mut connection_config = AsyncConnectionConfig::new();
        if let Some(response_timeout) = options.response_timeout {
            connection_config =
                connection_config.set_response_timeout(Duration::from_secs(response_timeout));
        }
        Ok(Self {
            name,
            client: redis::Client::open(params)?,
            ping_number: AtomicUsize::new(0),
            connection_config,
            recycle_check_frequency: options.recycle_check_frequency,
        })
    }
}

impl Manager for CustomSingleManager {
    type Type = TrackedConnection<MultiplexedConnection>;
    type Error = RedisError;

    async fn create(&self) -> Result<TrackedConnection<MultiplexedConnection>, RedisError> {
        self.client
            .get_multiplexed_async_connection_with_config(&self.connection_config)
            .await
            .map(|c| TrackedConnection::new(self.name, c))
    }

    async fn recycle(
        &self,
        conn: &mut TrackedConnection<MultiplexedConnection>,
        metrics: &Metrics,
    ) -> RecycleResult<RedisError> {
        // If the connection is marked to be detached, we return and error, signaling that this
        // connection must be detached from the pool.
        if conn.detach {
            return Err(RecycleError::Message(
                "the tracked connection was marked as detached".into(),
            ));
        }

        // If the interval has been reached, we optimistically assume the connection is active
        // without doing an actual `PING`.
        if !metrics
            .recycle_count
            .is_multiple_of(self.recycle_check_frequency)
        {
            return Ok(());
        }

        // Copied from deadpool_redis::Manager::recycle
        let ping_number = self.ping_number.fetch_add(1, Ordering::Relaxed).to_string();
        // Using pipeline to avoid roundtrip for UNWATCH
        let (n,) = redis::Pipeline::with_capacity(2)
            .cmd("UNWATCH")
            .ignore()
            .cmd("PING")
            .arg(&ping_number)
            .query_async::<(String,)>(conn)
            .await?;
        if n == ping_number {
            Ok(())
        } else {
            Err(RecycleError::message("Invalid PING response"))
        }
    }
}

impl From<Object<CustomSingleManager>> for CustomSingleConnection {
    fn from(conn: Object<CustomSingleManager>) -> Self {
        Self(conn)
    }
}

fn emit_metrics<T>(result: Result<T, &RedisError>, elapsed: Duration, client: &str, cmd: &str) {
    let result = match result {
        Ok(_) => "ok",
        Err(e) if e.is_timeout() => "timeout",
        Err(_) => "error",
    };

    metric!(
        timer(RedisTimers::CommandExecuted) = elapsed,
        result = result,
        client = client,
        cmd = cmd,
    );
}

/// Extracts a [`Cmd`]'s name from its first argument.
fn cmd_name(cmd: &Cmd) -> &str {
    match cmd.args_iter().next() {
        Some(redis::Arg::Simple(data)) => std::str::from_utf8(data).unwrap_or("<unknown>"),
        Some(redis::Arg::Cursor) | None => "<unknown>",
    }
}
