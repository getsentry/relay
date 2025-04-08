use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult};
use deadpool_redis::cluster::Manager as ClusterManager;
use deadpool_redis::Manager as SingleManager;
use futures::FutureExt;
use std::ops::{Deref, DerefMut};

use crate::redis;
use crate::redis::aio::MultiplexedConnection;
use crate::redis::cluster_async::ClusterConnection;
use crate::redis::{
    Cmd, IntoConnectionInfo, Pipeline, RedisError, RedisFuture, RedisResult, RetryMethod, Value,
};

/// A connection pool for Redis cluster deployments.
pub type CustomClusterPool = Pool<CustomClusterManager, CustomClusterConnection>;

/// A connection pool for single Redis instance deployments.
pub type CustomSinglePool = Pool<CustomSingleManager, CustomSingleConnection>;

/// A wrapper for a connection that can be tracked with metadata.
///
/// A connection is considered detached as soon as it is marked as detached and it can't be
/// un-detached.
pub struct TrackedConnection<C> {
    connection: C,
    detach: bool,
}

impl<C> TrackedConnection<C> {
    fn new(connection: C) -> Self {
        Self {
            connection,
            detach: false,
        }
    }

    /// Returns `true` when a [`RedisError`] should lead to the [`TrackedConnection`] being detached
    /// from the pool.
    fn should_be_detached(error: &RedisError) -> bool {
        match error.retry_method() {
            RetryMethod::Reconnect => true,
            RetryMethod::NoRetry => true,
            RetryMethod::RetryImmediately => false,
            RetryMethod::WaitAndRetry => false,
            RetryMethod::AskRedirect => false,
            RetryMethod::MovedRedirect => false,
            RetryMethod::ReconnectFromInitialConnections => true,
            _ => true,
        }
    }
}

impl<C: redis::aio::ConnectionLike + Send> redis::aio::ConnectionLike for TrackedConnection<C> {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        async move {
            let result = self.connection.req_packed_command(cmd).await;
            if let Err(error) = &result {
                self.detach = Self::should_be_detached(error);
            }

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
            let result = self
                .connection
                .req_packed_commands(cmd, offset, count)
                .await;
            if let Err(error) = &result {
                self.detach = Self::should_be_detached(error);
            }

            result
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        self.connection.get_db()
    }
}

impl<C> From<C> for TrackedConnection<C> {
    fn from(value: C) -> Self {
        Self::new(value)
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
#[derive(Debug)]
pub struct CustomClusterManager {
    manager: ClusterManager,
    recycle_check_frequency: usize,
}

impl CustomClusterManager {
    /// Creates a new cluster manager for the specified Redis nodes.
    ///
    /// The manager will attempt to connect to each node in the provided list and
    /// maintain connections to the Redis cluster.
    pub fn new<T: IntoConnectionInfo>(
        params: Vec<T>,
        read_from_replicas: bool,
        recycle_check_frequency: usize,
    ) -> RedisResult<Self> {
        Ok(Self {
            manager: ClusterManager::new(params, read_from_replicas)?,
            recycle_check_frequency,
        })
    }
}

impl Manager for CustomClusterManager {
    type Type = TrackedConnection<ClusterConnection>;
    type Error = RedisError;

    async fn create(&self) -> Result<TrackedConnection<ClusterConnection>, RedisError> {
        self.manager.create().await.map(TrackedConnection::from)
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
        if metrics.recycle_count % self.recycle_check_frequency != 0 {
            return Ok(());
        }

        self.manager.recycle(conn, metrics).await
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
    manager: SingleManager,
    recycle_check_frequency: usize,
}

impl CustomSingleManager {
    /// Creates a new manager for a single Redis instance.
    ///
    /// The manager will establish and maintain connections to the specified Redis
    /// instance, handling connection lifecycle and health checks.
    pub fn new<T: IntoConnectionInfo>(
        params: T,
        recycle_check_frequency: usize,
    ) -> RedisResult<Self> {
        Ok(Self {
            manager: SingleManager::new(params)?,
            recycle_check_frequency,
        })
    }
}

impl Manager for CustomSingleManager {
    type Type = TrackedConnection<MultiplexedConnection>;
    type Error = RedisError;

    async fn create(&self) -> Result<TrackedConnection<MultiplexedConnection>, RedisError> {
        self.manager.create().await.map(TrackedConnection::from)
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
        if metrics.recycle_count % self.recycle_check_frequency != 0 {
            return Ok(());
        }

        self.manager.recycle(conn, metrics).await
    }
}

impl From<Object<CustomSingleManager>> for CustomSingleConnection {
    fn from(conn: Object<CustomSingleManager>) -> Self {
        Self(conn)
    }
}
