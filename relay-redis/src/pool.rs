use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};

use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult};
use deadpool_redis::cluster::Manager as ClusterManager;
use deadpool_redis::Manager as SingleManager;

use crate::redis;
use crate::redis::aio::MultiplexedConnection;
use crate::redis::cluster_async::ClusterConnection;
use crate::redis::{
    Cmd, IntoConnectionInfo, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};

/// A connection pool for Redis cluster deployments.
pub type CustomClusterPool = Pool<CustomClusterManager, CustomClusterConnection>;

/// A connection pool for single Redis instance deployments.
pub type CustomSinglePool = Pool<CustomSingleManager, CustomSingleConnection>;

/// A counter that triggers at regular intervals.
///
/// [`IntervalCounter`] provides a thread-safe way to determine when a specific interval
/// has been reached. It's useful for operations that should occur periodically but not
/// on every call, such as connection health checks or cache refreshes.
#[derive(Debug)]
struct IntervalCounter {
    value: usize,
    max_value: usize,
}

impl IntervalCounter {
    /// Creates a new [`IntervalCounter`] with the specified interval.
    ///
    /// The counter will trigger (return `true` from [`Self::reached`]) every `max_size` calls.
    fn new(max_size: usize) -> Self {
        Self {
            value: 0,
            max_value: max_size,
        }
    }

    /// Checks if the interval has been reached and advances the counter.
    ///
    /// Returns `true` when the counter reaches zero, which happens every `max_value` calls.
    /// This method uses relaxed memory ordering as precise synchronization is not required
    /// for this use case.
    fn reached(&mut self) -> bool {
        let reached = self.value == 0;
        self.value = (self.value + 1) % self.max_value;
        reached
    }
}

/// A wrapper of connection that has a local counter that can be used to track the usage.
pub struct TrackedConnection<T> {
    inner: T,
    counter: IntervalCounter,
}

impl<T> TrackedConnection<T> {
    fn new(inner: T, refresh_interval: usize) -> Self {
        Self {
            inner,
            counter: IntervalCounter::new(refresh_interval),
        }
    }
}

impl<T> Deref for TrackedConnection<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for TrackedConnection<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: redis::aio::ConnectionLike> redis::aio::ConnectionLike for TrackedConnection<T> {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.inner.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        self.inner.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.inner.get_db()
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
    refresh_interval: usize,
}

impl CustomClusterManager {
    /// Creates a new cluster manager for the specified Redis nodes.
    ///
    /// The manager will attempt to connect to each node in the provided list and
    /// maintain connections to the Redis cluster.
    pub fn new<T: IntoConnectionInfo>(
        params: Vec<T>,
        read_from_replicas: bool,
        refresh_interval: usize,
    ) -> RedisResult<Self> {
        Ok(Self {
            manager: ClusterManager::new(params, read_from_replicas)?,
            refresh_interval,
        })
    }
}

impl Manager for CustomClusterManager {
    type Type = TrackedConnection<ClusterConnection>;
    type Error = RedisError;

    async fn create(&self) -> Result<TrackedConnection<ClusterConnection>, RedisError> {
        Ok(TrackedConnection::new(
            self.manager.create().await?,
            self.refresh_interval,
        ))
    }

    async fn recycle(
        &self,
        conn: &mut TrackedConnection<ClusterConnection>,
        metrics: &Metrics,
    ) -> RecycleResult<RedisError> {
        // If the interval has been reached, we optimistically assume the connection is active
        // without doing an actual `PING`.
        if !conn.counter.reached() {
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
    refresh_interval: usize,
}

impl CustomSingleManager {
    /// Creates a new manager for a single Redis instance.
    ///
    /// The manager will establish and maintain connections to the specified Redis
    /// instance, handling connection lifecycle and health checks.
    pub fn new<T: IntoConnectionInfo>(params: T, refresh_interval: usize) -> RedisResult<Self> {
        Ok(Self {
            manager: SingleManager::new(params)?,
            refresh_interval,
        })
    }
}

impl Manager for CustomSingleManager {
    type Type = TrackedConnection<MultiplexedConnection>;
    type Error = RedisError;

    async fn create(&self) -> Result<TrackedConnection<MultiplexedConnection>, RedisError> {
        Ok(TrackedConnection::new(
            self.manager.create().await?,
            self.refresh_interval,
        ))
    }

    async fn recycle(
        &self,
        conn: &mut TrackedConnection<MultiplexedConnection>,
        metrics: &Metrics,
    ) -> RecycleResult<RedisError> {
        // If the interval has been reached, we optimistically assume the connection is active
        // without doing an actual `PING`.
        if !conn.counter.reached() {
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
