use std::sync::atomic::{AtomicUsize, Ordering};

use deadpool::managed;
use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult};
use deadpool_redis::cluster::Manager as ClusterManager;
use deadpool_redis::Manager as SingleManager;

use crate::redis;
use crate::redis::aio::MultiplexedConnection;
use crate::redis::cluster::{ClusterClient, ClusterClientBuilder};
use crate::redis::cluster_async::ClusterConnection;
use crate::redis::{
    Client, Cmd, IntoConnectionInfo, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};

/// A connection pool for Redis cluster deployments.
pub type ClusterPool = Pool<CustomClusterManager, CustomClusterConnection>;

/// A connection pool for single Redis instance deployments.
pub type SinglePool = Pool<CustomSingleManager, CustomSingleConnection>;

#[derive(Debug)]
struct IntervalCounter {
    value: AtomicUsize,
    max_value: usize,
}

impl IntervalCounter {
    fn new(max_size: usize) -> Self {
        Self {
            value: AtomicUsize::new(0),
            max_value: max_size,
        }
    }

    fn is_reached(&self) -> bool {
        let mut reached = false;
        let value = self.value.load(Ordering::Relaxed);
        if value == 0 {
            reached = true;
        };

        // We do not want to use CAS since it's unnecessary overhead because we don't need any
        // synchronization.
        let next_value = (value + 1) % self.max_value;
        self.value.store(next_value, Ordering::Relaxed);

        reached
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
    interval_counter: IntervalCounter,
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
            interval_counter: IntervalCounter::new(refresh_interval),
        })
    }
}

impl Manager for CustomClusterManager {
    type Type = ClusterConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<ClusterConnection, RedisError> {
        self.manager.create().await
    }

    async fn recycle(
        &self,
        conn: &mut ClusterConnection,
        metrics: &Metrics,
    ) -> RecycleResult<RedisError> {
        // If the interval has been reached, we optimistically assume the connection is active
        // without doing an actual `PING`.
        if !self.interval_counter.is_reached() {
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
    interval_counter: IntervalCounter,
}

impl CustomSingleManager {
    /// Creates a new manager for a single Redis instance.
    ///
    /// The manager will establish and maintain connections to the specified Redis
    /// instance, handling connection lifecycle and health checks.
    pub fn new<T: IntoConnectionInfo>(params: T, refresh_interval: usize) -> RedisResult<Self> {
        Ok(Self {
            manager: SingleManager::new(params)?,
            interval_counter: IntervalCounter::new(refresh_interval),
        })
    }
}

impl Manager for CustomSingleManager {
    type Type = MultiplexedConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<MultiplexedConnection, RedisError> {
        self.manager.create().await
    }

    async fn recycle(
        &self,
        conn: &mut MultiplexedConnection,
        metrics: &Metrics,
    ) -> RecycleResult<RedisError> {
        // If the interval has been reached, we optimistically assume the connection is active
        // without doing an actual `PING`.
        if !self.interval_counter.is_reached() {
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
