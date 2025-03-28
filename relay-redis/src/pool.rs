use std::sync::atomic::{AtomicUsize, Ordering};

use deadpool::managed;
use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult};

use crate::redis;
use crate::redis::aio::MultiplexedConnection;
use crate::redis::cluster::{ClusterClient, ClusterClientBuilder};
use crate::redis::cluster_async::ClusterConnection;
use crate::redis::{
    Client, Cmd, IntoConnectionInfo, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};

pub type ClusterPool = Pool<CustomClusterManager, CustomClusterConnection>;
pub type SinglePool = Pool<CustomSingleManager, CustomSingleConnection>;

pub struct CustomClusterManager {
    client: ClusterClient,
    ping_number: AtomicUsize,
}

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

impl std::fmt::Debug for CustomClusterManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("client", &format!("{:p}", &self.client))
            .field("ping_number", &self.ping_number)
            .finish()
    }
}

impl CustomClusterManager {
    /// Creates a new [`CustomClusterManager`] from the given `params`.
    ///
    /// # Errors
    ///
    /// If establishing a new [`ClusterClientBuilder`] fails.
    pub fn new<T: IntoConnectionInfo>(
        params: Vec<T>,
        read_from_replicas: bool,
    ) -> RedisResult<Self> {
        let mut client = ClusterClientBuilder::new(params);
        if read_from_replicas {
            client = client.read_from_replicas();
        }
        Ok(Self {
            client: client.build()?,
            ping_number: AtomicUsize::new(0),
        })
    }
}

impl Manager for CustomClusterManager {
    type Type = ClusterConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<ClusterConnection, RedisError> {
        let conn = self.client.get_async_connection().await?;
        Ok(conn)
    }

    // TODO: implement custom recycling logic which sends a ping only every x connections.
    async fn recycle(
        &self,
        conn: &mut ClusterConnection,
        _: &Metrics,
    ) -> RecycleResult<RedisError> {
        let ping_number = self.ping_number.fetch_add(1, Ordering::Relaxed).to_string();
        let n = redis::cmd("PING")
            .arg(&ping_number)
            .query_async::<String>(conn)
            .await?;
        if n == ping_number {
            Ok(())
        } else {
            Err(managed::RecycleError::message("Invalid PING response"))
        }
    }
}

impl From<Object<CustomClusterManager>> for CustomClusterConnection {
    fn from(conn: Object<CustomClusterManager>) -> Self {
        Self(conn)
    }
}

pub struct CustomSingleManager {
    client: Client,
    ping_number: AtomicUsize,
}

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

impl CustomSingleManager {
    /// Creates a new [`CustomSingleManager`] from the given `params`.
    ///
    /// # Errors
    ///
    /// If establishing a new [`Client`] fails.
    pub fn new<T: IntoConnectionInfo>(params: T) -> RedisResult<Self> {
        Ok(Self {
            client: Client::open(params)?,
            ping_number: AtomicUsize::new(0),
        })
    }
}

impl Manager for CustomSingleManager {
    type Type = MultiplexedConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<MultiplexedConnection, RedisError> {
        let conn = self.client.get_multiplexed_async_connection().await?;
        Ok(conn)
    }

    // TODO: implement custom recycling logic which sends a ping only every x connections.
    async fn recycle(
        &self,
        conn: &mut MultiplexedConnection,
        _: &Metrics,
    ) -> RecycleResult<RedisError> {
        let ping_number = self.ping_number.fetch_add(1, Ordering::Relaxed).to_string();
        // Using pipeline to avoid roundtrip for UNWATCH
        let (n,) = Pipeline::with_capacity(2)
            .cmd("UNWATCH")
            .ignore()
            .cmd("PING")
            .arg(&ping_number)
            .query_async::<(String,)>(conn)
            .await?;
        if n == ping_number {
            Ok(())
        } else {
            Err(managed::RecycleError::message("Invalid PING response"))
        }
    }
}

impl From<Object<CustomSingleManager>> for CustomSingleConnection {
    fn from(conn: Object<CustomSingleManager>) -> Self {
        Self(conn)
    }
}
