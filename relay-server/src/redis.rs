use failure::Fail;
use relay_config::Redis;

use r2d2::Pool;

#[derive(Debug, Fail)]
pub enum RedisError {
    #[fail(display = "failed to connect to redis")]
    RedisPool(#[cause] r2d2::Error),

    #[fail(display = "failed to talk to redis")]
    Redis(#[cause] redis::RedisError),
}

/// "Abstraction" over cluster vs non-cluster mode. This probably can never really abstract a lot
/// without changes in r2d2 and redis-rs.
#[derive(Clone)]
pub enum RedisPool {
    Cluster(Pool<redis::cluster::ClusterClient>),
    Single(Pool<redis::Client>),
}

impl RedisPool {
    pub fn from_config(config: &Redis) -> Result<Self, RedisError> {
        match config {
            Redis::Cluster { cluster_nodes } => {
                RedisPool::cluster(cluster_nodes.iter().map(String::as_str).collect())
            }
            Redis::Single(ref server) => RedisPool::single(&server),
        }
    }

    pub fn cluster(servers: Vec<&str>) -> Result<Self, RedisError> {
        Ok(RedisPool::Cluster(
            Pool::builder()
                .max_size(24)
                .build(redis::cluster::ClusterClient::open(servers).map_err(RedisError::Redis)?)
                .map_err(RedisError::RedisPool)?,
        ))
    }

    pub fn single(server: &str) -> Result<Self, RedisError> {
        Ok(RedisPool::Single(
            Pool::builder()
                .max_size(24)
                .build(redis::Client::open(server).map_err(RedisError::Redis)?)
                .map_err(RedisError::RedisPool)?,
        ))
    }
}
