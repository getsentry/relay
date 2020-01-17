#[cfg(feature = "processing")]
pub use real_implementation::*;

#[cfg(not(feature = "processing"))]
pub use noop_implementation::*;

#[cfg(feature = "processing")]
mod real_implementation {
    use failure::Fail;
    use r2d2::Pool;

    use relay_config::{Config, Redis};

    pub type OptionalRedisPool = Option<RedisPool>;

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
        pub fn from_config(config: &Config) -> Result<OptionalRedisPool, RedisError> {
            if config.processing_enabled() {
                match config.redis() {
                    Redis::Cluster { cluster_nodes } => {
                        RedisPool::cluster(cluster_nodes.iter().map(String::as_str).collect())
                            .map(Some)
                    }
                    Redis::Single(ref server) => RedisPool::single(&server).map(Some),
                }
            } else {
                Ok(None)
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
}

#[cfg(not(feature = "processing"))]
mod noop_implementation {
    use failure::Fail;

    use relay_config::Config;

    pub type OptionalRedisPool = ();
    pub struct RedisPool;

    #[derive(Debug, Fail)]
    #[fail(display = "unreachable")]
    pub struct RedisError;

    impl RedisPool {
        pub fn from_config(_config: &Config) -> Result<OptionalRedisPool, RedisError> {
            Ok(())
        }
    }
}
