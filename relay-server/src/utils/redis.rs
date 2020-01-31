#[cfg(feature = "processing")]
pub use real_implementation::*;

#[cfg(not(feature = "processing"))]
pub use noop_implementation::*;

#[cfg(feature = "processing")]
mod real_implementation {
    use failure::Fail;
    use r2d2::Pool;

    use relay_config::{Config, Redis};

    #[derive(Debug, Fail)]
    pub enum RedisError {
        #[fail(display = "failed to connect to redis")]
        RedisPool(#[cause] r2d2::Error),

        #[fail(display = "failed to talk to redis")]
        Redis(#[cause] redis::RedisError),
    }

    /// "Abstraction" over cluster vs non-cluster mode.
    ///
    /// Even just writing a method that takes a command and executes it doesn't really work because
    /// there's both `Cmd` and `ScriptInvocation` to take care of, and both have sync vs async
    /// APIs.
    ///
    /// Basically don't waste your time here, if you want to abstract over this, consider
    /// upstreaming to the redis crate.
    #[derive(Clone)]
    pub enum RedisPool {
        Cluster(Pool<redis::cluster::ClusterClient>),
        Single(Pool<redis::Client>),
    }

    impl RedisPool {
        pub fn from_config(config: &Config) -> Result<Option<RedisPool>, RedisError> {
            if config.processing_enabled() {
                match config.redis() {
                    Some(Redis::Cluster { ref cluster_nodes }) => {
                        RedisPool::cluster(cluster_nodes.iter().map(String::as_str).collect())
                            .map(Some)
                    }
                    Some(Redis::Single(ref server)) => RedisPool::single(&server).map(Some),
                    None => Ok(None),
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

    // This is an unconstructable type to make Option<RedisPool> zero-sized
    #[derive(Clone)]
    pub enum RedisPool {}

    #[derive(Debug, Fail)]
    #[fail(display = "unreachable")]
    pub struct RedisError;

    impl RedisPool {
        pub fn from_config(_config: &Config) -> Result<Option<RedisPool>, RedisError> {
            Ok(None)
        }
    }
}
