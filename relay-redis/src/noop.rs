use failure::Fail;

use relay_config::Config;

/// This is an unconstructable type to make `Option<RedisPool>` zero-sized.
#[derive(Clone)]
pub enum RedisPool {}

/// An error returned from `RedisPool`.
#[derive(Debug, Fail)]
#[fail(display = "unreachable")]
pub struct RedisError;

impl RedisPool {
    /// Creates a `RedisPool` from the given configuration.
    ///
    /// Always returns `Ok(None)`.
    pub fn from_config(_config: &Config) -> Result<Option<RedisPool>, RedisError> {
        Ok(None)
    }
}
