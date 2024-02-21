mod cache;
mod limiter;
mod quota;
mod script;

pub use self::limiter::{RedisSetLimiter, RedisSetLimiterOptions};

/// Key prefix used for Redis keys.
const KEY_PREFIX: &str = "relay:cardinality";
/// Redis key version.
///
/// The version is embedded in the key as a static segment, increment the version whenever there are
/// breaking changes made to the keys or storage format in Redis.
const KEY_VERSION: u32 = 1;
