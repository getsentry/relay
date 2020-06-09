//! Quotas and rate limiting for Relay.

#![warn(missing_docs)]

/// The default timeout to apply when a scope is fully rejected. This
/// typically happens for disabled keys, projects, or organizations.
const REJECT_ALL_SECS: u64 = 60;

mod quota;
mod rate_limit;

pub use self::quota::*;
pub use self::rate_limit::*;

#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "redis")]
pub use self::redis::*;
