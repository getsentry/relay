//! Relay Cardinality Module

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod error;
pub mod limiter;
#[cfg(feature = "redis")]
mod redis;
mod statsd;

pub use self::error::*;
pub use self::limiter::{CardinalityLimits, Config as CardinalityLimiterConfig};
#[cfg(feature = "redis")]
pub use self::redis::{RedisSetLimiter, SlidingWindow};

/// Redis Set based cardinality limiter.
#[cfg(feature = "redis")]
pub type CardinalityLimiter = self::limiter::CardinalityLimiter<RedisSetLimiter>;

/// Internal alias for better readability.
type OrganizationId = u64;
