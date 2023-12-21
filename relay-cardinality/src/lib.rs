//! Relay Cardinality Module

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

#[cfg(feature = "redis")]
mod cache;
mod error;
pub mod limiter;
#[cfg(feature = "redis")]
mod redis;
mod statsd;
#[cfg(feature = "redis")]
mod utils;
mod window;

pub use self::error::*;
pub use self::limiter::{
    CardinalityItem, CardinalityLimits, CardinalityScope, Config as CardinalityLimiterConfig,
};
#[cfg(feature = "redis")]
pub use self::redis::RedisSetLimiter;
pub use self::window::SlidingWindow;

/// Redis Set based cardinality limiter.
#[cfg(feature = "redis")]
pub type CardinalityLimiter = self::limiter::CardinalityLimiter<RedisSetLimiter>;

/// Internal alias for better readability.
type OrganizationId = u64;
