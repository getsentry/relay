//! Relay Cardinality Module

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod config;
mod error;
pub mod limiter;
#[cfg(feature = "redis")]
mod redis;
mod statsd;
mod window;

pub use self::config::*;
pub use self::error::*;
pub use self::limiter::{CardinalityItem, CardinalityLimits, Scoping};
#[cfg(feature = "redis")]
pub use self::redis::RedisSetLimiter;
pub use self::window::SlidingWindow;

/// Redis Set based cardinality limiter.
#[cfg(feature = "redis")]
pub type CardinalityLimiter = self::limiter::CardinalityLimiter<RedisSetLimiter>;

/// Internal alias for better readability.
type OrganizationId = u64;
