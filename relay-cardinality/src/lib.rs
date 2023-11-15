//! Metrics Cardinality Limiter

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod error;
mod limiter;
mod redis;

pub use self::error::*;
pub use self::limiter::{Limiter, Scope};
pub use self::redis::RedisSetLimiter;

/// Redis Set based cardinality limiter.
pub type CardinalityLimiter = self::limiter::CardinalityLimiter<RedisSetLimiter>;

// TODO: I want a new type for organization ids
pub(crate) type OrganizationId = u64;
