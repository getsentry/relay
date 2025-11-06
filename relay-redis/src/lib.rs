//! Abstraction over Redis caches.
//!
//! By default, this library only implements an empty noop client. With the `impl` feature, the
//! actual Redis client is implemented.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod config;
pub use self::config::*;

#[cfg(feature = "impl")]
mod pool;

#[cfg(feature = "impl")]
mod real;
#[cfg(feature = "impl")]
pub use self::real::*;

#[cfg(feature = "impl")]
mod scripts;
#[cfg(feature = "impl")]
pub use self::scripts::*;

#[cfg(feature = "impl")]
mod statsd;

#[cfg(not(feature = "impl"))]
mod noop;

#[cfg(not(feature = "impl"))]
pub use self::noop::*;

/// Typical `Result` when dealing with Redis.
pub type Result<T, E = RedisError> = std::result::Result<T, E>;
