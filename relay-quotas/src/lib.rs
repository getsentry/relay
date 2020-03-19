//! Quotas and rate limiting for Relay.

#![warn(missing_docs)]

mod types;
pub use self::types::*;

#[cfg(feature = "legacy")]
pub mod legacy;

#[cfg(feature = "rate-limiter")]
mod rate_limiter;
#[cfg(feature = "rate-limiter")]
pub use self::rate_limiter::*;
