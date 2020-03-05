//! Implements

#![warn(missing_docs)]

mod types;
pub use self::types::*;

#[cfg(feature = "legacy")]
pub mod legacy;

#[cfg(feature = "rate-limit")]
mod rate_limiter;
#[cfg(feature = "rate-limit")]
pub use self::rate_limiter::*;
