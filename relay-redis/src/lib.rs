//! Abstraction over Redis caches.
//!
//! By default, this library only implements an empty noop client. With the `impl` feature, the
//! actual Redis client is implemented.
#![warn(missing_docs)]

#[cfg(feature = "impl")]
mod real;
#[cfg(feature = "impl")]
pub use self::real::*;

#[cfg(not(feature = "impl"))]
mod noop;
#[cfg(not(feature = "impl"))]
pub use self::noop::*;
