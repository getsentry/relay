//! Functionality for aggregating and storing of metrics metadata.

mod aggregator;
mod protocol;
#[cfg(feature = "redis")]
mod redis;

pub use self::aggregator::*;
pub use self::protocol::*;
#[cfg(feature = "redis")]
pub use self::redis::*;
