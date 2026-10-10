//! Relay serialization toolkit.
//!
//! Used to ensure we can bound the work when deserializing untrusted data.

#![warn(missing_docs)]

mod meter;

pub mod prost;
pub mod serde;
