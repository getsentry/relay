//! Basic types for the meta protocol.
//!
//! This only provides the types (or aliases) that are used for the meta
//! part of the protocol.  This is the core annotation system as well as
//! the dynamic value parts and the metadata that goes with it.

mod annotated;
mod impls;
mod meta;
mod span_attributes;
mod traits;
mod value;

pub use self::annotated::*;
pub use self::impls::*;
pub use self::meta::*;
pub use self::span_attributes::*;
pub use self::traits::*;
pub use self::value::*;
