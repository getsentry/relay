//! Basic types for the meta protocol.
//!
//! This only provides the types (or aliases) that are used for the meta
//! part of the protocol.  This is the core annotation system as well as
//! the dynamic value parts and the metadata that goes with it.

mod annotated;
mod meta;
mod value;

pub use self::annotated::{Annotated, IsEmpty, MetaMap, MetaTree};
pub use self::meta::{Meta, Range, Remark, RemarkType};
pub use self::value::{Array, Map, Object, Value, ValueDescription};
