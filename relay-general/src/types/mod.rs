//! Basic types for the meta protocol.
//!
//! This only provides the types (or aliases) that are used for the meta
//! part of the protocol.  This is the core annotation system as well as
//! the dynamic value parts and the metadata that goes with it.

mod annotated;
mod impls;
mod meta;
mod traits;
mod value;

pub use self::annotated::{
    Annotated, MetaMap, MetaTree, ProcessingAction, ProcessingResult, SerializableAnnotated,
};
pub use self::impls::SerializePayload;
pub use self::meta::{Error, ErrorKind, Meta, Range, Remark, RemarkType};
pub use self::traits::{Empty, FromValue, IntoValue, SkipSerialization};
pub use self::value::{to_value, Array, Map, Object, Value, ValueDescription};
