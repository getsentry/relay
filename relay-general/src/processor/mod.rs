//! Provides support for processing structures.

mod attrs;
mod chunks;
mod funcs;
mod impls;
mod selector;
mod size;
mod traits;

pub use self::attrs::{
    FieldAttrs, Path, PathItem, ProcessingState, UnknownValueTypeError, ValueType,
};
pub use self::chunks::{join_chunks, process_chunked_value, split_chunks, Chunk};
pub use self::funcs::process_value;

// TODO(markus): move to pii module
pub use self::selector::{SelectorPathItem, SelectorSpec};
pub use self::size::{estimate_size, estimate_size_flat};
pub use self::traits::{AttrMap, Attributes, ProcessValue, Processor};
