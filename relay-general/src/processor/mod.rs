//! Provides support for processing structures.

mod attrs;
mod chunks;
mod funcs;
mod impls;
mod selector;
mod size;
mod traits;

pub use self::attrs::{
    BagSize, CharacterSet, FieldAttrs, MaxChars, Path, Pii, ProcessingState, UnknownValueTypeError,
    ValueType,
};
pub use self::chunks::{join_chunks, process_chunked_value, split_chunks, Chunk};
pub use self::funcs::process_value;
pub use self::selector::{SelectorPathItem, SelectorSpec};
pub use self::size::{estimate_size, estimate_size_flat};
pub use self::traits::{ProcessValue, Processor};
