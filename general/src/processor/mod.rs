//! Provides support for processing structures.

mod attrs;
mod chunks;
mod impls;
mod size;
mod traits;

pub use self::attrs::{BagSize, FieldAttrs, MaxChars, Path, PiiKind, ProcessingState};
pub use self::chunks::{join_chunks, map_value_chunked, split_chunks, Chunk};
pub use self::size::estimate_size;
pub use self::traits::{ProcessValue, Processor};

use crate::types::Annotated;

/// Processes the value using the given processor.
pub fn process_value<T, P>(value: Annotated<T>, processor: &mut P) -> Annotated<T>
where
    T: ProcessValue,
    P: Processor,
{
    ProcessValue::process_value(value, processor, ProcessingState::root())
}
