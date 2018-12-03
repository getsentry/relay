//! Provides support for processing structures.

mod attrs;
mod chunks;
mod funcs;
mod impls;
mod size;
mod traits;

pub use self::attrs::{BagSize, FieldAttrs, MaxChars, Path, PiiKind, ProcessingState};
pub use self::chunks::{join_chunks, process_chunked_value, split_chunks, Chunk};
pub use self::funcs::{apply_value, process_value, require_value};
pub use self::size::estimate_size;
pub use self::traits::{ProcessResult, ProcessValue, Processor};
