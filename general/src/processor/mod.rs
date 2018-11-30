//! Provides support for processing structures.

mod attrs;
mod chunks;
mod impls;
mod size;
mod traits;

pub use self::attrs::{BagSize, FieldAttrs, MaxChars, Path, PiiKind, ProcessingState};
pub use self::chunks::{join_chunks, map_value_chunked, split_chunks, Chunk};
pub use self::impls::SerializePayload;
pub use self::size::SizeEstimatingSerializer;
pub use self::traits::{FromValue, ProcessValue, Processor, ToValue};
