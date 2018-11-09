//! Provides support for processing structures.

mod attrs;
mod chunks;
mod impls;
mod size;
mod traits;

pub use self::attrs::*;
pub use self::chunks::*;
pub use self::impls::*;
pub use self::size::*;
pub use self::traits::*;

use crate::types::Annotated;

/// Processes an annotated value with a processor.
pub fn process<T, P>(value: Annotated<T>, processor: &P) -> Annotated<T>
where
    T: ProcessValue,
    P: Processor,
{
    ProcessValue::process_value(value, processor, ProcessingState::root())
}
