use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::Annotated;

/// Processes the value using the given processor.
#[inline]
pub fn process_value<T, P>(
    annotated: &mut Annotated<T>,
    processor: &mut P,
    state: &ProcessingState<'_>,
) where
    T: ProcessValue,
    P: Processor,
{
    processor.process_any(annotated.value().is_some(), annotated.meta_mut(), state);
    annotated.apply(|value, meta| ProcessValue::process_value(value, meta, processor, state))
}
