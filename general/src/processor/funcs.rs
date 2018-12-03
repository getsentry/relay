use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::Annotated;

/// Processes the value using the given processor.
#[inline]
pub fn process_value<T, P>(annotated: &mut Annotated<T>, processor: &mut P, state: ProcessingState)
where
    T: ProcessValue,
    P: Processor,
{
    annotated.apply(|value, meta| ProcessValue::process_value(value, meta, processor, state))
}

/// Attaches a value required error if the value is missing.
pub fn require_value<T>(annotated: &mut Annotated<T>) {
    if annotated.value().is_none() && !annotated.meta().has_errors() {
        annotated.meta_mut().add_error("value required", None);
    }
}
