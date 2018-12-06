use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{Annotated, ErrorKind};

/// Processes the value using the given processor.
#[inline]
pub fn process_value<T, P>(
    annotated: &mut Annotated<T>,
    processor: &mut P,
    state: ProcessingState<'_>,
) where
    T: ProcessValue,
    P: Processor,
{
    annotated.apply(|value, meta| ProcessValue::process_value(value, meta, processor, state))
}

/// Attaches a value required error if the value is missing.
pub fn require_value<T>(annotated: &mut Annotated<T>) {
    if annotated.value().is_none() && !annotated.meta().has_errors() {
        annotated.meta_mut().add_error(ErrorKind::MissingAttribute);
    }
}
