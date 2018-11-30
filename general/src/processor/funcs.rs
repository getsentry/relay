use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::Annotated;

/// Processes the value using the given processor.
pub fn process_value<T, P>(value: Annotated<T>, processor: &mut P) -> Annotated<T>
where
    T: ProcessValue,
    P: Processor,
{
    ProcessValue::process_value(value, processor, ProcessingState::root())
}

/// Attaches a value required error if the value is missing.
pub fn require_value<T>(value: &mut Annotated<T>) {
    if value.value().is_none() && !value.meta().has_errors() {
        value.meta_mut().add_error("value required", None);
    }
}
