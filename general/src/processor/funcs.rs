use crate::processor::{ProcessResult, ProcessValue, ProcessingState, Processor};
use crate::types::{Annotated, Meta};

#[inline]
pub fn apply_value<T, F, R>(annotated: &mut Annotated<T>, f: F)
where
    F: FnOnce(&mut T, &mut Meta) -> R,
    R: Into<ProcessResult>,
{
    let result = match (annotated.0.as_mut(), &mut annotated.1) {
        (Some(value), meta) => f(value, meta).into(),
        (None, _) => Default::default(),
    };

    if result == ProcessResult::Discard {
        annotated.0 = None;
    }
}

/// Processes the value using the given processor.
#[inline]
pub fn process_value<T, P>(annotated: &mut Annotated<T>, processor: &mut P, state: ProcessingState)
where
    T: ProcessValue,
    P: Processor,
{
    apply_value(annotated, |value, meta| {
        ProcessValue::process_value(value, meta, processor, state)
    })
}

/// Attaches a value required error if the value is missing.
pub fn require_value<T>(annotated: &mut Annotated<T>) {
    if annotated.value().is_none() && !annotated.meta().has_errors() {
        annotated.meta_mut().add_error("value required", None);
    }
}
