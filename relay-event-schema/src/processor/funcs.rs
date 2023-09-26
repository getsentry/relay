use relay_protocol::{Annotated, IntoValue, Meta};

use crate::processor::{
    ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};

/// Modifies this value based on the action returned by `f`.
#[inline]
pub fn apply<T, F, R>(v: &mut Annotated<T>, f: F) -> ProcessingResult
where
    T: IntoValue,
    F: FnOnce(&mut T, &mut Meta) -> R,
    R: Into<ProcessingResult>,
{
    let result = match (v.0.as_mut(), &mut v.1) {
        (Some(value), meta) => f(value, meta).into(),
        (None, _) => Ok(()),
    };

    match result {
        Ok(()) => (),
        Err(ProcessingAction::DeleteValueHard) => v.0 = None,
        Err(ProcessingAction::DeleteValueSoft) => {
            v.1.set_original_value(v.0.take());
        }
        x @ Err(ProcessingAction::InvalidTransaction(_)) => return x,
    }

    Ok(())
}

/// Processes the value using the given processor.
#[inline]
pub fn process_value<T, P>(
    annotated: &mut Annotated<T>,
    processor: &mut P,
    state: &ProcessingState<'_>,
) -> ProcessingResult
where
    T: ProcessValue,
    P: Processor,
{
    let action = processor.before_process(annotated.0.as_ref(), &mut annotated.1, state);
    apply(annotated, |_, _| action)?;

    apply(annotated, |value, meta| {
        ProcessValue::process_value(value, meta, processor, state)
    })?;

    let action = processor.after_process(annotated.0.as_ref(), &mut annotated.1, state);
    apply(annotated, |_, _| action)?;

    Ok(())
}
