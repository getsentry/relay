use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{Annotated, ProcessingResult};

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
    dbg!("before before process");
    dbg!(&annotated);
    let action = processor.before_process(annotated.0.as_ref(), &mut annotated.1, state);
    annotated.apply(|_, _| action)?;

    dbg!("after before process");
    annotated.apply(|value, meta| ProcessValue::process_value(value, meta, processor, state))?;
    dbg!("after process_value inner");

    let action = processor.after_process(annotated.0.as_ref(), &mut annotated.1, state);
    annotated.apply(|_, _| action)?;

    Ok(())
}
