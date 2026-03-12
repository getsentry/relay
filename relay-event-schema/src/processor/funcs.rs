use relay_protocol::{Annotated, IntoValue, Meta, Remark, RemarkType};

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
        Err(ProcessingAction::DeleteValueHard) => delete_hard(v),
        Err(ProcessingAction::DeleteValueWithRemark(rule_id)) => delete_with_remark(v, rule_id),
        Err(ProcessingAction::DeleteValueSoft) => delete_soft(v),

        x @ Err(ProcessingAction::InvalidTransaction(_)) => return x,
    }

    Ok(())
}

/// Deletes an `Annotated`'s value.
pub fn delete_hard<T>(v: &mut Annotated<T>) {
    v.0 = None;
}

/// Deletes an `Annotated`'s value and adds a remark to the metadata.
///
/// The passed `rule_id` is used in the remark.
pub fn delete_with_remark<T>(v: &mut Annotated<T>, rule_id: &str) {
    v.0 = None;
    v.1.add_remark(Remark {
        ty: RemarkType::Removed,
        rule_id: rule_id.to_owned(),
        range: None,
    });
}

/// Deletes this `Annotated`'s value, but retains it as the original
/// value in the metadata.
pub fn delete_soft<T: IntoValue>(v: &mut Annotated<T>) {
    v.1.set_original_value(v.0.take());
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
