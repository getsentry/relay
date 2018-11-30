use uuid::Uuid;

use crate::processor::{process_value, ProcessResult, ProcessValue, ProcessingState, Processor};
use crate::types::{Array, Meta, Object, Value};

impl ProcessValue for String {
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        processor.process_string(value, meta, state)
    }
}

impl ProcessValue for bool {
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        processor.process_bool(value, meta, state)
    }
}

impl ProcessValue for u64 {
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        processor.process_u64(value, meta, state)
    }
}

impl ProcessValue for i64 {
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        processor.process_i64(value, meta, state)
    }
}

impl ProcessValue for f64 {
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        processor.process_f64(value, meta, state)
    }
}

impl ProcessValue for Uuid {}

impl<T> ProcessValue for Array<T>
where
    T: ProcessValue,
{
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        Self::process_child_values(value, processor, state.clone());
        processor.process_array(value, meta, state)
    }

    #[inline]
    fn process_child_values<P>(value: &mut Self, processor: &mut P, state: ProcessingState)
    where
        P: Processor,
    {
        for (index, element) in value.iter_mut().enumerate() {
            process_value(element, processor, state.enter_index(index, None));
        }
    }
}

impl<T> ProcessValue for Object<T>
where
    T: ProcessValue,
{
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        Self::process_child_values(value, processor, state.clone());
        processor.process_object(value, meta, state)
    }

    #[inline]
    fn process_child_values<P>(value: &mut Self, processor: &mut P, state: ProcessingState)
    where
        P: Processor,
    {
        for (k, v) in value.iter_mut() {
            process_value(v, processor, state.enter_borrowed(k, None));
        }
    }
}

impl<T> ProcessValue for Box<T>
where
    T: ProcessValue,
{
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        ProcessValue::process_value(value.as_mut(), meta, processor, state)
    }
}

impl ProcessValue for Value {
    #[inline]
    fn process_value<P>(
        value: &mut Self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState,
    ) -> ProcessResult
    where
        P: Processor,
    {
        match value {
            Value::Null => Default::default(),
            Value::Bool(v) => ProcessValue::process_value(v, meta, processor, state),
            Value::I64(v) => ProcessValue::process_value(v, meta, processor, state),
            Value::U64(v) => ProcessValue::process_value(v, meta, processor, state),
            Value::F64(v) => ProcessValue::process_value(v, meta, processor, state),
            Value::String(v) => ProcessValue::process_value(v, meta, processor, state),
            Value::Array(v) => ProcessValue::process_value(v, meta, processor, state),
            Value::Object(v) => ProcessValue::process_value(v, meta, processor, state),
        }
    }
}
