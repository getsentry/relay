use std::mem;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{Annotated, Array, Object, Value};

primitive_process_value!(String, process_string);
primitive_process_value!(bool, process_bool);
primitive_process_value!(u64, process_u64);
primitive_process_value!(i64, process_i64);
primitive_process_value!(f64, process_f64);

impl ProcessValue for Uuid {}

impl<T: ProcessValue> ProcessValue for Array<T> {
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        processor.process_array(value, state)
    }

    fn process_child_values<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        match value {
            Annotated(Some(mut value), meta) => Annotated(
                Some({
                    for (idx, v) in value.iter_mut().enumerate() {
                        let inner_state = state.enter_index(idx, None);
                        let value =
                            Annotated(v.0.take(), mem::replace(&mut v.1, Default::default()));
                        *v = ProcessValue::process_value(value, processor, inner_state);
                    }
                    value
                }),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl<T: ProcessValue> ProcessValue for Object<T> {
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        processor.process_object(value, state)
    }

    fn process_child_values<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        match value {
            Annotated(Some(mut value), meta) => Annotated(
                Some({
                    for (k, v) in value.iter_mut() {
                        let inner_state = state.enter_borrowed(&k, None);
                        let mut value =
                            Annotated(v.0.take(), mem::replace(&mut v.1, Default::default()));
                        *v = ProcessValue::process_value(value, processor, inner_state);
                    }
                    value
                }),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ProcessValue for Value {
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Null), meta) => Annotated(Some(Value::Null), meta),
            Annotated(Some(Value::Bool(value)), meta) => {
                ProcessValue::process_value(Annotated(Some(value), meta), processor, state)
                    .map_value(Value::Bool)
            }
            Annotated(Some(Value::I64(value)), meta) => {
                ProcessValue::process_value(Annotated(Some(value), meta), processor, state)
                    .map_value(Value::I64)
            }
            Annotated(Some(Value::U64(value)), meta) => {
                ProcessValue::process_value(Annotated(Some(value), meta), processor, state)
                    .map_value(Value::U64)
            }
            Annotated(Some(Value::F64(value)), meta) => {
                ProcessValue::process_value(Annotated(Some(value), meta), processor, state)
                    .map_value(Value::F64)
            }
            Annotated(Some(Value::String(value)), meta) => {
                ProcessValue::process_value(Annotated(Some(value), meta), processor, state)
                    .map_value(Value::String)
            }
            Annotated(Some(Value::Object(mut items)), meta) => ProcessValue::process_value(
                Annotated(
                    Some({
                        for (k, v) in items.iter_mut() {
                            let inner_state = state.enter_borrowed(&k, None);
                            let mut value =
                                Annotated(v.0.take(), mem::replace(&mut v.1, Default::default()));
                            *v = ProcessValue::process_value(value, processor, inner_state);
                        }
                        items
                    }),
                    meta,
                ),
                processor,
                state,
            ).map_value(Value::Object),
            Annotated(Some(Value::Array(mut items)), meta) => ProcessValue::process_value(
                Annotated(
                    Some({
                        for (idx, v) in items.iter_mut().enumerate() {
                            let inner_state = state.enter_index(idx, None);
                            let value =
                                Annotated(v.0.take(), mem::replace(&mut v.1, Default::default()));
                            *v = ProcessValue::process_value(value, processor, inner_state);
                        }
                        items
                    }),
                    meta,
                ),
                processor,
                state,
            ).map_value(Value::Array),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ProcessValue for DateTime<Utc> {
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        processor.process_datetime(value, state)
    }
}

impl<T: ProcessValue> ProcessValue for Box<T> {
    /// Executes a processor on the tree.
    fn process_child_values<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        let value: Annotated<T> = Annotated(value.0.map(|x| *x), value.1);
        let rv = ProcessValue::process_value(value, processor, state);
        Annotated(rv.0.map(Box::new), rv.1)
    }
}
