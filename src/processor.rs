use std::collections::BTreeMap;

use uuid::Uuid;

use meta::{Annotated, Meta, MetaValue};
use types::{Event, Exception, Frame, Stacktrace, Value};

#[derive(Debug, Clone)]
pub struct ProcessingState {
    path: Vec<String>,
}

impl ProcessingState {
    #[inline(always)]
    pub fn enter(&self, path: &str) -> ProcessingState {
        let mut rv = self.clone();
        rv.path.push(path.to_string());
        rv
    }

    #[inline(always)]
    pub fn enter_item(&self, idx: usize) -> ProcessingState {
        let mut rv = self.clone();
        rv.path.push(idx.to_string());
        rv
    }
}

pub trait Processor {
    #[inline(always)]
    fn process_event(&self, event: Annotated<Event>, state: ProcessingState) -> Annotated<Event> {
        let _state = state;
        event
    }
    #[inline(always)]
    fn process_exception(
        &self,
        exception: Annotated<Exception>,
        state: ProcessingState,
    ) -> Annotated<Exception> {
        let _state = state;
        exception
    }
    #[inline(always)]
    fn process_stacktrace(
        &self,
        stacktrace: Annotated<Stacktrace>,
        state: ProcessingState,
    ) -> Annotated<Stacktrace> {
        let _state = state;
        stacktrace
    }
    #[inline(always)]
    fn process_frame(&self, frame: Annotated<Frame>, state: ProcessingState) -> Annotated<Frame> {
        let _state = state;
        frame
    }
}

pub trait MetaStructure {
    fn metastructure_from_metavalue(value: MetaValue) -> Annotated<Self>
    where
        Self: Sized;
    fn metastructure_to_metavalue(value: Annotated<Self>) -> MetaValue
    where
        Self: Sized;
    #[inline(always)]
    fn process<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        let _processor = processor;
        let _state = state;
        value
    }
}

macro_rules! primitive_meta_structure {
    ($type:ident, $meta_type:ident) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn metastructure_from_metavalue(value: MetaValue) -> Annotated<Self> {
                match value {
                    MetaValue::$meta_type(value, meta) => Annotated(Some(value), meta),
                    MetaValue::Null(meta) => Annotated(None, meta),
                    // TODO: add error
                    other => Annotated(None, other.into_meta()),
                }
            }
            #[inline(always)]
            fn metastructure_to_metavalue(value: Annotated<Self>) -> MetaValue {
                match value {
                    Annotated(Some(value), meta) => MetaValue::$meta_type(value, meta),
                    Annotated(None, meta) => MetaValue::Null(meta),
                }
            }
        }
    };
}

macro_rules! primitive_meta_structure_through_string {
    ($type:ident) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn metastructure_from_metavalue(value: MetaValue) -> Annotated<Self> {
                match value {
                    MetaValue::String(value, meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        // TODO: add error
                        Err(_err) => Annotated(None, meta),
                    },
                    MetaValue::Null(meta) => Annotated(None, meta),
                    // TODO: add error
                    other => Annotated(None, other.into_meta()),
                }
            }
            #[inline(always)]
            fn metastructure_to_metavalue(value: Annotated<Self>) -> MetaValue {
                match value {
                    Annotated(Some(value), meta) => MetaValue::String(value.to_string(), meta),
                    Annotated(None, meta) => MetaValue::Null(meta),
                }
            }
        }
    };
}

primitive_meta_structure!(String, String);
primitive_meta_structure!(u8, U8);
primitive_meta_structure!(u16, U16);
primitive_meta_structure!(u32, U32);
primitive_meta_structure!(u64, U64);
primitive_meta_structure!(i8, I8);
primitive_meta_structure!(i16, I16);
primitive_meta_structure!(i32, I32);
primitive_meta_structure!(i64, I64);
primitive_meta_structure_through_string!(Uuid);

impl<T: MetaStructure> MetaStructure for Vec<Annotated<T>> {
    fn metastructure_from_metavalue(value: MetaValue) -> Annotated<Self> {
        match value {
            MetaValue::Array(items, meta) => Annotated(
                Some(
                    items
                        .into_iter()
                        .map(MetaStructure::metastructure_from_metavalue)
                        .collect(),
                ),
                meta,
            ),
            MetaValue::Null(meta) => Annotated(None, meta),
            // TODO: add error
            other => Annotated(None, other.into_meta()),
        }
    }
    #[inline(always)]
    fn metastructure_to_metavalue(value: Annotated<Self>) -> MetaValue {
        match value {
            Annotated(Some(value), meta) => MetaValue::Array(
                value
                    .into_iter()
                    .map(MetaStructure::metastructure_to_metavalue)
                    .collect(),
                meta,
            ),
            Annotated(None, meta) => MetaValue::Null(meta),
        }
    }
}

impl<T: MetaStructure> MetaStructure for BTreeMap<String, Annotated<T>> {
    fn metastructure_from_metavalue(value: MetaValue) -> Annotated<Self> {
        match value {
            MetaValue::Object(items, meta) => Annotated(
                Some(
                    items
                        .into_iter()
                        .map(|(k, v)| (k, MetaStructure::metastructure_from_metavalue(v)))
                        .collect(),
                ),
                meta,
            ),
            MetaValue::Null(meta) => Annotated(None, meta),
            // TODO: add error
            other => Annotated(None, other.into_meta()),
        }
    }
    #[inline(always)]
    fn metastructure_to_metavalue(value: Annotated<Self>) -> MetaValue {
        match value {
            Annotated(Some(value), meta) => MetaValue::Object(
                value
                    .into_iter()
                    .map(|(k, v)| (k, MetaStructure::metastructure_to_metavalue(v)))
                    .collect(),
                meta,
            ),
            Annotated(None, meta) => MetaValue::Null(meta),
        }
    }
}

impl MetaStructure for Value {
    #[inline(always)]
    fn metastructure_from_metavalue(value: MetaValue) -> Annotated<Value> {
        value.into()
    }
    #[inline(always)]
    fn metastructure_to_metavalue(value: Annotated<Value>) -> MetaValue {
        value.into()
    }
    #[inline(always)]
    fn process<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(Value::Array(
                    items
                        .into_iter()
                        .enumerate()
                        .map(|(idx, value)| {
                            MetaStructure::process(value, processor, state.enter_item(idx))
                        }).collect(),
                )),
                meta,
            ),
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(Value::Object(
                    items
                        .into_iter()
                        .map(|(k, v)| {
                            let v = MetaStructure::process(v, processor, state.enter(&k));
                            (k, v)
                        }).collect(),
                )),
                meta,
            ),
            other => other,
        }
    }
}
