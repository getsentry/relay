use std::collections::BTreeMap;

use uuid::Uuid;

use meta::{Annotated, Value};
use types::{Event, Exception, Frame, Stacktrace};

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
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized;
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
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
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::$meta_type(value)), meta) => Annotated(Some(value), meta),
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.errors_mut().push(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => Annotated(Some(Value::$meta_type(value)), meta),
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
        }
    };
}

macro_rules! numeric_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::U64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::I64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::F64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.errors_mut().push(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => Annotated(Some(Value::$meta_type(value)), meta),
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
        }
    };
}

macro_rules! primitive_meta_structure_through_string {
    ($type:ident, $expectation:expr) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.errors_mut().push(err.to_string());
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.errors_mut().push(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => {
                        Annotated(Some(Value::String(value.to_string())), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
        }
    };
}

primitive_meta_structure!(String, String, "a string");
primitive_meta_structure!(bool, Bool, "a boolean");
numeric_meta_structure!(u64, U64, "an unsigned integer");
numeric_meta_structure!(i64, I64, "a signed integer");
numeric_meta_structure!(f64, F64, "a floating point value");
primitive_meta_structure_through_string!(Uuid, "a uuid");

impl<T: MetaStructure> MetaStructure for Vec<Annotated<T>> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(items.into_iter().map(MetaStructure::from_value).collect()),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            // TODO: add error
            Annotated(_, meta) => Annotated(None, meta),
        }
    }
    #[inline(always)]
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Array(
                    value.into_iter().map(MetaStructure::to_value).collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl<T: MetaStructure> MetaStructure for BTreeMap<String, Annotated<T>> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(
                    items
                        .into_iter()
                        .map(|(k, v)| (k, MetaStructure::from_value(v)))
                        .collect(),
                ),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            // TODO: add error
            Annotated(_, meta) => Annotated(None, meta),
        }
    }
    #[inline(always)]
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Object(
                    value
                        .into_iter()
                        .map(|(k, v)| (k, MetaStructure::to_value(v)))
                        .collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl MetaStructure for Value {
    #[inline(always)]
    fn from_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }
    #[inline(always)]
    fn to_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }
    #[inline(always)]
    fn process<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        let _processor = processor;
        let _state = state;
        value
    }
}
