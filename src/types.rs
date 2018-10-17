use std::collections::BTreeMap;

use uuid::Uuid;

use meta::{Annotated, Meta, MetaValue};

pub type Array<T> = Vec<Annotated<T>>;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Array(Vec<Annotated<Value>>),
    Object(BTreeMap<String, Annotated<Value>>),
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_event")]
pub struct Event {
    #[metastructure(field = "event_id")]
    pub id: Annotated<Uuid>,
    pub exceptions: Annotated<Array<Exception>>,
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_stacktrace")]
pub struct Stacktrace {
    pub frames: Annotated<Array<Frame>>,
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_frame")]
pub struct Frame {
    pub function: Annotated<String>,
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_exception")]
pub struct Exception {
    #[metastructure(field = "type", required = "true")]
    pub ty: Annotated<String>,
    pub value: Annotated<String>,
    pub module: Annotated<String>,
    pub stacktrace: Annotated<Stacktrace>,
    pub raw_stacktrace: Annotated<Stacktrace>,
    #[metastructure(additional_properties)]
    pub other: BTreeMap<String, Annotated<Value>>,
}
