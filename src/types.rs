use std::collections::BTreeMap;

use uuid::Uuid;

use meta::{Annotated, Meta, Value};

pub type Array<T> = Vec<Annotated<T>>;

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
