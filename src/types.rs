use std::collections::BTreeMap;

use uuid::Uuid;

use meta::Annotated;


pub enum MetaValue {
    Null(Meta),
    U64(u64, Meta),
    String(String, Meta),
    Array(Vec<MetaValue>, Meta),
    Object(BTreeMap<String, MetaValue>, Meta),
}

impl MetaValue {
    fn into_meta(self) -> Meta {
        match self {
            MetaValue::Null(meta) => meta,
            MetaValue::U64(_, meta) => meta,
            MetaValue::String(_, meta) => meta,
            MetaValue::Array(_, meta) => meta,
            MetaValue::Object(_, meta) => meta,
        }
    }
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_event")]
pub struct Event {
    #[metastructure(field = "event_id")]
    pub id: Annotated<Uuid>,
    pub fingerprint: Annotated<Vec<String>>,
    pub exceptions: Annotated<Values<Exception>>,
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
    #[metastructure(field = "type", required = true)]
    pub ty: Annotated<String>,
    pub value: Annotated<String>,
    pub module: Annotated<String>,
    pub stacktrace: Annotated<Stacktrace>,
    pub raw_stacktrace: Annotated<Stacktrace>,
    #[metastructure(additional_properties)]
    pub other: Map<Value>,
}


/*
pub struct DefaultsProcessor;

impl Processor for DefaultsProcessor {
    fn process_event(&self, mut event: Annotated<Event>) -> Annotated<Event> {
        event.with_value_mut(|event| {
            event.id = event.id.set_if_missing(Uuid::new_v4);
            event.fingerprint = event.fingerprint.set_if_missing(|| vec!["{{ default }}".to_string()]);
        });
        event
    }
}

pub struct CapProcessor;

impl Processor for CapProcessor {
    fn process_exception(&self, mut exception: Annotated<Exception>) -> Annotated<Exception> {
        exception.with_value_mut(|exception| {
            exception.ty.enforce_maximum_length(100);
            exception.value.enforce_maximum_length(300);
            exception.module.enforce_maximum_length(200);
        });
        exception
    }

    fn process_frame(&self, mut frame: Annotated<Frame>) -> Annotated<Frame> {
        frame.with_value_mut(|frame| {
            frame.function.enforce_maximum_length(200);
        });
        frame
    }
}
*/
