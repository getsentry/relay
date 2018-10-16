use types::{Annotated, Event, Exception, Stacktrace, MetaValue};

#[derive(Debug, Clone)]
pub struct ProcessingState {
    path: Vec<String>,
}

pub trait Processor {
    #[inline(always)]
    fn process_event(&self, event: Annotated<Event>) -> Annotated<Event> { event }
    #[inline(always)]
    fn process_exception(&self, exception: Annotated<Exception>) -> Annotated<Exception> { exception }
    #[inline(always)]
    fn process_stacktrace(&self, stacktrace: Annotated<Stacktrace>) -> Annotated<Stacktrace> { stacktrace }
    #[inline(always)]
    fn process_frame(&self, frame: Annotated<Frame>) -> Annotated<Frame> { frame }
};

pub trait MetaStructure {
    fn metastructure_from_metavalue(value: MetaValue) -> Annotated<Self>;
    fn metastructure_to_metavalue(value: Annotated<Self>) -> MetaValue;
    #[inline(always)]
    fn process<P: Processor>(value: Annotated<Self>, processor: &P, state: ProcessingState) -> Annotated<Self> { self }
}
