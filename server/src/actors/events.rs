use actix::{Actor, Addr, Context, Handler, Message};
use bytes::Bytes;
use uuid::Uuid;

use actors::project::{EventMetaData, Project};
use utils::Response;

#[derive(Debug, Fail)]
#[fail(display = "could not process event")]
pub struct ProcessingError;

pub struct EventProcessor;

impl EventProcessor {
    pub fn new() -> Self {
        EventProcessor
    }
}

impl Actor for EventProcessor {
    type Context = Context<Self>;
}

pub struct StoreEvent {
    pub meta: EventMetaData,
    pub data: Bytes,
    pub project: Addr<Project>,
}

#[derive(Serialize)]
pub struct StoreEventResponse {
    id: Uuid,
}

impl Message for StoreEvent {
    type Result = Result<StoreEventResponse, ProcessingError>;
}

impl Handler<StoreEvent> for EventProcessor {
    type Result = Response<StoreEventResponse, ProcessingError>;

    fn handle(&mut self, message: StoreEvent, context: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}
