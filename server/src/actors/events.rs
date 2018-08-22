use actix::{Actor, Addr, Handler, Message, SyncContext};
use bytes::Bytes;
use uuid::Uuid;

use actors::project::{EventMetaData, Project};
use actors::upstream::UpstreamRelay;

#[derive(Debug, Fail)]
#[fail(display = "could not process event")]
pub struct ProcessingError;

pub struct EventProcessor {
    upstream: Addr<UpstreamRelay>,
}

impl EventProcessor {
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        EventProcessor { upstream }
    }
}

impl Actor for EventProcessor {
    type Context = SyncContext<Self>;
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
    type Result = Result<StoreEventResponse, ProcessingError>;

    fn handle(&mut self, message: StoreEvent, context: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}
