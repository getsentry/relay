use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::Mutex;

use crate::envelope::Envelope;
use crate::services::buffer::envelopebuffer::priority::PriorityEnvelopeBuffer;
use crate::services::buffer::envelopestack::InMemoryEnvelopeStack;

mod priority;

pub trait EnvelopeBuffer: std::fmt::Debug + Send {
    fn push(&mut self, envelope: Box<Envelope>);
    fn peek(&mut self) -> Option<&Envelope>;
    fn pop(&mut self) -> Option<Box<Envelope>>;
    fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool);
}

pub fn create(config: &Config) -> Arc<Mutex<dyn EnvelopeBuffer>> {
    // TODO: create a DiskMemoryStack
    Arc::new(Mutex::new(
        PriorityEnvelopeBuffer::<InMemoryEnvelopeStack>::new(),
    ))
}
