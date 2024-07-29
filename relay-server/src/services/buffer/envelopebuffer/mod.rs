use std::sync::Arc;

use relay_config::Config;
use tokio::sync::Mutex;

use crate::services::buffer::envelopebuffer::priority::PriorityEnvelopeBuffer;
use crate::services::buffer::envelopestack::memory::InMemoryEnvelopeStack;

pub mod priority; // TODO

/// Creates a memory or disk based [`EnvelopeBuffer`], depending on the given config.
pub fn create(config: &Config) -> Arc<Mutex<PriorityEnvelopeBuffer<InMemoryEnvelopeStack>>> {
    // TODO: create a DiskMemoryStack
    Arc::new(Mutex::new(
        PriorityEnvelopeBuffer::<InMemoryEnvelopeStack>::new(),
    ))
}
