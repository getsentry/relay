use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::Mutex;

use crate::envelope::Envelope;
use crate::services::buffer::envelopebuffer::priority::PriorityEnvelopeBuffer;
use crate::services::buffer::envelopestack::InMemoryEnvelopeStack;

mod priority;

/// A buffer that stores & prioritizes envelopes.
pub trait EnvelopeBuffer: std::fmt::Debug + Send {
    /// Adds an envelope to the buffer.
    fn push(&mut self, envelope: Box<Envelope>);

    /// Returns a reference to the next envelope.
    ///
    /// Returns `None` if the buffer is empty.
    fn peek(&mut self) -> Option<&Envelope>;

    /// Returns and removes the next envelope.
    ///
    /// Returns `None` if the buffer is empty.
    fn pop(&mut self) -> Option<Box<Envelope>>;

    /// Marks a project as ready or not ready.
    ///
    /// The buffer reprioritizes its envelopes based on this information.
    fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) -> bool;
}

/// Creates a memory or disk based [`EnvelopeBuffer`], depending on the given config.
pub fn create(config: &Config) -> Arc<Mutex<dyn EnvelopeBuffer>> {
    // TODO: create a DiskMemoryStack
    Arc::new(Mutex::new(
        PriorityEnvelopeBuffer::<InMemoryEnvelopeStack>::new(),
    ))
}
