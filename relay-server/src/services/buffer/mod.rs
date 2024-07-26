#![deny(missing_docs)]
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::MutexGuard;

use crate::envelope::Envelope;

mod envelopebuffer;
mod envelopestack;

/// Wrapper for the EnvelopeBuffer implementation.
#[derive(Debug)]
pub struct EnvelopeBuffer(
    /// TODO: Reconsider synchronization mechanism.
    /// We can either
    /// - keep the interface sync and use a std Mutex. In this case, we create a queue of threads.
    /// - use an async interface with a tokio mutex. In this case, we create a queue of futures.
    /// - use message passing (service or channel). In this case, we create a queue of messages.
    ///
    /// From the tokio docs:
    ///
    /// >  The primary use case for the async mutex is to provide shared mutable access to IO resources such as a database connection.
    /// > [...] when you do want shared access to an IO resource, it is often better to spawn a task to manage the IO resource,
    /// > and to use message passing to communicate with that task.
    Arc<tokio::sync::Mutex<dyn envelopebuffer::EnvelopeBuffer>>,
);

impl EnvelopeBuffer {
    pub fn from_config(config: &Config) -> Option<Self> {
        // TODO: create a DiskMemoryStack if db config is given.
        config
            .spool_v2()
            .then(|| Self(envelopebuffer::create(config)))
    }

    pub async fn push(&self, envelope: Box<Envelope>) {
        let mut guard = self.0.lock().await;
        guard.push(envelope);
    }

    pub async fn peek(&self) -> Option<Peek> {
        let mut guard = self.0.lock().await;
        guard.peek()?;

        Some(Peek(guard))
    }
}

pub struct Peek<'a>(MutexGuard<'a, dyn envelopebuffer::EnvelopeBuffer>);

impl Peek<'_> {
    pub fn get(&mut self) -> &Envelope {
        self.0
            .peek()
            .expect("element disappeared while holding lock")
    }

    pub fn remove(mut self) -> Box<Envelope> {
        self.0
            .pop()
            .expect("element disappeared while holding lock")
    }

    pub fn mark_ready(&mut self, project_key: &ProjectKey, ready: bool) {
        self.0.mark_ready(project_key, ready);
    }
}
