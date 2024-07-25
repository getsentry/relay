#![deny(missing_docs)]
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::{Mutex, MutexGuard};

use crate::envelope::Envelope;

mod envelopebuffer;
mod envelopestack;

/// Wrapper for the EnvelopeBuffer implementation.
#[derive(Debug)]
pub struct EnvelopeBuffer(Arc<Mutex<dyn envelopebuffer::EnvelopeBuffer>>);

impl EnvelopeBuffer {
    pub fn from_config(config: &Config) -> Self {
        // TODO: create a DiskMemoryStack if db config is given.
        Self(envelopebuffer::create(config))
    }

    pub async fn push(&self, envelope: Box<Envelope>) {
        let mut guard = self.0.lock().await;
        guard.push(envelope);
    }

    pub async fn peek(&self) -> Peek {
        Peek(self.0.lock().await)
    }

    pub async fn mark_ready(&self, project: &ProjectKey, is_ready: bool) {
        let mut guard = self.0.lock().await;
        guard.mark_ready(project, is_ready)
    }
}

pub struct Peek<'a>(MutexGuard<'a, dyn envelopebuffer::EnvelopeBuffer>);

impl Peek<'_> {
    pub fn get(&mut self) -> Option<&Envelope> {
        self.0.peek()
    }

    pub fn remove(&mut self) -> Option<Box<Envelope>> {
        self.0.pop()
    }
}
