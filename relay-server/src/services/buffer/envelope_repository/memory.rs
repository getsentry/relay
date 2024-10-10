use crate::services::buffer::common::ProjectKeyPair;
use crate::{Envelope, MemoryChecker};
use hashbrown::HashMap;
use std::convert::Infallible;

/// Provides in-memory storage for envelopes, organized by project key pairs.
#[derive(Debug)]
pub struct MemoryEnvelopeRepository {
    #[allow(clippy::vec_box)]
    envelopes: HashMap<ProjectKeyPair, Vec<Box<Envelope>>>,
    memory_checker: MemoryChecker,
}

impl MemoryEnvelopeRepository {
    /// Creates a new [`MemoryEnvelopeRepository`] with the given memory checker.
    pub fn new(memory_checker: MemoryChecker) -> Self {
        Self {
            envelopes: HashMap::new(),
            memory_checker,
        }
    }

    /// Pushes an envelope to the repository for the given project key pair.
    pub async fn push(
        &mut self,
        project_key_pair: ProjectKeyPair,
        envelope: Box<Envelope>,
    ) -> Result<(), Infallible> {
        self.envelopes
            .entry(project_key_pair)
            .or_default()
            .push(envelope);

        Ok(())
    }

    /// Peeks at the next envelope for the given project key pair without removing it.
    pub async fn peek(
        &self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<&Envelope>, Infallible> {
        Ok(self
            .envelopes
            .get(&project_key_pair)
            .and_then(|envelopes| envelopes.last().map(|boxed| boxed.as_ref())))
    }

    /// Pops and returns the next envelope for the given project key pair.
    pub async fn pop(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<Box<Envelope>>, Infallible> {
        Ok(self
            .envelopes
            .get_mut(&project_key_pair)
            .and_then(|envelopes| envelopes.pop()))
    }

    /// Attempts to flush envelopes to storage.
    pub async fn flush(&mut self) -> bool {
        // Only if there are no envelopes we can signal to the caller that it is safe to drop the
        // buffer.
        self.envelopes.is_empty()
    }

    /// Checks if there is capacity to store more envelopes.
    pub fn has_store_capacity(&self) -> bool {
        self.memory_checker.check_memory().has_capacity()
    }

    /// Retrieves the total count of envelopes in the store.
    pub fn store_total_count(&self) -> u64 {
        self.envelopes.values().map(|e| e.len() as u64).sum()
    }
}
