use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_provider::InitializationState;
use crate::{Envelope, MemoryChecker};
use hashbrown::HashMap;
use std::convert::Infallible;

/// Provides in-memory storage for envelopes, organized by project key pairs.
#[derive(Debug)]
pub struct MemoryEnvelopeProvider {
    #[allow(clippy::vec_box)]
    envelopes: HashMap<ProjectKeyPair, Vec<Box<Envelope>>>,
    memory_checker: MemoryChecker,
}

impl MemoryEnvelopeProvider {
    /// Creates a new MemoryEnvelopeProvider with the given memory checker.
    pub fn new(memory_checker: MemoryChecker) -> Self {
        Self {
            envelopes: HashMap::new(),
            memory_checker,
        }
    }

    /// Initializes the provider, returning an empty initialization state.
    pub async fn initialize(&self) -> InitializationState {
        InitializationState::empty()
    }

    /// Pushes an envelope to the provider for the given project key pair.
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

    /// Attempts to flush envelopes to storage. This is a no-op for in-memory implementation.
    pub async fn flush(&mut self) -> bool {
        // This is a noop for the in-memory implementation since we don't have any way to flush
        // envelopes to storage.
        false
    }

    /// Checks if there is capacity to store more envelopes.
    pub fn has_store_capacity(&self) -> bool {
        self.memory_checker.check_memory().has_capacity()
    }
}
