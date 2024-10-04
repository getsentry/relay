use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_provider::sqlite::SqliteEnvelopeProviderError;
use crate::services::buffer::stack_provider::InitializationState;
use crate::{Envelope, MemoryChecker, SqliteEnvelopeStore};
use hashbrown::HashMap;
use relay_config::Config;
use std::convert::Infallible;

#[derive(Debug)]
pub struct MemoryEnvelopeProvider {
    #[allow(clippy::vec_box)]
    envelopes: HashMap<ProjectKeyPair, Vec<Box<Envelope>>>,
    memory_checker: MemoryChecker,
}

impl MemoryEnvelopeProvider {
    pub fn new(memory_checker: MemoryChecker) -> Self {
        Self {
            envelopes: HashMap::new(),
            memory_checker,
        }
    }

    pub async fn initialize(&self) -> InitializationState {
        InitializationState::empty()
    }

    pub async fn push(
        &mut self,
        project_key_pair: ProjectKeyPair,
        envelope: Box<Envelope>,
    ) -> Result<(), Infallible> {
        Ok(())
    }

    pub async fn peek(
        &self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<&Envelope>, Infallible> {
        Ok(None)
    }

    pub async fn pop(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<Box<Envelope>>, Infallible> {
        Ok(None)
    }

    pub async fn flush(mut self) {}

    pub fn has_store_capacity(&self) -> bool {
        true
    }
}
