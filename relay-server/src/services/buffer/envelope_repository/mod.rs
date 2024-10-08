use crate::services::buffer::common::{EnvelopeBufferError, ProjectKeyPair};
use crate::services::buffer::envelope_repository::memory::MemoryEnvelopeRepository;
use crate::services::buffer::envelope_repository::sqlite::SqliteEnvelopeRepository;
use crate::{Envelope, MemoryChecker};
use hashbrown::HashSet;
use relay_config::Config;

mod memory;
pub mod sqlite;

/// State of the initialization of the [`EnvelopeRepository`].
///
/// This state is necessary for initializing resources whenever a [`EnvelopeRepository`] is used.
#[derive(Debug)]
pub struct InitializationState {
    pub project_key_pairs: HashSet<ProjectKeyPair>,
}

impl InitializationState {
    /// Create a new [`InitializationState`].
    pub fn new(project_key_pairs: HashSet<ProjectKeyPair>) -> Self {
        Self { project_key_pairs }
    }

    /// Creates a new empty [`InitializationState`].
    pub fn empty() -> Self {
        Self {
            project_key_pairs: HashSet::new(),
        }
    }
}

/// Represents different types of envelope repositories.
#[derive(Debug)]
pub enum EnvelopeRepository {
    /// In-memory envelope repository.
    Memory(MemoryEnvelopeRepository),
    /// SQLite-based envelope repository.
    SQLite(SqliteEnvelopeRepository),
}

impl EnvelopeRepository {
    /// Creates a new memory-based envelope repository.
    pub fn memory(memory_checker: MemoryChecker) -> Result<Self, EnvelopeBufferError> {
        Ok(Self::Memory(MemoryEnvelopeRepository::new(memory_checker)))
    }

    /// Creates a new SQLite-based envelope repository.
    pub async fn sqlite(config: &Config) -> Result<Self, EnvelopeBufferError> {
        Ok(Self::SQLite(SqliteEnvelopeRepository::new(config).await?))
    }

    /// Initializes the [`EnvelopeRepository`] and returns an [`InitializationState`].
    pub async fn initialize(&mut self) -> InitializationState {
        match self {
            EnvelopeRepository::Memory(_) => InitializationState::empty(),
            EnvelopeRepository::SQLite(repository) => repository.initialize().await,
        }
    }

    /// Pushes an envelope to the repository for the given project key pair.
    pub async fn push(
        &mut self,
        project_key_pair: ProjectKeyPair,
        envelope: Box<Envelope>,
    ) -> Result<(), EnvelopeBufferError> {
        match self {
            EnvelopeRepository::Memory(repository) => {
                repository.push(project_key_pair, envelope).await?
            }
            EnvelopeRepository::SQLite(repository) => {
                repository.push(project_key_pair, envelope).await?
            }
        }

        Ok(())
    }

    /// Peeks at the next envelope for the given project key pair without removing it.
    pub async fn peek(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<&Envelope>, EnvelopeBufferError> {
        let envelope = match self {
            EnvelopeRepository::Memory(repository) => repository.peek(project_key_pair).await?,
            EnvelopeRepository::SQLite(repository) => repository.peek(project_key_pair).await?,
        };

        Ok(envelope)
    }

    /// Pops and returns the next envelope for the given project key pair.
    pub async fn pop(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<Box<Envelope>>, EnvelopeBufferError> {
        let envelope = match self {
            EnvelopeRepository::Memory(repository) => repository.pop(project_key_pair).await?,
            EnvelopeRepository::SQLite(repository) => repository.pop(project_key_pair).await?,
        };

        Ok(envelope)
    }

    /// Flushes the [`Envelope`]s in the [`EnvelopeRepository`].
    pub async fn flush(&mut self) -> bool {
        match self {
            EnvelopeRepository::Memory(repository) => repository.flush().await,
            EnvelopeRepository::SQLite(repository) => repository.flush().await,
        }
    }

    /// Returns the string representation of the [`EnvelopeRepository`]'s strategy.
    pub fn name(&self) -> &'static str {
        match self {
            EnvelopeRepository::Memory(_) => "memory",
            EnvelopeRepository::SQLite(_) => "sqlite",
        }
    }
}
