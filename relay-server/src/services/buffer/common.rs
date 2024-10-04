use relay_base_schema::project::ProjectKey;
use std::convert::Infallible;

use crate::services::buffer::envelope_provider::sqlite::SqliteEnvelopeProviderError;
use crate::services::buffer::envelope_store::sqlite::SqliteEnvelopeStoreError;
use crate::Envelope;

/// Error that occurs while interacting with the envelope buffer.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeBufferError {
    #[error("sqlite")]
    SqliteStore(#[from] SqliteEnvelopeStoreError),

    #[error("sqlite")]
    SqliteProvider(#[from] SqliteEnvelopeProviderError),

    #[error("failed to push envelope to the buffer")]
    PushFailed,
}

impl From<Infallible> for EnvelopeBufferError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

/// Struct that represents two project keys.
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ProjectKeyPair {
    pub own_key: ProjectKey,
    pub sampling_key: ProjectKey,
}

impl ProjectKeyPair {
    pub fn new(own_key: ProjectKey, sampling_key: ProjectKey) -> Self {
        Self {
            own_key,
            sampling_key,
        }
    }

    pub fn from_envelope(envelope: &Envelope) -> Self {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);
        Self::new(own_key, sampling_key)
    }

    pub fn iter(&self) -> impl Iterator<Item = ProjectKey> {
        let Self {
            own_key,
            sampling_key,
        } = self;
        std::iter::once(*own_key).chain((own_key != sampling_key).then_some(*sampling_key))
    }
}