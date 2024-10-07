use relay_base_schema::project::ProjectKey;
use std::convert::Infallible;

use crate::services::buffer::envelope_repository::sqlite::SqliteEnvelopeRepositoryError;
use crate::services::buffer::envelope_store::sqlite::SqliteEnvelopeStoreError;
use crate::Envelope;

/// Error that occurs while interacting with the envelope buffer.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeBufferError {
    #[error("sqlite")]
    SqliteStore(#[from] SqliteEnvelopeStoreError),

    #[error("sqlite")]
    SqliteProvider(#[from] SqliteEnvelopeRepositoryError),

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
    /// [`ProjectKey`] of the project of the envelope.
    pub own_key: ProjectKey,
    /// [`ProjectKey`] of the root project of the trace to which the envelope belongs.
    pub sampling_key: ProjectKey,
}

impl ProjectKeyPair {
    /// Creates a new [`ProjectKeyPair`] with the given `own_key` and `sampling_key`.
    pub fn new(own_key: ProjectKey, sampling_key: ProjectKey) -> Self {
        Self {
            own_key,
            sampling_key,
        }
    }

    /// Creates a [`ProjectKeyPair`] from an [`Envelope`].
    ///
    /// The `own_key` is set to the public key from the envelope's metadata.
    /// The `sampling_key` is set to the envelope's sampling key if present,
    /// otherwise it defaults to the `own_key`.
    pub fn from_envelope(envelope: &Envelope) -> Self {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);
        Self::new(own_key, sampling_key)
    }

    /// Returns an iterator over the project keys.
    ///
    /// The iterator always yields the `own_key` and yields the `sampling_key`
    /// only if it's different from the `own_key`.
    pub fn iter(&self) -> impl Iterator<Item = ProjectKey> {
        let Self {
            own_key,
            sampling_key,
        } = self;
        std::iter::once(*own_key).chain((own_key != sampling_key).then_some(*sampling_key))
    }
}
