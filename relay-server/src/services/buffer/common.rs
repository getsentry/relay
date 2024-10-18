use relay_base_schema::project::ProjectKey;

use crate::Envelope;

/// Struct that represents two project keys.
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ProjectKeyPair {
    /// [`ProjectKey`] of the project to which the event belongs.
    pub own_key: ProjectKey,
    /// [`ProjectKey`] of the root project of the trace to which the event belongs.
    pub sampling_key: ProjectKey,
}

impl ProjectKeyPair {
    /// Creates a new [`ProjectKeyPair`].
    pub fn new(own_key: ProjectKey, sampling_key: ProjectKey) -> Self {
        Self {
            own_key,
            sampling_key,
        }
    }

    /// Creates a new [`ProjectKeyPair`] from an [`Envelope`].
    pub fn from_envelope(envelope: &Envelope) -> Self {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);
        Self::new(own_key, sampling_key)
    }

    /// Returns an iterator that yields the `own_key` and `sampling_key`.
    ///
    /// The `sampling_key` will be returned as a second element of the iterator only if
    /// it's different from the `own_key`.
    pub fn iter(&self) -> impl Iterator<Item = ProjectKey> {
        let Self {
            own_key,
            sampling_key,
        } = self;
        std::iter::once(*own_key).chain((own_key != sampling_key).then_some(*sampling_key))
    }
}
