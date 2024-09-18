use relay_base_schema::project::ProjectKey;
use std::time::Instant;

use crate::Envelope;

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

pub struct ProjectKeyPairFetch {
    own_project_next_fetch: Option<Instant>,
    sampling_project_next_fetch: Option<Instant>,
}

impl ProjectKeyPairFetch {
    pub fn new(
        own_project_next_fetch: Option<Instant>,
        sampling_project_next_fetch: Option<Instant>,
    ) -> Self {
        Self {
            own_project_next_fetch,
            sampling_project_next_fetch,
        }
    }

    pub fn fetch_own_project_key(&self) -> bool {
        self.own_project_next_fetch
            .map_or(false, |n| Instant::now() >= n)
    }

    pub fn fetch_sampling_project_key(&self) -> bool {
        self.sampling_project_next_fetch
            .map_or(false, |n| Instant::now() >= n)
    }
}
