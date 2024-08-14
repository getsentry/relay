use std::future::Future;

use hashbrown::HashSet;
use relay_base_schema::project::ProjectKey;

use crate::Envelope;

pub mod sqlite;

/// Trait that models a store of [`Envelope`]s.
pub trait EnvelopeStore {
    type Envelope;

    type Error;

    /// Inserts one or more envelopes into the store.
    fn insert_many(
        &mut self,
        envelopes: impl IntoIterator<Item = Self::Envelope>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Deletes one or more envelopes that match `own_key` and `sampling_key` up to `limit` from
    /// the store.
    fn delete_many(
        &mut self,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<Box<Envelope>>, Self::Error>>;

    /// Returns a set of project key pairs, representing all the unique combinations of
    /// `own_key` and `project_key` that are found in the store.
    #[allow(dead_code)]
    fn project_key_pairs(
        &self,
    ) -> impl Future<Output = Result<HashSet<(ProjectKey, ProjectKey)>, Self::Error>>;

    /// Returns the usage of the store where the definition of usage depends on the implementation.
    fn usage(&self) -> u64;
}
