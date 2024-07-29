pub mod sqlite;

use crate::Envelope;
use relay_base_schema::project::ProjectKey;
use std::future::Future;

pub trait EnvelopeStore {
    type Envelope;

    type Error;

    fn insert_many(
        &mut self,
        envelopes: impl Iterator<Item = Self::Envelope>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn delete_many(
        &mut self,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<Box<Envelope>>, Self::Error>>;

    fn project_keys_pairs(
        &self,
    ) -> impl Future<Output = Result<impl Iterator<Item = (String, String)>, Self::Error>>;

    fn used_size(&self) -> impl Future<Output = Result<i64, Self::Error>>;
}
