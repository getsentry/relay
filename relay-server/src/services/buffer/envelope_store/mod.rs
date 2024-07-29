pub mod sqlite;

use crate::Envelope;
use std::future::Future;

pub trait EnvelopeStore {
    type Envelope;

    type Error;

    fn insert_many(
        &mut self,
        envelopes: impl Iterator<Item = Self::Envelope>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn delete_many(&mut self) -> impl Future<Output = Result<Vec<Envelope>, Self::Error>>;

    fn project_keys_pairs(
        &self,
    ) -> impl Future<Output = Result<impl Iterator<Item = (String, String)>, Self::Error>>;

    fn used_size(&self) -> impl Future<Output = Result<i64, Self::Error>>;
}
