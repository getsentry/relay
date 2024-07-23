use crate::envelope::Envelope;
use crate::services::spooler::envelope_stack::EnvelopeStack;
use relay_base_schema::project::ProjectKey;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Pool, QueryBuilder, Sqlite};
use std::collections::VecDeque;
// Threshold x
// When x is reached, we flush x / 2 oldest elements to disk
// When the stack is empty we load x / 2 from disk (to give a bit of leeway for incoming elements)
// The idea is that we want to keep the most fresh data in memory and flush it to disk only when
// a threshold is surpassed, we also don't want to flush all the data, since we want to keep the msot
// recent data in memory as much as possible.

// TODO:
//  add metrics to counter how many insertions we have vs how many reads we have per second

pub struct SQLiteEnvelopeStack {
    db: Pool<Sqlite>,
    spool_threshold: usize,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    buffer: VecDeque<Envelope>,
}

impl SQLiteEnvelopeStack {
    pub fn new(db: Pool<Sqlite>, spool_threshold: usize) -> Self {
        Self {
            db,
            spool_threshold,
            buffer: VecDeque::with_capacity(spool_threshold),
        }
    }

    fn hit_spool_threshold(&self) -> bool {
        self.buffer.len() + 1 > self.spool_threshold
    }

    async fn spool_to_disk(&mut self) {
        // TODO: we can make a custom iterator to consume back elements until threshold to avoid
        //  allocating a vector.
        let mut envelopes = Vec::with_capacity(self.spool_threshold / 2);
        for _ in 0..(self.spool_threshold / 2) {
            let Some(value) = self.buffer.pop_back() else {
                break;
            };

            envelopes.push(value);
        }

        let insert_envelopes = envelopes.iter().map(|e| InsertEnvelope {
            received_at: received_at(e),
            own_key: self.own_key,
            sampling_key: self.sampling_key,
            encoded_envelope: e.to_vec().unwrap(),
        });
        let result = build_insert_many_envelopes(insert_envelopes)
            .build()
            .execute(&self.db)
            .await;
    }

    async fn load_from_disk(&mut self) {}
}

impl EnvelopeStack for SQLiteEnvelopeStack {
    async fn push(&mut self, envelope: Envelope) {
        if self.hit_spool_threshold() {
            self.spool_to_disk();
        }

        self.buffer.push_front(envelope);
    }

    async fn peek(&self) -> Option<&Envelope> {
        self.buffer.back()
    }

    async fn pop(&mut self) -> Option<Envelope> {
        todo!()
    }
}

struct InsertEnvelope {
    received_at: i64,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    encoded_envelope: Vec<u8>,
}

fn build_insert_many_envelopes<'a>(
    envelopes: impl Iterator<Item = InsertEnvelope>,
) -> QueryBuilder<'a, Sqlite> {
    let mut builder: QueryBuilder<Sqlite> =
        QueryBuilder::new("INSERT INTO envelopes (received_at, own_key, sampling_key, envelope) ");

    builder.push_values(envelopes, |mut b, envelope| {
        b.push_bind(envelope.received_at)
            .push_bind(envelope.own_key.to_string())
            .push_bind(envelope.sampling_key.to_string())
            .push_bind(envelope.encoded_envelope);
    });

    builder
}

fn received_at(envelope: &Envelope) -> i64 {
    relay_common::time::instant_to_date_time(envelope.meta().start_time()).timestamp_millis()
}
