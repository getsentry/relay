use crate::envelope::Envelope;

mod sqlite;

pub trait EnvelopeStack {
    async fn push(&mut self, envelope: Envelope);

    async fn peek(&self) -> Option<&Envelope>;

    async fn pop(&mut self) -> Option<Envelope>;
}
