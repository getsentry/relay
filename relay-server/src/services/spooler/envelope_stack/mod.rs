use crate::envelope::Envelope;

mod sqlite;

pub trait EnvelopeStack {
    type Error;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error>;

    async fn peek(&mut self) -> Result<&Box<Envelope>, Self::Error>;

    async fn pop(&mut self) -> Result<Box<Envelope>, Self::Error>;
}
