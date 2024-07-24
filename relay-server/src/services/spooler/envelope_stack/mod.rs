use crate::envelope::Envelope;

mod sqlite;

pub trait EnvelopeStack {
    type Error;

    #[allow(dead_code)]
    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error>;

    #[allow(dead_code)]
    async fn peek(&mut self) -> Result<&Box<Envelope>, Self::Error>;

    #[allow(dead_code)]
    async fn pop(&mut self) -> Result<Box<Envelope>, Self::Error>;
}
