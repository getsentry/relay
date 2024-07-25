use crate::envelope::Envelope;

pub trait EnvelopeStack: Send {
    fn new(envelope: Box<Envelope>) -> Self;

    fn push(&mut self, envelope: Box<Envelope>);

    fn pop(&mut self) -> Option<Box<Envelope>>;

    fn peek(&self) -> Option<&Envelope>;
}

#[derive(Debug)]
pub struct InMemoryEnvelopeStack(Vec<Box<Envelope>>);

impl EnvelopeStack for InMemoryEnvelopeStack {
    fn new(envelope: Box<Envelope>) -> Self {
        Self(vec![envelope])
    }

    fn push(&mut self, envelope: Box<Envelope>) {
        self.0.push(envelope)
    }

    fn pop(&mut self) -> Option<Box<Envelope>> {
        self.0.pop()
    }

    fn peek(&self) -> Option<&Envelope> {
        self.0.last().map(Box::as_ref)
    }
}
