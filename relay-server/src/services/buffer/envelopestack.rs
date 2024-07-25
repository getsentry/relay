use crate::envelope::Envelope;

pub trait EnvelopeStack {
    fn new(envelope: Box<Envelope>) -> Self;

    fn push(&mut self, envelope: Box<Envelope>);

    fn pop(&mut self) -> Option<Box<Envelope>>;

    fn peek(&self) -> Option<&Envelope>;
}
