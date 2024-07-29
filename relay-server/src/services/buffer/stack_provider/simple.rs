use std::marker::PhantomData;
use crate::{Envelope, EnvelopeStack};
use crate::services::buffer::envelope_stack::StackProvider;

pub struct SimpleStackProvider<T>(PhantomData<T>);

impl <S: EnvelopeStack> StackProvider for SimpleStackProvider<S> {
    type Stack = S;
    
    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        S::new(envelope)
    }
}
