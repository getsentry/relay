use actix::prelude::dev::ToEnvelope;
use actix::prelude::*;
use futures::prelude::*;

use crate::actors::envelopes::EnvelopeContext;
use crate::actors::outcome::{DiscardReason, Outcome, OutcomeProducer};

pub trait SendWithOutcome<A> {
    fn send_with_outcome_error<Msg>(
        &self,
        message: Msg,
        envelope_context: EnvelopeContext,
        outcome_producer: Addr<OutcomeProducer>,
    ) -> ResponseFuture<Msg::Result, ()>
    where
        Msg: Message,
        A: Handler<Msg>,
        A::Context: ToEnvelope<A, Msg>,
        Msg: Message + Send + 'static,
        Msg::Result: Send;
}

impl<A> SendWithOutcome<A> for Addr<A>
where
    A: Actor,
{
    fn send_with_outcome_error<Msg>(
        &self,
        message: Msg,
        envelope_context: EnvelopeContext,
        outcome_producer: Addr<OutcomeProducer>,
    ) -> ResponseFuture<Msg::Result, ()>
    where
        A: Handler<Msg>,
        A::Context: ToEnvelope<A, Msg>,
        Msg: Message + Send + 'static,
        Msg::Result: Send,
    {
        let fut = self.send(message).map_err(move |_| {
            envelope_context
                .send_outcomes(Outcome::Invalid(DiscardReason::Internal), outcome_producer);
        });
        Box::new(fut)
    }
}
