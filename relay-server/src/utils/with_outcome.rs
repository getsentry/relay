use actix::prelude::dev::ToEnvelope;
use actix::prelude::*;
use futures::prelude::*;

use crate::actors::envelopes::EnvelopeContext;
use crate::actors::outcome::{DiscardReason, Outcome, OutcomeProducer};

pub trait SendWithOutcome<Msg>
where
    Msg: Message,
{
    fn send_with_outcome_error(
        &self,
        message: Msg,
        envelope_context: &EnvelopeContext,
        outcome_producer: Addr<OutcomeProducer>,
    ) -> ResponseFuture<Msg::Result, ()>;
}

impl<A, Msg> SendWithOutcome<Msg> for Addr<A>
where
    A: Actor,
    A: Handler<Msg>,
    A::Context: ToEnvelope<A, Msg>,
    Msg: Message + Send + 'static,
    Msg::Result: Send,
{
    fn send_with_outcome_error(
        &self,
        message: Msg,
        envelope_context: &EnvelopeContext,
        outcome_producer: Addr<OutcomeProducer>,
    ) -> ResponseFuture<Msg::Result, ()> {
        let envelope_context = *envelope_context;
        let fut = self.send(message).map_err(move |err| {
            envelope_context
                .send_outcomes(Outcome::Invalid(DiscardReason::Internal), outcome_producer);
            ()
        });
        Box::new(fut)
    }
}
