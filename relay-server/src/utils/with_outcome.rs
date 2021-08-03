use actix::prelude::dev::ToEnvelope;
use actix::prelude::*;
use futures::prelude::*;

use crate::actors::envelopes::EnvelopeContext;
use crate::actors::outcome::{DiscardReason, Outcome};

/// Extension trait for [`Addr`] to log [`Outcome`] for failed messages.
pub trait SendWithOutcome<A> {
    /// Sends an asynchronous message to the actor, tracking outcomes on failure.
    ///
    /// Communication channel to the actor is bounded. If the returned `Future` object get dropped,
    /// the message cancels.
    ///
    /// If the actor rejects the message, an `Invalid` outcome with internal reason is logged. Any
    /// error within the message result is not tracked by this function.
    fn send_tracked<M>(
        &self,
        message: M,
        envelope_context: EnvelopeContext,
    ) -> ResponseFuture<M::Result, MailboxError>
    where
        M: Message,
        A: Handler<M>,
        A::Context: ToEnvelope<A, M>,
        M: Message + Send + 'static,
        M::Result: Send;
}

impl<A> SendWithOutcome<A> for Addr<A>
where
    A: Actor,
{
    fn send_tracked<M>(
        &self,
        message: M,
        envelope_context: EnvelopeContext,
    ) -> ResponseFuture<M::Result, MailboxError>
    where
        A: Handler<M>,
        A::Context: ToEnvelope<A, M>,
        M: Message + Send + 'static,
        M::Result: Send,
    {
        let future = self.send(message).map_err(move |mailbox_error| {
            envelope_context.send_outcomes(Outcome::Invalid(DiscardReason::Internal));
            mailbox_error
        });

        Box::new(future)
    }
}
