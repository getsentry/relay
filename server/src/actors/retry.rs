use std::cmp;
use std::time::Duration;

use actix::dev::ToEnvelope;
use actix::prelude::*;
use futures::future;
use futures::prelude::*;

enum RetryAction {
    /// The attempted action has been completed and no further runs will be necessary.
    Complete,

    /// The attempted action has succeeded but will have to run again (attempts will reset).
    Continue,

    /// The action failed and needs to retry (increasing the delay).
    Retry,
}

struct RunAttempt(u32);

impl Message for RunAttempt {
    type Result = Result<RetryAction, ()>;
}

struct RetryTimer<A: Actor> {
    default_interval: Duration,
    max_interval: Duration,
    addr: Option<Addr<A>>,
    running: bool,
}

impl<A> RetryTimer<A>
where
    A: Actor + Handler<RunAttempt>,
    A::Context: ToEnvelope<A, RunAttempt>,
{
    pub fn new(max_retry_interval: Duration) -> Self {
        RetryTimer::with_batching(max_retry_interval, Duration::new(0, 0))
    }

    pub fn with_batching(default_interval: Duration, max_interval: Duration) -> Self {
        RetryTimer {
            default_interval,
            max_interval,
            addr: None,
            running: false,
        }
    }

    fn schedule_attempt(&mut self, context: &mut Context<Self>, attempt: u32) {
        let timeout = cmp::min(
            self.default_interval + Duration::from_secs(1) * 2u32.pow(attempt - 1),
            self.max_interval,
        );

        context.run_later(timeout, |a, c| a.run_attempt(c, attempt));
    }

    fn run_attempt(&mut self, context: &mut Context<Self>, attempt: u32) {
        if let Some(addr) = self.addr {
            addr.send(RunAttempt(attempt))
                .into_actor(self)
                .drop_err()
                .and_then(|result, actor, context| {
                    future::result(
                        result.map(|action| actor.handle_result(context, action, attempt)),
                    ).into_actor(actor)
                })
                .spawn(context);
        }
    }

    fn handle_result(&mut self, context: &mut Context<Self>, action: RetryAction, attempt: u32) {
        match action {
            RetryAction::Complete => {
                // Done
            }
            RetryAction::Continue => {
                self.schedule_attempt(context, 0);
            }
            RetryAction::Retry => {
                self.schedule_attempt(context, attempt + 1);
            }
        }
    }
}

impl<A: Actor> Actor for RetryTimer<A> {
    type Context = Context<Self>;
}

struct ConnectRetry<A: Actor>(Addr<A>);

impl<A: Actor> Message for ConnectRetry<A> {
    type Result = ();
}

impl<A: Actor> Handler<ConnectRetry<A>> for RetryTimer<A> {
    type Result = ();

    fn handle(&mut self, message: ConnectRetry<A>, context: &mut Self::Context) -> Self::Result {
        self.addr = Some(message.0);
    }
}

struct RequestAttempt;

impl Message for RequestAttempt {
    type Result = ();
}

impl<A> Handler<RequestAttempt> for RetryTimer<A>
where
    A: Actor + Handler<RunAttempt>,
    A::Context: ToEnvelope<A, RunAttempt>,
{
    type Result = ();

    fn handle(&mut self, message: RequestAttempt, context: &mut Self::Context) -> Self::Result {
        if !self.running {
            self.schedule_attempt(context, 0);
        }
    }
}
