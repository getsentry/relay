use actix::{fut, prelude::*};
use std::time::{Duration, Instant};
use tokio_timer::Delay;

pub fn run_later<A, F>(
    timeout: Duration,
    f: F,
) -> impl ActorFuture<Item = (), Error = (), Actor = A>
where
    A: Actor,
    F: FnOnce(&mut A, &mut A::Context) + 'static,
{
    fut::wrap_future(Delay::new(Instant::now() + timeout)).then(|_, actor, context| {
        f(actor, context);
        fut::ok(())
    })
}
