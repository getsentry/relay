use actix::prelude::*;
use futures::prelude::*;

pub struct DropGuardedFuture<F: Sized> {
    name: &'static str,
    future: F,
    done: bool,
}

impl<F> DropGuardedFuture<F> {
    pub fn new(name: &'static str, future: F) -> Self {
        DropGuardedFuture {
            name,
            future,
            done: false,
        }
    }
}

impl<F> Drop for DropGuardedFuture<F> {
    fn drop(&mut self) {
        if !self.done {
            if cfg!(test) {
                panic!("Dropped unfinished future during shutdown: {}", self.name);
            } else {
                relay_log::error!("Dropped unfinished future during shutdown: {}", self.name);
            }
        }
    }
}

impl<F> Future for DropGuardedFuture<F>
where
    F: Future,
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let rv = self.future.poll();
        self.done = !matches!(rv, Ok(Async::NotReady));
        rv
    }
}

impl<F> ActorFuture for DropGuardedFuture<F>
where
    F: ActorFuture,
{
    type Item = F::Item;
    type Error = F::Error;
    type Actor = F::Actor;

    fn poll(
        &mut self,
        srv: &mut Self::Actor,
        ctx: &mut <Self::Actor as Actor>::Context,
    ) -> Poll<Self::Item, Self::Error> {
        let rv = self.future.poll(srv, ctx);
        self.done = !matches!(rv, Ok(Async::NotReady));
        rv
    }
}

pub trait FutureExt: Sized {
    fn drop_guard(self, name: &'static str) -> DropGuardedFuture<Self> {
        DropGuardedFuture::new(name, self)
    }
}

impl<F> FutureExt for F where F: Sized {}

#[test]
#[should_panic(expected = "Dropped unfinished future during shutdown: bye")]
fn test_basic() {
    use std::time::{Duration, Instant};
    use tokio_timer::Delay;

    System::run(|| {
        actix::spawn(
            Delay::new(Instant::now() + Duration::from_millis(100)).then(|_| {
                System::current().stop();
                Ok(())
            }),
        );

        actix::spawn(
            Delay::new(Instant::now() + Duration::from_millis(200))
                .drop_guard("bye")
                .then(|_| Ok(())),
        );
    });
}
