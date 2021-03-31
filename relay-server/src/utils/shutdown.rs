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

#[cfg(test)]
mod tests {
    use actix::clock::Clock;

    #[test]
    #[should_panic(expected = "Dropped unfinished future during shutdown: bye")]
    fn test_basic() {
        use std::time::Duration;

        use super::*;
        let mut test_now = relay_test::TestNow::new();
        let test_clock = Clock::new_with_now(test_now.clone());
        let sys = System::builder().clock(test_clock);

        sys.run(move || {
            actix::spawn(relay_test::delay(Duration::from_millis(1000)).then(|_| {
                println!("Stopping...");
                actix::System::current().stop();
                Ok(())
            }));

            actix::spawn(
                relay_test::delay(Duration::from_millis(2000))
                    .drop_guard("bye")
                    .then(|_| Ok(())),
            );

            test_now.advance(Duration::from_millis(1500));
        });
    }
}
