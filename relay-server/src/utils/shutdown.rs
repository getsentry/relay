use futures01::prelude::*;

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

pub trait FutureExt: Sized {
    fn drop_guard(self, name: &'static str) -> DropGuardedFuture<Self> {
        DropGuardedFuture::new(name, self)
    }
}

impl<F> FutureExt for F where F: Sized {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actix::System;

    use super::*;

    #[test]
    #[should_panic(expected = "Dropped unfinished future during shutdown: bye")]
    fn test_drop_guard() {
        System::run(|| {
            actix::spawn(
                relay_test::delay(Duration::from_secs(10))
                    .drop_guard("bye")
                    .then(|_| Ok(())),
            );

            actix::System::current().stop();
        });
    }

    #[test]
    fn test_no_drop() {
        System::run(|| {
            actix::spawn(
                relay_test::delay(Duration::from_millis(100))
                    .drop_guard("bye")
                    .then(|_| {
                        actix::System::current().stop();
                        Ok(())
                    }),
            );
        });
    }
}
