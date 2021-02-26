use actix::{Message, Recipient};
use futures::{Async, Future, Poll};

/// Message send on the notification channel when the tracked future finishes or is disposed.
pub struct TrackedFutureFinished;

impl Message for TrackedFutureFinished {
    type Result = ();
}

pub trait IntoTracked<F>
where
    F: Future,
{
    fn track(self, notifier: Recipient<TrackedFutureFinished>) -> TrackedFuture<F>;
}

pub struct TrackedFuture<F> {
    notified: bool,
    notifier: Recipient<TrackedFutureFinished>,
    inner: F,
}

impl<F> TrackedFuture<F> {
    fn notify(&mut self) {
        self.notifier
            .do_send(TrackedFutureFinished)
            .map_err(|_| {
                relay_log::error!("TrackedFuture could not notify completion");
            })
            .ok();
    }
}

impl<F> IntoTracked<F> for F
where
    F: Future,
{
    fn track(self, notifier: Recipient<TrackedFutureFinished>) -> TrackedFuture<F> {
        TrackedFuture {
            notified: false,
            inner: self,
            notifier,
        }
    }
}

impl<F> Future for TrackedFuture<F>
where
    F: Future,
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let ret_val = self.inner.poll();

        match ret_val {
            Ok(Async::NotReady) => {}
            _ => {
                // future is finished notify channel
                self.notified = true;
                self.notify();
            }
        }
        ret_val
    }
}

impl<F> Drop for TrackedFuture<F> {
    fn drop(&mut self) {
        if !self.notified {
            //future dropped without being brought to completion
            relay_log::info!("Tracked future dropped without being disposed");
            self.notify();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::utils::test;
    use actix::{Actor, Context, Handler};
    use failure::_core::time::Duration;
    use futures::sync::oneshot::{channel, Sender};
    use tokio_timer::Timeout;

    struct TestActor {
        notifier: Option<Sender<()>>,
    }

    impl TestActor {
        pub fn new(notifier: Sender<()>) -> Self {
            TestActor {
                notifier: Some(notifier),
            }
        }
    }

    impl Actor for TestActor {
        type Context = Context<Self>;
    }

    impl Handler<TrackedFutureFinished> for TestActor {
        type Result = ();

        fn handle(&mut self, _message: TrackedFutureFinished, _ctx: &mut Self::Context) {
            let notifier = self.notifier.take().unwrap();
            notifier.send(()).ok();
        }
    }

    struct NeverEndingFuture;

    impl Future for NeverEndingFuture {
        type Item = ();
        type Error = ();

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            relay_log::debug!("never ending future poll");
            Ok(Async::NotReady)
        }
    }

    #[test]
    fn test_tracked_future_termination() {
        test::setup();
        let mut futures = [
            Some(futures::future::ok::<bool, ()>(true)),
            Some(futures::future::err::<bool, ()>(())),
        ];
        for future in &mut futures {
            test::block_fn(|| {
                let (tx, rx) = channel::<()>();
                let addr = TestActor::new(tx).start();
                let rec = addr.recipient::<TrackedFutureFinished>();

                let fut = future.take().unwrap().track(rec).map(|_| ());

                actix::spawn(fut);

                Timeout::new(rx, Duration::from_millis(10)).map_err(|_| {
                    relay_log::error!("tracked future didn't not send a notification");
                    panic!("no notification received before timeout");
                })
            })
            .ok();
        }
    }

    #[test]
    fn test_tracked_future_dropped() {
        test::setup();
        test::block_fn(|| {
            let (tx, rx) = channel::<()>();
            let addr = TestActor::new(tx).start();
            let rec = addr.recipient::<TrackedFutureFinished>();

            // dispose of future before it finishes (don't even schedule it)
            {
                let _fut = NeverEndingFuture.track(rec).map(|_| ());
            }
            Timeout::new(rx, Duration::from_millis(10)).map_err(|_| {
                relay_log::error!("tracked future didn't not send a notification");
                panic!("no notification received before timeout");
            })
        })
        .ok();
    }
}
