use futures::{sync::mpsc::Sender, Async, Future, Poll};

pub trait IntoTracked<F>
where
    F: Future,
{
    fn to_tracked(self, notifier: Sender<()>) -> TrackedFuture<F>;
}

pub struct TrackedFuture<F> {
    notified: bool,
    notifier: Sender<()>,
    inner: F,
}

impl<F> IntoTracked<F> for F
where
    F: Future,
{
    fn to_tracked(self, notifier: Sender<()>) -> TrackedFuture<F> {
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
                // TODO is this correct, I think I should use start_send() but don't know how,
                self.notifier
                    .try_send(())
                    .map_err(|_| {
                        log::error!("TrackedFuture could not notify completion");
                    })
                    .ok();
            }
        }
        ret_val
    }
}

impl<F> Drop for TrackedFuture<F> {
    fn drop(&mut self) {
        if !self.notified {
            //future dropped without being brought to completion
            self.notifier
                .try_send(())
                .map_err(|_| {
                    log::error!("TrackedFuture could not notify completion");
                })
                .ok();
        }
    }
}
