use std::any::type_name;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::*;
use failure::Fail;
use futures::future::Shared;
use futures::prelude::*;
use futures::sync::oneshot;
use parking_lot::RwLock;
use tokio_timer::Timeout;

#[derive(Debug, Fail, Copy, Clone, Eq, PartialEq)]
#[fail(display = "timed out")]
pub struct TimeoutError;

#[derive(Debug)]
struct SyncHandleInner {
    count: AtomicUsize,
    sync_tx: RwLock<Option<oneshot::Sender<()>>>,
}

impl SyncHandleInner {
    pub fn new() -> Self {
        SyncHandleInner {
            count: AtomicUsize::new(0),
            sync_tx: RwLock::new(None),
        }
    }

    pub fn acquire(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn release(&self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
        self.check();
    }

    fn check(&self) {
        if self.count.load(Ordering::Acquire) == 0 {
            if let Some(tx) = self.sync_tx.write().take() {
                tx.send(()).ok();
            }
        }
    }
}

pub struct SyncHandle {
    inner: Arc<SyncHandleInner>,
    timeout_tx: Option<oneshot::Sender<()>>,
    timeout_rx: Shared<oneshot::Receiver<()>>,
    future: Option<Shared<ResponseFuture<(), ()>>>,
}

impl SyncHandle {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();

        SyncHandle {
            inner: Arc::new(SyncHandleInner::new()),
            timeout_tx: Some(sender),
            timeout_rx: receiver.shared(),
            future: None,
        }
    }

    pub fn now(&mut self) -> ResponseFuture<(), TimeoutError> {
        self.timeout(Duration::new(0, 0))
    }

    pub fn timeout(&mut self, timeout: Duration) -> ResponseFuture<(), TimeoutError> {
        let future = match self.future {
            Some(ref future) => future,
            None => {
                let timeout_tx = self
                    .timeout_tx
                    .take()
                    .expect("SyncHandle future created twice");

                let (sync_tx, sync_rx) = oneshot::channel();
                *self.inner.sync_tx.write() = Some(sync_tx);
                self.inner.check();

                let future = Box::new(Timeout::new(sync_rx, timeout).map_err(|_| {
                    timeout_tx.send(()).ok();
                })) as ResponseFuture<(), ()>;

                self.future.get_or_insert(future.shared())
            }
        };

        Box::new(future.clone().then(|result| match result {
            Ok(_) => Ok(()),
            Err(_) => Err(TimeoutError),
        }))
    }

    pub fn requested(&self) -> bool {
        self.future.is_some()
    }
}

#[derive(Debug)]
pub struct SyncedFuture<F, E> {
    sync: Arc<SyncHandleInner>,
    inner: Option<(F, Shared<oneshot::Receiver<()>>)>,
    error: Option<E>,
}

impl<F, E> SyncedFuture<F, E> {
    pub fn new(inner: F, handle: &SyncHandle, error: E) -> Self {
        handle.inner.acquire();

        SyncedFuture {
            sync: handle.inner.clone(),
            inner: Some((inner, handle.timeout_rx.clone())),
            error: Some(error),
        }
    }

    fn poll<P, R>(&mut self, poll: P) -> Poll<R, E>
    where
        P: FnOnce(&mut F) -> Poll<R, E>,
    {
        // NOTE: This implementation is taken from `futures::future::Select2`. We cannot use that
        // here directly since `ActorFuture` does not offer the same functionality.
        let (mut future, mut timeout) = self.inner.take().expect("cannot poll SyncedFuture twice");

        let result = match poll(&mut future) {
            Err(e) => Err(e),
            Ok(Async::Ready(v)) => Ok(Async::Ready(v)),
            Ok(Async::NotReady) => match timeout.poll() {
                Err(_) => {
                    log::error!(
                        "synced future spawned after timeout expired (Item = {}, Error = {})",
                        type_name::<R>(),
                        type_name::<E>()
                    );

                    Err(self.error.take().unwrap())
                }
                Ok(Async::Ready(_)) => Err(self.error.take().unwrap()),
                Ok(Async::NotReady) => {
                    self.inner = Some((future, timeout));
                    Ok(Async::NotReady)
                }
            },
        };

        if self.inner.is_none() {
            self.sync.release();
        }

        result
    }
}

impl<F, E> Future for SyncedFuture<F, E>
where
    F: Future<Error = E>,
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.poll(F::poll)
    }
}

impl<F, E> ActorFuture for SyncedFuture<F, E>
where
    F: ActorFuture<Error = E>,
{
    type Item = F::Item;
    type Error = F::Error;
    type Actor = F::Actor;

    fn poll(
        &mut self,
        srv: &mut Self::Actor,
        ctx: &mut <Self::Actor as Actor>::Context,
    ) -> Poll<Self::Item, Self::Error> {
        self.poll(|future| future.poll(srv, ctx))
    }
}

pub trait SyncFuture: Future {
    fn sync(self, handle: &SyncHandle, error: Self::Error) -> SyncedFuture<Self, Self::Error>
    where
        Self: Sized,
    {
        SyncedFuture::new(self, handle, error)
    }
}

impl<F: Future> SyncFuture for F {}

pub trait SyncActorFuture: ActorFuture {
    fn sync(self, handle: &SyncHandle, error: Self::Error) -> SyncedFuture<Self, Self::Error>
    where
        Self: Sized,
    {
        SyncedFuture::new(self, handle, error)
    }
}

impl<F: ActorFuture> SyncActorFuture for F {}
