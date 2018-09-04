use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::prelude::*;
use futures::prelude::*;
use parking_lot::RwLock;

#[derive(Debug, Fail)]
#[fail(display = "timed out")]
pub struct TimeoutError;

pub struct Shutdown {
    pub timeout: Option<Duration>,
}

impl Message for Shutdown {
    type Result = Result<(), TimeoutError>;
}

#[derive(Debug)]
struct SyncHandleInner {
    count: AtomicUsize,
    timeout: RwLock<Option<Instant>>,
}

impl SyncHandleInner {
    fn new() -> Self {
        SyncHandleInner {
            count: AtomicUsize::new(0),
            timeout: RwLock::new(None),
        }
    }

    pub fn remaining(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn timed_out(&self) -> bool {
        self.timeout.read().map_or(false, |t| Instant::now() >= t)
    }

    pub fn acquire(&mut self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn release(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct SyncHandle {
    inner: Arc<SyncHandleInner>,
}

impl SyncHandle {
    pub fn new() -> Self {
        SyncHandle {
            inner: Arc::new(SyncHandleInner::new()),
        }
    }

    pub fn start(&mut self, timeout: Duration) {
        self.inner
            .timeout
            .write()
            .get_or_insert_with(|| Instant::now() + timeout);
    }

    pub fn now(&mut self) {
        *self.inner.timeout.write() = Some(Instant::now())
    }

    pub fn started(&self) -> bool {
        self.inner.timeout.read().is_some()
    }

    pub fn succeeded(&self) -> bool {
        self.inner.remaining() == 0
    }

    pub fn timed_out(&self) -> bool {
        self.inner.timed_out()
    }
}

impl Default for SyncHandle {
    fn default() -> Self {
        SyncHandle::new()
    }
}

impl Future for SyncHandle {
    type Item = ();
    type Error = TimeoutError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.succeeded() {
            Ok(Async::Ready(()))
        } else if self.timed_out() {
            Err(TimeoutError)
        } else {
            Ok(Async::NotReady)
        }
    }
}

#[derive(Debug)]
pub struct SyncedFuture<F, E> {
    handle: Arc<SyncHandleInner>,
    inner: F,
    error: Option<E>,
}

impl<F, E> SyncedFuture<F, E> {
    pub fn new(inner: F, handle: &SyncHandle, error: E) -> Self {
        handle.inner.acquire();
        SyncedFuture {
            handle: handle.inner.clone(),
            inner,
            error: Some(error),
        }
    }
}

impl<F, E> Future for SyncedFuture<F, E>
where
    F: Future<Error = E>,
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.handle.timed_out() {
            return Err(self.error.take().unwrap());
        }

        let poll = self.inner.poll();
        if let Ok(Async::Ready(_)) = &poll {
            self.handle.release();
        }
        poll
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
        if self.handle.timed_out() {
            return Err(self.error.take().unwrap());
        }

        let poll = self.inner.poll(srv, ctx);
        if let Ok(Async::Ready(_)) = &poll {
            self.handle.release();
        }
        poll
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
