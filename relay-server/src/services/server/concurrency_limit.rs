use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;
use tower::{Layer, Service};

/// Enforces a limit on the concurrent number of services that can be created by a [`tower::MakeService`].
#[derive(Debug, Clone)]
pub struct ConcurrencyLimitLayer {
    permits: Option<usize>,
}

impl ConcurrencyLimitLayer {
    pub fn new(permits: Option<usize>) -> Self {
        Self { permits }
    }
}

impl<S> Layer<S> for ConcurrencyLimitLayer {
    type Service = ConcurrencyLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyLimit::new(inner, self.permits)
    }
}

/// Enforces a concurrency limit on a [`tower::MakeService`].
///
/// The yielded service keeps the permit alive until it is discarded.
/// This limits the amount of concurrently created services by this [`tower::MakeService`].
pub struct ConcurrencyLimit<T> {
    inner: T,
    semaphore: Option<PollSemaphore>,
    permit: Option<Permit>,
}

impl<T> ConcurrencyLimit<T> {
    pub fn new(inner: T, permits: Option<usize>) -> Self {
        Self {
            inner,
            semaphore: permits.map(|permits| PollSemaphore::new(Arc::new(Semaphore::new(permits)))),
            permit: None,
        }
    }
}

impl<S, Request> Service<Request> for ConcurrencyLimit<S>
where
    S: Service<Request>,
{
    type Response = Permitted<S::Response>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.permit.is_none() {
            self.permit = match self.semaphore.as_mut() {
                Some(semaphore) => {
                    let permit = futures::ready!(semaphore.poll_acquire(cx));
                    debug_assert!(
                        permit.is_some(),
                        "Semaphore is never closed, so `poll_acquire` should never fail"
                    );
                    permit
                        .map(Arc::new)
                        .map(|_permit| Permit::Permitted { _permit })
                }
                None => Some(Permit::Unlimited),
            }
        }

        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let permit = self.permit.take().expect("poll_ready must be called first");

        let future = self.inner.call(req);
        ResponseFuture::new(future, permit)
    }
}

impl<T: Clone> Clone for ConcurrencyLimit<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            semaphore: self.semaphore.clone(),
            permit: None,
        }
    }
}

#[derive(Clone, Debug)]
enum Permit {
    Unlimited,
    Permitted { _permit: Arc<OwnedSemaphorePermit> },
}

pin_project_lite::pin_project! {
    /// Future for the [`ConcurrencyLimit`] service.
    #[derive(Debug)]
    pub struct ResponseFuture<T> {
        #[pin]
        inner: T,
        permit: Permit,
    }
}

impl<T> ResponseFuture<T> {
    fn new(inner: T, permit: Permit) -> ResponseFuture<T> {
        ResponseFuture { inner, permit }
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<Permitted<T>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let inner = match futures::ready!(this.inner.poll(cx)) {
            Ok(inner) => inner,
            Err(err) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(Permitted {
            inner,
            _permit: this.permit.clone(),
        }))
    }
}

/// A service holding a permit.
///
/// Clones of the service all share the same permit.
#[derive(Clone)]
pub struct Permitted<T> {
    inner: T,
    _permit: Permit,
}

impl<S, Request> Service<Request> for Permitted<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req)
    }
}
