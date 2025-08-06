use std::fmt::{self, Debug};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::JoinHandle;

use futures::future::{FutureExt as _, Shared};

/// The service failed.
#[derive(Debug)]
pub struct ServiceError(tokio::task::JoinError);

impl ServiceError {
    /// Returns true if the error was caused by a panic.
    pub fn is_panic(&self) -> bool {
        self.0.is_panic()
    }

    /// Consumes the error and returns the panic that caused it.
    ///
    /// Returns `None` if the error was not caused by a panic.
    pub fn into_panic(self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        self.0.try_into_panic().ok()
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl std::error::Error for ServiceError {}

/// An owned handle to await the termination of a service.
///
/// This is very similar to a [`std::thread::JoinHandle`] or [`tokio::task::JoinHandle`].
///
/// The handle does not need to be awaited or polled for the service to start execution.
/// On drop, the join handle will detach from the service and the service will continue execution.
pub struct ServiceJoinHandle {
    fut: Option<Shared<MapJoinResult>>,
    error_rx: tokio::sync::oneshot::Receiver<tokio::task::JoinError>,
    handle: tokio::task::AbortHandle,
}

impl ServiceJoinHandle {
    /// Returns `true` if the service has finished.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl Future for ServiceJoinHandle {
    type Output = Result<(), ServiceError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(fut) = &mut self.fut
            && let Ok(()) = futures::ready!(fut.poll_unpin(cx))
        {
            return Poll::Ready(Ok(()));
        }
        self.fut = None;

        match futures::ready!(self.error_rx.poll_unpin(cx)) {
            Ok(error) => Poll::Ready(Err(ServiceError(error))),
            Err(_) => Poll::Ready(Ok(())),
        }
    }
}

/// A [`ServiceError`] without the service error/panic.
///
/// It does not contain the original error, just the status.
#[derive(Debug, Clone)]
pub struct ServiceStatusError {
    is_panic: bool,
}

impl ServiceStatusError {
    /// Returns true if the error was caused by a panic.
    pub fn is_panic(&self) -> bool {
        self.is_panic
    }
}

impl fmt::Display for ServiceStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.is_panic() {
            true => write!(f, "service panic"),
            false => write!(f, "service failed"),
        }
    }
}

impl std::error::Error for ServiceStatusError {}

/// A companion handle to [`ServiceJoinHandle`].
///
/// The handle can also be awaited and queried for the termination status of a service,
/// but unlike the [`ServiceJoinHandle`] it only reports an error status and not
/// the original error/panic.
///
/// This handle can also be freely cloned and therefor awaited multiple times.
#[derive(Debug, Clone)]
pub struct ServiceStatusJoinHandle {
    fut: Shared<MapJoinResult>,
    handle: tokio::task::AbortHandle,
}

impl ServiceStatusJoinHandle {
    /// Returns `true` if the service has finished.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl Future for ServiceStatusJoinHandle {
    type Output = Result<(), ServiceStatusError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

/// Turns a [`tokio::task::JoinHandle<()>`] from a service task into two separate handles.
///
/// Each returned handle can be awaited for the termination of a service
/// and be queried for early termination synchronously using `is_terminated`,
/// but only the [`ServiceJoinHandle`] yields the original error/panic.
pub(crate) fn split(
    handle: tokio::task::JoinHandle<()>,
) -> (ServiceStatusJoinHandle, ServiceJoinHandle) {
    let (tx, rx) = tokio::sync::oneshot::channel();

    let handle1 = handle.abort_handle();
    let handle2 = handle.abort_handle();

    let shared = MapJoinResult {
        handle,
        error: Some(tx),
    }
    .shared();

    (
        ServiceStatusJoinHandle {
            handle: handle1,
            fut: shared.clone(),
        },
        ServiceJoinHandle {
            error_rx: rx,
            handle: handle2,
            fut: Some(shared),
        },
    )
}

/// Utility future which detaches the error/panic of a [`JoinHandle`].
#[derive(Debug)]
struct MapJoinResult {
    handle: JoinHandle<()>,
    error: Option<tokio::sync::oneshot::Sender<tokio::task::JoinError>>,
}

impl Future for MapJoinResult {
    type Output = Result<(), ServiceStatusError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ret = match futures::ready!(self.handle.poll_unpin(cx)) {
            Ok(()) => Ok(()),
            Err(error) => {
                let status = ServiceStatusError {
                    is_panic: error.is_panic(),
                };

                let _ = self
                    .error
                    .take()
                    .expect("shared future to not be ready multiple times")
                    .send(error);

                Err(status)
            }
        };

        Poll::Ready(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_pending {
        ($fut:expr) => {
            match &mut $fut {
                fut => {
                    for _ in 0..30 {
                        assert!(matches!(futures::poll!(&mut *fut), Poll::Pending));
                    }
                }
            }
        };
    }

    #[tokio::test]
    async fn test_split_no_error() {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let (mut status, mut error) = split(crate::spawn!(async move {
            rx.await.unwrap();
        }));

        assert_pending!(status);
        assert_pending!(error);

        assert!(!status.is_finished());
        assert!(!error.is_finished());

        tx.send(()).unwrap();

        assert!(status.await.is_ok());
        assert!(error.is_finished());
        assert!(error.await.is_ok());
    }

    #[tokio::test]
    async fn test_split_with_error_await_status_first() {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let (mut status, mut error) = split(crate::spawn!(async move {
            rx.await.unwrap();
            panic!("test panic");
        }));

        assert_pending!(status);
        assert_pending!(error);

        assert!(!status.is_finished());
        assert!(!error.is_finished());

        tx.send(()).unwrap();

        let status = status.await.unwrap_err();
        assert!(status.is_panic());

        assert!(error.is_finished());

        let error = error.await.unwrap_err();
        assert!(error.is_panic());
        assert!(error.into_panic().unwrap().downcast_ref() == Some(&"test panic"));
    }

    #[tokio::test]
    async fn test_split_with_error_await_error_first() {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let (mut status, mut error) = split(crate::spawn!(async move {
            rx.await.unwrap();
            panic!("test panic");
        }));

        assert_pending!(status);
        assert_pending!(error);

        assert!(!status.is_finished());
        assert!(!error.is_finished());

        tx.send(()).unwrap();

        let error = error.await.unwrap_err();
        assert!(error.is_panic());
        assert!(error.into_panic().unwrap().downcast_ref() == Some(&"test panic"));

        assert!(status.is_finished());

        let status = status.await.unwrap_err();
        assert!(status.is_panic());
    }
}
