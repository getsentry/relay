use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::JoinHandle;

use futures::future::{FutureExt as _, Shared};

pub struct ServiceStatus(tokio::task::JoinError);

impl ServiceStatus {
    pub fn is_panic(&self) -> bool {
        self.0.is_panic()
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    pub fn into_panic(self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        self.0.try_into_panic().ok()
    }
}

pub struct ServiceJoinHandle {
    fut: Option<Shared<MapJoinResult>>,
    error_rx: tokio::sync::oneshot::Receiver<tokio::task::JoinError>,
    handle: tokio::task::AbortHandle,
}

impl ServiceJoinHandle {
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl Future for ServiceJoinHandle {
    type Output = Result<(), ServiceStatus>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(fut) = &mut self.fut {
            match futures::ready!(fut.poll_unpin(cx)) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(JoinError::Error(error)) => return Poll::Ready(Err(ServiceStatus(error))),
                Err(JoinError::ErrorGone { .. }) => {}
            }
        }
        self.fut = None;

        match futures::ready!(self.error_rx.poll_unpin(cx)) {
            Ok(error) => Poll::Ready(Err(ServiceStatus(error))),
            Err(_) => Poll::Ready(Ok(())),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServiceStatusWithoutError {
    pub is_cancelled: bool,
    pub is_panic: bool,
}

#[derive(Debug)]
pub(crate) struct ServiceJoinHandleNoError {
    error: Option<tokio::sync::oneshot::Sender<tokio::task::JoinError>>,
    fut: Shared<MapJoinResult>,
    handle: tokio::task::AbortHandle,
}

impl ServiceJoinHandleNoError {
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl Future for ServiceJoinHandleNoError {
    type Output = Result<(), ServiceStatusWithoutError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = match futures::ready!(self.fut.poll_unpin(cx)) {
            Ok(()) => Ok(()),
            Err(JoinError::Error(error)) => {
                let status = ServiceStatusWithoutError {
                    is_cancelled: error.is_cancelled(),
                    is_panic: error.is_panic(),
                };

                // Move the error to the sibling future, if there is still interest.
                let _ = self
                    .error
                    .take()
                    .expect("future to not be completed")
                    .send(error);

                Err(status)
            }
            Err(JoinError::ErrorGone {
                is_cancelled,
                is_panic,
            }) => Err(ServiceStatusWithoutError {
                is_cancelled,
                is_panic,
            }),
        };

        // Notify the other side that there will never be an error.
        //
        // Technically is not necessary, because the other side should
        // never expect an error.
        self.error = None;

        Poll::Ready(result)
    }
}

pub(crate) fn split(
    handle: tokio::task::JoinHandle<()>,
) -> (ServiceJoinHandleNoError, ServiceJoinHandle) {
    let (tx, rx) = tokio::sync::oneshot::channel();

    let handle1 = handle.abort_handle();
    let handle2 = handle.abort_handle();

    let shared = MapJoinResult(handle).shared();

    (
        ServiceJoinHandleNoError {
            error: Some(tx),
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

struct MapJoinResult(JoinHandle<()>);

impl Future for MapJoinResult {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = futures::ready!(self.0.poll_unpin(cx));
        Poll::Ready(result.map_err(JoinError::Error))
    }
}

enum JoinError {
    Error(tokio::task::JoinError),
    ErrorGone { is_cancelled: bool, is_panic: bool },
}

impl Clone for JoinError {
    fn clone(&self) -> Self {
        match self {
            Self::Error(err) => Self::ErrorGone {
                is_cancelled: err.is_cancelled(),
                is_panic: err.is_panic(),
            },
            &Self::ErrorGone {
                is_cancelled,
                is_panic,
            } => Self::ErrorGone {
                is_cancelled,
                is_panic,
            },
        }
    }
}
