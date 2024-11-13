use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

use crate::statsd::RelayCounters;

pin_project_lite::pin_project! {
    pub struct IdleTimeout<T> {
        #[pin]
        inner: T,
        timeout: Duration,
        #[pin]
        sleep: Option<Sleep>,
    }
}

impl<T> IdleTimeout<T> {
    pub fn new(inner: T, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            sleep: None,
        }
    }

    fn wrap_poll<F, R>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        poll_fn: F,
    ) -> Poll<std::io::Result<R>>
    where
        F: FnOnce(Pin<&mut T>, &mut Context<'_>) -> Poll<std::io::Result<R>>,
    {
        let mut this = self.project();
        match poll_fn(this.inner, cx) {
            Poll::Ready(ret) => {
                // Any activity on the stream resets the timeout.
                this.sleep.set(None);
                Poll::Ready(ret)
            }
            Poll::Pending => {
                // No activity on the stream, start the idle timeout.
                if this.sleep.is_none() {
                    this.sleep.set(Some(tokio::time::sleep(*this.timeout)));
                }

                let sleep = this.sleep.as_pin_mut().expect("sleep timer was just set");
                match sleep.poll(cx) {
                    Poll::Ready(_) => {
                        relay_log::trace!("closing idle server connection");
                        relay_statsd::metric!(
                            counter(RelayCounters::ServerConnectionIdleTimeout) += 1
                        );
                        Poll::Ready(Err(std::io::ErrorKind::TimedOut.into()))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl<T> AsyncRead for IdleTimeout<T>
where
    T: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.wrap_poll(cx, |stream, cx| stream.poll_read(cx, buf))
    }
}

impl<T> AsyncWrite for IdleTimeout<T>
where
    T: AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.wrap_poll(cx, |stream, cx| stream.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        self.wrap_poll(cx, |stream, cx| stream.poll_flush(cx))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.wrap_poll(cx, |stream, cx| stream.poll_shutdown(cx))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.wrap_poll(cx, |stream, cx| stream.poll_write_vectored(cx, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}
