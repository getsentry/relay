use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::FutureExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep};

use crate::statsd::RelayCounters;

/// A wrapper for [`AsyncRead`] and [`AsyncWrite`] streams with a maximum idle time.
///
/// If there is no activity in the underlying stream for the specified `timeout`
/// the [`IdleTimeout`] will abort the operation and return [`std::io::ErrorKind::TimedOut`].
pub struct IdleTimeout<T> {
    inner: T,
    timeout: Option<Duration>,
    // `Box::pin` the sleep timer, the entire connection/stream is required to be `Unpin` anyways.
    timer: Option<Pin<Box<Sleep>>>,
    is_idle: bool,
}

impl<T: Unpin> IdleTimeout<T> {
    /// Creates a new [`IdleTimeout`] with the specified timeout.
    ///
    /// A `None` timeout is equivalent to an infinite timeout.
    pub fn new(inner: T, timeout: Option<Duration>) -> Self {
        Self {
            inner,
            timeout,
            timer: None,
            is_idle: false,
        }
    }

    fn wrap_poll<F, R>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        poll_fn: F,
    ) -> Poll<std::io::Result<R>>
    where
        F: FnOnce(Pin<&mut T>, &mut Context<'_>) -> Poll<std::io::Result<R>>,
    {
        match poll_fn(Pin::new(&mut self.inner), cx) {
            Poll::Ready(ret) => {
                // Any activity on the stream resets the timeout.
                self.is_idle = false;
                Poll::Ready(ret)
            }
            Poll::Pending => {
                // No timeout configured -> nothing to do.
                let Some(timeout) = self.timeout else {
                    return Poll::Pending;
                };

                let was_idle = self.is_idle;
                self.is_idle = true;

                let timer = match &mut self.timer {
                    // No timer created and we're idle now, create a timer with the appropriate deadline.
                    entry @ None => entry.insert(Box::pin(tokio::time::sleep(timeout))),
                    Some(sleep) => {
                        // Only if we were not idle, we have to reset the schedule.
                        if !was_idle {
                            let deadline = Instant::now() + timeout;
                            sleep.as_mut().reset(deadline);
                        }
                        sleep
                    }
                };

                match timer.poll_unpin(cx) {
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

impl<T: AsyncRead + Unpin> AsyncRead for IdleTimeout<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.wrap_poll(cx, |stream, cx| stream.poll_read(cx, buf))
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for IdleTimeout<T> {
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

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use tokio::io::{AsyncReadExt, AsyncWriteExt, SimplexStream};

    use super::*;

    macro_rules! assert_timeout {
        ($duration:expr, $future:expr) => {
            if let Ok(r) = tokio::time::timeout($duration, $future).await {
                assert!(
                    false,
                    "expected {} to fail, but it returned {:?} in time",
                    stringify!($future),
                    r
                )
            }
        };
    }

    #[tokio::test(start_paused = true)]
    async fn test_read() {
        let (receiver, mut sender) = tokio::io::simplex(64);
        let mut receiver = IdleTimeout::new(receiver, Some(Duration::from_secs(3)));

        assert_timeout!(Duration::from_millis(2900), receiver.read_u8());
        assert_timeout!(Duration::from_millis(70), receiver.read_u8());
        assert_timeout!(Duration::from_millis(29), receiver.read_u8());

        sender.write_u8(1).await.unwrap();
        assert_eq!(receiver.read_u8().await.unwrap(), 1);

        // Timeout must be reset after reading.
        assert_timeout!(Duration::from_millis(2900), receiver.read_u8());
        assert_timeout!(Duration::from_millis(70), receiver.read_u8());
        assert_timeout!(Duration::from_millis(29), receiver.read_u8());

        // Only now it should fail.
        assert_eq!(
            receiver.read_u8().await.unwrap_err().kind(),
            ErrorKind::TimedOut
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_read_no_idle_time() {
        let (receiver, _sender) = tokio::io::simplex(64);
        let mut receiver = IdleTimeout::new(receiver, None);

        // A year should be enough...
        assert_timeout!(Duration::from_secs(365 * 24 * 3600), receiver.read_u8());
    }

    #[tokio::test(start_paused = true)]
    async fn test_write() {
        let (mut receiver, sender) = tokio::io::simplex(1);
        let mut sender = IdleTimeout::new(sender, Some(Duration::from_secs(3)));

        // First byte can immediately write.
        sender.write_u8(1).await.unwrap();
        // Second write, is blocked on the 1 byte sized buffer.
        assert_timeout!(Duration::from_millis(2900), sender.write_u8(2));
        assert_timeout!(Duration::from_millis(70), sender.write_u8(2));
        assert_timeout!(Duration::from_millis(29), sender.write_u8(2));

        // Consume the blocking byte and write successfully.
        assert_eq!(receiver.read_u8().await.unwrap(), 1);
        sender.write_u8(2).await.unwrap();

        // Timeout must be reset.
        assert_timeout!(Duration::from_millis(2900), sender.write_u8(3));
        assert_timeout!(Duration::from_millis(70), sender.write_u8(3));
        assert_timeout!(Duration::from_millis(29), sender.write_u8(3));

        // Only now it should fail.
        assert_eq!(
            sender.write_u8(3).await.unwrap_err().kind(),
            ErrorKind::TimedOut
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_write_no_timeout() {
        let (_receiver, sender) = tokio::io::simplex(1);
        let mut sender = IdleTimeout::new(sender, None);

        sender.write_u8(1).await.unwrap();
        // A year should be enough...
        assert_timeout!(Duration::from_secs(365 * 24 * 3600), sender.write_u8(2));
    }

    #[tokio::test(start_paused = true)]
    async fn test_read_write() {
        let stream = SimplexStream::new_unsplit(1);
        let mut stream = IdleTimeout::new(stream, Some(Duration::from_secs(3)));

        // First byte can immediately write.
        stream.write_u8(1).await.unwrap();
        // And read.
        assert_eq!(stream.read_u8().await.unwrap(), 1);

        // The buffer is empty, but we should not time out.
        assert_timeout!(Duration::from_millis(2900), stream.read_u8());
        assert_timeout!(Duration::from_millis(70), stream.read_u8());
        assert_timeout!(Duration::from_millis(29), stream.read_u8());

        // A write resets the read timer.
        stream.write_u8(2).await.unwrap();
        tokio::time::advance(Duration::from_millis(2900)).await;
        assert_eq!(stream.read_u8().await.unwrap(), 2);

        // Same for writes.
        stream.write_u8(3).await.unwrap();
        assert_timeout!(Duration::from_millis(2900), stream.write_u8(3));
        assert_timeout!(Duration::from_millis(70), stream.write_u8(3));
        assert_timeout!(Duration::from_millis(29), stream.write_u8(3));

        assert_eq!(stream.read_u8().await.unwrap(), 3);
        tokio::time::advance(Duration::from_millis(2900)).await;
        stream.write_u8(99).await.unwrap();

        // Buffer is full and no one is clearing it, this should fail.
        assert_eq!(
            stream.write_u8(0).await.unwrap_err().kind(),
            ErrorKind::TimedOut
        );

        // Make sure reads are also timing out.
        assert_eq!(stream.read_u8().await.unwrap(), 99);
        assert_eq!(
            stream.read_u8().await.unwrap_err().kind(),
            ErrorKind::TimedOut
        );
    }
}
