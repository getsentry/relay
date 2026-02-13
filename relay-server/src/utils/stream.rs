use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use sync_wrapper::SyncWrapper;

/// A streaming body that validates the total byte count against the announced length.
///
/// Returns an error if the stream provides more bytes than `expected_length` (checked per chunk)
/// or fewer bytes than `expected_length` (checked when the stream ends).
///
/// This type is `Sync` via [`SyncWrapper`], allowing it to be sent across thread boundaries
/// as required by the upload service.
pub struct ExactStream<S> {
    inner: SyncWrapper<S>,
    expected_length: usize,
    bytes_received: usize,
}

impl<S> ExactStream<S> {
    /// Creates a new `ExactStream` wrapping the given stream with the expected total length.
    pub fn new(stream: S, expected_length: usize) -> Self {
        Self {
            inner: SyncWrapper::new(stream),
            expected_length,
            bytes_received: 0,
        }
    }

    /// Returns the expected total length of the stream.
    pub fn expected_length(&self) -> usize {
        self.expected_length
    }
}

impl<S, E> Stream for ExactStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
    E: Into<io::Error>,
{
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let inner = Pin::new(this.inner.get_mut());

        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                this.bytes_received += bytes.len();
                if this.bytes_received > this.expected_length {
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        format!(
                            "stream exceeded expected length: received {} > {}",
                            this.bytes_received, this.expected_length
                        ),
                    ))))
                } else {
                    Poll::Ready(Some(Ok(bytes)))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => {
                if this.bytes_received < this.expected_length {
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "stream shorter than expected length: received {} < {}",
                            this.bytes_received, this.expected_length
                        ),
                    ))))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
