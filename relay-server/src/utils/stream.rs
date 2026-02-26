use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use sync_wrapper::SyncWrapper;

/// A streaming body that validates the total byte count against an expected length if provided.
///
/// Returns an error if the stream provides more bytes than `expected_length` (checked per chunk)
/// or fewer bytes than `expected_length` (checked when the stream ends).
///
/// This type is `Sync` via [`SyncWrapper`], allowing it to be sent across thread boundaries
/// as required by the upload service.
#[derive(Debug)]
pub struct CountingStream<S> {
    inner: Option<SyncWrapper<S>>,
    expected_length: Option<usize>,
    byte_counter: ByteCounter,
}

/// A shared counter that can be read after the [`CountingStream`] has been moved.
#[derive(Clone, Debug)]
pub struct ByteCounter(Arc<AtomicUsize>);

impl ByteCounter {
    fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }

    fn add(&self, n: usize) -> usize {
        return n + self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl<S> CountingStream<S> {
    /// Creates a new [`CountingStream`] wrapping the given stream with the expected total length.
    pub fn new(stream: S, expected_length: Option<usize>) -> Self {
        Self {
            inner: Some(SyncWrapper::new(stream)),
            expected_length,
            byte_counter: ByteCounter::new(),
        }
    }

    /// Returns the expected total length of the stream.
    pub fn expected_length(&self) -> Option<usize> {
        self.expected_length
    }

    /// Returns a shared handle to read the byte count after the stream is consumed.
    pub fn byte_counter(&self) -> ByteCounter {
        self.byte_counter.clone()
    }
}

impl<S, E> Stream for CountingStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
    E: Into<io::Error>,
{
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = &mut this.inner else {
            return Poll::Ready(None);
        };
        let inner = Pin::new(inner.get_mut());

        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let bytes_received = this.byte_counter.add(bytes.len());
                if let Some(expected_length) = this.expected_length
                    && bytes_received > expected_length
                {
                    this.inner = None;
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        format!(
                            "stream exceeded expected length: received {} > {}",
                            bytes_received, expected_length
                        ),
                    ))))
                } else {
                    Poll::Ready(Some(Ok(bytes)))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => {
                let bytes_received = this.byte_counter.get();
                if let Some(expected_length) = this.expected_length
                    && bytes_received < expected_length
                {
                    this.inner = None;
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "stream shorter than expected length: received {} < {}",
                            bytes_received, expected_length
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
