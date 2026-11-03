use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder, ZstdDecoder};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

/// Validates that a decoder's encoded input ends without trailing bytes or source errors.
pub(super) struct Decoder<D>(D);

impl<D> Decoder<D> {
    pub(super) fn new(decoder: D) -> Self {
        Self(decoder)
    }
}

/// Access to the encoded input retained by a decoder.
trait DecoderInput: AsyncRead {
    type Input: AsyncBufRead + Unpin;

    fn input(&mut self) -> &mut Self::Input;
}

impl<R: AsyncBufRead + Unpin> DecoderInput for ZlibDecoder<R> {
    type Input = R;

    fn input(&mut self) -> &mut R {
        self.get_mut()
    }
}

impl<R: AsyncBufRead + Unpin> DecoderInput for GzipDecoder<R> {
    type Input = R;

    fn input(&mut self) -> &mut R {
        self.get_mut()
    }
}

impl<R: AsyncBufRead + Unpin> DecoderInput for BrotliDecoder<R> {
    type Input = R;

    fn input(&mut self) -> &mut R {
        self.get_mut()
    }
}

impl<R: AsyncBufRead + Unpin> DecoderInput for ZstdDecoder<R> {
    type Input = R;

    fn input(&mut self) -> &mut R {
        self.get_mut()
    }
}

impl<D: DecoderInput + Unpin> AsyncRead for Decoder<D> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let decoder = &mut self.get_mut().0;
        let filled = buf.filled().len();
        ready!(Pin::new(&mut *decoder).poll_read(cx, buf))?;
        if buf.filled().len() != filled {
            return Poll::Ready(Ok(()));
        }

        // Check the codec's input buffer and source before reporting EOF so trailing bytes
        // and late source errors cannot be ignored.
        if !ready!(Pin::new(decoder.input()).poll_fill_buf(cx))?.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing data after compressed upload body",
            )));
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_read_does_not_poll_input() {
        let input = futures::stream::poll_fn(|_| -> Poll<Option<io::Result<bytes::Bytes>>> {
            panic!("an empty read must not poll the input");
        });
        let mut decoder = Decoder::new(GzipDecoder::new(tokio_util::io::StreamReader::new(input)));
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        let mut buf = ReadBuf::new(&mut []);

        assert!(matches!(
            Pin::new(&mut decoder).poll_read(&mut cx, &mut buf),
            Poll::Ready(Ok(()))
        ));
    }
}
