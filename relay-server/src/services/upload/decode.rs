use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder, ZstdDecoder};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

use super::HttpCompression;

/// Decompresses an upload body, rejecting trailing bytes and propagating source errors.
pub(super) enum Decoder<R> {
    Deflate(Box<ZlibDecoder<R>>),
    Gzip(Box<GzipDecoder<R>>),
    Br(Box<BrotliDecoder<R>>),
    Zstd(Box<ZstdDecoder<R>>),
}

impl<R: AsyncBufRead> Decoder<R> {
    pub(super) fn new(compression: HttpCompression, reader: R) -> Self {
        match compression {
            // HTTP deflate uses the zlib wrapper, matching Tower's decoder.
            HttpCompression::Deflate => Self::Deflate(Box::new(ZlibDecoder::new(reader))),
            HttpCompression::Gzip => Self::Gzip(Box::new(GzipDecoder::new(reader))),
            HttpCompression::Br => Self::Br(Box::new(BrotliDecoder::new(reader))),
            HttpCompression::Zstd => {
                let mut decoder = ZstdDecoder::new(reader);
                // Match Tower's support for concatenated zstd frames.
                decoder.multiple_members(true);
                Self::Zstd(Box::new(decoder))
            }
        }
    }
}

impl<R: AsyncBufRead + Unpin> AsyncRead for Decoder<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let filled = buf.filled().len();
        let (result, input) = match self.get_mut() {
            Self::Deflate(decoder) => (
                Pin::new(decoder.as_mut()).poll_read(cx, buf),
                decoder.get_mut(),
            ),
            Self::Gzip(decoder) => (
                Pin::new(decoder.as_mut()).poll_read(cx, buf),
                decoder.get_mut(),
            ),
            Self::Br(decoder) => (
                Pin::new(decoder.as_mut()).poll_read(cx, buf),
                decoder.get_mut(),
            ),
            Self::Zstd(decoder) => (
                Pin::new(decoder.as_mut()).poll_read(cx, buf),
                decoder.get_mut(),
            ),
        };
        ready!(result)?;
        if buf.filled().len() != filled {
            return Poll::Ready(Ok(()));
        }

        // Check the codec's input buffer and source before reporting EOF so trailing bytes
        // and late source errors cannot be ignored.
        if !ready!(Pin::new(input).poll_fill_buf(cx))?.is_empty() {
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
        for compression in [
            HttpCompression::Deflate,
            HttpCompression::Gzip,
            HttpCompression::Br,
            HttpCompression::Zstd,
        ] {
            let input = futures::stream::poll_fn(|_| -> Poll<Option<io::Result<bytes::Bytes>>> {
                panic!("an empty read must not poll the input");
            });
            let mut decoder = Decoder::new(compression, tokio_util::io::StreamReader::new(input));
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            let mut buf = ReadBuf::new(&mut []);

            assert!(matches!(
                Pin::new(&mut decoder).poll_read(&mut cx, &mut buf),
                Poll::Ready(Ok(()))
            ));
        }
    }
}
