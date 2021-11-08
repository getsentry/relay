use std::fmt;
use std::io::{self, Write};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use actix_web::dev::Payload;
use actix_web::error::PayloadError;
use actix_web::http::header::CONTENT_ENCODING;
use actix_web::{FromRequest, HttpMessage, HttpRequest};
use brotli2::write::BrotliDecoder;
use bytes::Bytes;
use flate2::write::{DeflateDecoder, GzDecoder};
use futures::{Async, Poll, Stream};
use relay_config::HttpEncoding;

/// Start size for the [`Decoder`]'s internal target buffer.
///
/// The allocated buffer will be smaller if the `Decoder`'s limit is set to a lower value. Likewise,
/// the buffer grows dynamically up to the limit as large payloads are being decompressed.
const DECODE_BUFFER_SIZE: usize = 8192;

/// A shared reference to an actix request payload.
///
/// The reference to the stream can be cloned and be polled repeatedly, but it is not thread-safe.
/// When the payload is exhaused, it no longer returns any data from.
///
/// To obtain a reference, call [`SharedPayload::get`]. The first time, this takes the body out of
/// the request. Subsequent calls to `request.payload()` or `request.body()` will return empty. This
/// type also implements [`FromRequest`] for the use in actix request handlers.
#[derive(Clone, Debug)]
pub struct SharedPayload {
    inner: Payload,
    done: Rc<AtomicBool>,
}

impl SharedPayload {
    /// Extracts the shared request payload from the given request.
    pub fn get<S>(request: &HttpRequest<S>) -> Self {
        let mut extensions = request.extensions_mut();

        if let Some(payload) = extensions.get::<Self>() {
            return payload.clone();
        }

        let payload = Self {
            inner: request.payload(),
            done: Rc::new(AtomicBool::new(false)),
        };

        extensions.insert(payload.clone());
        payload
    }

    /// Puts unused data back into the payload to be consumed again.
    ///
    /// The chunk will be prepended to the stream. When called multiple times, data will be polled
    /// in reverse order from unreading.
    pub fn unread_data(&mut self, chunk: Bytes) {
        self.inner.unread_data(chunk);
    }
}

impl<S> FromRequest<S> for SharedPayload {
    type Config = ();
    type Result = Self;

    fn from_request(request: &HttpRequest<S>, _config: &Self::Config) -> Self::Result {
        Self::get(request)
    }
}

impl Stream for SharedPayload {
    type Item = <Payload as Stream>::Item;
    type Error = <Payload as Stream>::Error;

    #[inline]
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.done.load(Ordering::Relaxed) {
            return Ok(Async::Ready(None));
        }

        let poll = self.inner.poll();

        // Fuse the stream on error. Subsequent polls will never be ready
        if matches!(poll, Ok(Async::Ready(None)) | Err(_)) {
            self.done.store(true, Ordering::Relaxed);
        }

        poll
    }
}

#[derive(Debug, Default)]
/// A plain sink for chunks of binary data with a limit.
///
/// The sink will grow to the stated `limit` and then stop writing more data. When used in
/// combination with [`Write::write_all`], this will result in [`ErrorKind::WriteZero`] when the
/// sink overflows.
struct Sink {
    buffer: Vec<u8>,
    remaining: usize,
}

impl Sink {
    /// Creates a new `Sink` with the given `limit`.
    pub fn new(limit: usize) -> Self {
        Self {
            buffer: Vec::new(),
            remaining: limit,
        }
    }

    /// Returns the sink's full buffer, leaving it empty.
    pub fn take(&mut self) -> Bytes {
        std::mem::take(&mut self.buffer).into()
    }
}

impl Write for Sink {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.remaining {
            buf = &buf[..self.remaining];
        }

        if !buf.is_empty() && self.buffer.is_empty() {
            self.buffer.reserve(DECODE_BUFFER_SIZE.min(self.remaining));
        }

        self.buffer.extend_from_slice(buf);
        self.remaining -= buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Writes data into the given `write`, returning `true` on overflow.
fn write_overflowing<W: Write>(write: &mut W, slice: &[u8]) -> io::Result<bool> {
    // TODO: flush may not succeed if we're not writing..?
    match write.write_all(slice).and_then(|()| write.flush()) {
        Ok(()) => Ok(false),
        Err(e) => match e.kind() {
            io::ErrorKind::WriteZero => Ok(true),
            _ => Err(e),
        },
    }
}

/// Internal dispatch for all supported [`HttpEncoding`]s.
enum DecoderInner {
    Identity(Box<Sink>),
    Br(Box<BrotliDecoder<Sink>>),
    Gzip(Box<GzDecoder<Sink>>),
    Deflate(Box<DeflateDecoder<Sink>>),
}

/// Stateful decoder for all supported [`HttpEncoding`]s.
///
/// Use [`decode`](Self::decode) to feed data into the decoder's internal buffer. To read the
/// intermediate buffer, use [`take`](Self::take). Decoding can continue afterwards, but the taken
/// payload will not be returned again.
///
/// To initialize the decoder, assign a `limit`. When the decoded buffer reaches the size limit, it
/// is truncated and an overflow is returned.
pub struct Decoder {
    inner: DecoderInner,
}

impl Decoder {
    /// Creates a new `Decoder` with the given size limit.
    ///
    /// If the request is not encoded, this decoder is a noop.
    pub fn new<S>(request: &HttpRequest<S>, limit: usize) -> Self {
        let sink = Sink::new(limit);

        let encoding = request
            .headers()
            .get(CONTENT_ENCODING)
            .and_then(|enc| enc.to_str().ok())
            .map(HttpEncoding::parse)
            .unwrap_or_default();

        let inner = match encoding {
            HttpEncoding::Identity => DecoderInner::Identity(Box::new(sink)),
            HttpEncoding::Br => DecoderInner::Br(Box::new(BrotliDecoder::new(sink))),
            HttpEncoding::Gzip => DecoderInner::Gzip(Box::new(GzDecoder::new(sink))),
            HttpEncoding::Deflate => DecoderInner::Deflate(Box::new(DeflateDecoder::new(sink))),
        };

        Self { inner }
    }

    // pub fn decode(&mut self, bytes: Bytes) -> io::Result<(Bytes, bool)> {
    //     match *self {
    //         Decoder::Identity(remaining) => {
    //             let overflow = bytes.len() > remaining;
    //             let decoded = bytes.slice_to(remaining.min(bytes.len()));
    //             Ok((decoded, overflow))
    //         }
    //     }
    // }

    /// Decodes a chunk of data.
    ///
    /// The decoded bytes are written into the Decoder's buffer, which can be obtained using
    /// [`take`](Self::take). Returns `Ok(false)` if decoding has completed successfully without an
    /// overflow. Returns `Ok(true)` if decoding has stopped prematurely due to an overflow. In this
    /// case, the buffer contains the decoded payload up to the limit. Returns `Err` if there was an
    /// error decoding.
    pub fn decode(&mut self, bytes: Bytes) -> io::Result<bool> {
        // TODO: Optimize identity?
        match &mut self.inner {
            DecoderInner::Identity(inner) => write_overflowing(inner, &bytes),
            DecoderInner::Br(inner) => write_overflowing(inner, &bytes),
            DecoderInner::Gzip(inner) => write_overflowing(inner, &bytes),
            DecoderInner::Deflate(inner) => write_overflowing(inner, &bytes),
        }
    }

    /// Returns decoded bytes from the Decoder's buffer.
    ///
    /// This can be called at any time during decoding. However, the limit stated during
    /// initialization remains in effect across the entire decoded payload.
    pub fn take(&mut self) -> Bytes {
        match &mut self.inner {
            DecoderInner::Identity(inner) => inner.take(),
            DecoderInner::Br(inner) => inner.get_mut().take(),
            DecoderInner::Gzip(inner) => inner.get_mut().take(),
            DecoderInner::Deflate(inner) => inner.get_mut().take(),
        }
    }
}

impl fmt::Debug for Decoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            DecoderInner::Identity(inner) => f.debug_tuple("Identity").field(inner).finish(),
            DecoderInner::Br(_inner) => f.debug_tuple("Br").finish(),
            DecoderInner::Gzip(inner) => f.debug_tuple("Gzip").field(inner).finish(),
            DecoderInner::Deflate(inner) => f.debug_tuple("Deflate").field(inner).finish(),
        }
    }
}

/// A payload based on [`SharedPayload`] that decompresses the request body on-the-fly.
///
/// Note that this stream is not fused.
#[derive(Debug)]
pub struct DecodingPayload {
    payload: SharedPayload,
    decoder: Decoder,
}

impl DecodingPayload {
    /// Creates a decoding payload, resolving chunks of uncompressed request payload.
    pub fn new<S>(request: &HttpRequest<S>, limit: usize) -> Self {
        Self {
            payload: SharedPayload::get(request),
            decoder: Decoder::new(request, limit),
        }
    }
}

impl Stream for DecodingPayload {
    type Item = <SharedPayload as Stream>::Item;
    type Error = <SharedPayload as Stream>::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let encoded = match self.payload.poll() {
            Ok(Async::Ready(Some(encoded))) => encoded,
            Ok(Async::Ready(None)) => return Ok(Async::Ready(None)),
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(error) => return Err(error),
        };

        if self.decoder.decode(encoded)? {
            Err(PayloadError::Overflow)
        } else {
            let chunk = self.decoder.take();
            Ok(Async::Ready(if chunk.is_empty() {
                None
            } else {
                Some(chunk)
            }))
        }
    }
}
