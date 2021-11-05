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

#[derive(Clone, Copy, Debug)]
enum ContentEncoding {
    Identity,
    Br,
    Gzip,
    Deflate,
}

impl ContentEncoding {
    pub fn parse(str: &str) -> Self {
        let str = str.trim();
        if str.eq_ignore_ascii_case("br") {
            Self::Br
        } else if str.eq_ignore_ascii_case("gzip") {
            Self::Gzip
        } else if str.eq_ignore_ascii_case("deflate") {
            Self::Deflate
        } else {
            Self::Identity
        }
    }

    pub fn from_request<S>(request: &HttpRequest<S>) -> Self {
        request
            .headers()
            .get(CONTENT_ENCODING)
            .and_then(|enc| enc.to_str().ok())
            .map(ContentEncoding::parse)
            .unwrap_or_default()
    }
}

impl Default for ContentEncoding {
    fn default() -> Self {
        Self::Identity
    }
}

#[derive(Debug, Default)]
// TODO: Make not pub
pub struct Sink {
    buffer: Vec<u8>,
    remaining: usize,
}

impl Sink {
    pub fn new(limit: usize) -> Self {
        Self {
            buffer: Vec::new(),
            remaining: limit,
        }
    }

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

// #[derive(Debug)]
pub enum Decoder {
    // Identity(usize),
    Identity(Box<Sink>),
    Br(Box<BrotliDecoder<Sink>>),
    Gzip(Box<GzDecoder<Sink>>),
    Deflate(Box<DeflateDecoder<Sink>>),
}

impl Decoder {
    pub fn new<S>(request: &HttpRequest<S>, limit: usize) -> Self {
        let sink = Sink::new(limit);

        match ContentEncoding::from_request(request) {
            ContentEncoding::Identity => Self::Identity(Box::new(sink)),
            ContentEncoding::Br => Self::Br(Box::new(BrotliDecoder::new(sink))),
            ContentEncoding::Gzip => Self::Gzip(Box::new(GzDecoder::new(sink))),
            ContentEncoding::Deflate => Self::Deflate(Box::new(DeflateDecoder::new(sink))),
        }
    }

    // pub fn decode(&mut self, bytes: Bytes) -> io::Result<(Bytes, bool)> {
    //     match *self {
    //         Decoder::Identity(remaining) => {
    //             let overflow = bytes.len() > remaining;
    //             let decoded = bytes.slice_to(remaining.min(bytes.len()));
    //             Ok((decoded, overflow))
    //         }
    //         Decoder::Br(ref mut inner) => {
    //             let overflow = write_overflowing(inner, &bytes)?;
    //             Ok((inner.get_mut().take(), overflow))
    //         }
    //         Decoder::Gzip(ref mut inner) => {
    //             let overflow = write_overflowing(inner, &bytes)?;
    //             Ok((inner.get_mut().take(), overflow))
    //         }
    //         Decoder::Deflate(ref mut inner) => {
    //             let overflow = write_overflowing(inner, &bytes)?;
    //             Ok((inner.get_mut().take(), overflow))
    //         }
    //     }
    // }

    pub fn decode(&mut self, bytes: Bytes) -> io::Result<bool> {
        // TODO: Optimize identity?
        match self {
            Decoder::Identity(inner) => write_overflowing(inner, &bytes),
            Decoder::Br(inner) => write_overflowing(inner, &bytes),
            Decoder::Gzip(inner) => write_overflowing(inner, &bytes),
            Decoder::Deflate(inner) => write_overflowing(inner, &bytes),
        }
    }

    pub fn take(&mut self) -> Bytes {
        match self {
            Decoder::Identity(inner) => inner.take(),
            Decoder::Br(inner) => inner.get_mut().take(),
            Decoder::Gzip(inner) => inner.get_mut().take(),
            Decoder::Deflate(inner) => inner.get_mut().take(),
        }
    }
}

impl fmt::Debug for Decoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(inner) => f.debug_tuple("Identity").field(inner).finish(),
            Self::Br(_inner) => f.debug_tuple("Br").finish(),
            Self::Gzip(inner) => f.debug_tuple("Gzip").field(inner).finish(),
            Self::Deflate(inner) => f.debug_tuple("Deflate").field(inner).finish(),
        }
    }
}

// NOTE: Not fused.
#[derive(Debug)]
pub struct DecodingPayload {
    payload: SharedPayload,
    decoder: Decoder,
}

impl DecodingPayload {
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
                None // TODO: Continue looping??
            } else {
                Some(chunk)
            }))
        }
    }
}
