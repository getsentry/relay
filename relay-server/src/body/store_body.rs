use std::borrow::Cow;
use std::io::{self, ErrorKind, Read};

use actix_web::{error::PayloadError, HttpRequest};
use bytes::Bytes;
use flate2::read::ZlibDecoder;
use futures01::prelude::*;
use url::form_urlencoded;

use relay_statsd::metric;

use crate::body::RequestBody;
use crate::statsd::RelayHistograms;

/// Future that resolves to a complete store endpoint body.
pub struct StoreBody {
    inner: RequestBody,
    result: Option<Result<Bytes, PayloadError>>,
}

impl StoreBody {
    /// Create `StoreBody` for request.
    pub fn new<S>(req: &HttpRequest<S>, limit: usize) -> Self {
        Self {
            inner: RequestBody::new(req, limit),
            result: data_from_querystring(req).map(|body| decode_bytes(body.as_bytes())),
        }
    }
}

impl Future for StoreBody {
    type Item = Bytes;
    type Error = PayloadError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(result) = self.result.take() {
            return result.map(Async::Ready);
        }

        let poll = match self.inner.poll()? {
            Async::Ready(body) => {
                metric!(histogram(RelayHistograms::RequestSizeBytesRaw) = body.len() as u64);
                let decoded = decode_bytes(body)?;
                metric!(
                    histogram(RelayHistograms::RequestSizeBytesUncompressed) = decoded.len() as u64
                );
                Async::Ready(decoded)
            }
            Async::NotReady => Async::NotReady,
        };

        Ok(poll)
    }
}

fn data_from_querystring<S>(req: &HttpRequest<S>) -> Option<Cow<'_, str>> {
    if req.method() != "GET" {
        return None;
    }

    let (_, value) = form_urlencoded::parse(req.query_string().as_bytes())
        .find(|(key, _)| key == "sentry_data")?;

    Some(value)
}

fn decode_bytes<B: Into<Bytes> + AsRef<[u8]>>(body: B) -> Result<Bytes, PayloadError> {
    if body.as_ref().starts_with(b"{") {
        return Ok(body.into());
    }

    // TODO: Switch to a streaming decoder
    // see https://github.com/alicemaz/rust-base64/pull/56
    let binary_body =
        base64::decode(&body).map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
    if binary_body.starts_with(b"{") {
        return Ok(binary_body.into());
    }

    let mut decode_stream = ZlibDecoder::new(binary_body.as_slice());
    let mut bytes = vec![];
    decode_stream.read_to_end(&mut bytes)?;

    Ok(Bytes::from(bytes))
}
