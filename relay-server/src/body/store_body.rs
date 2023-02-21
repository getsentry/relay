use std::borrow::Cow;
use std::io::{self, ErrorKind, Read};

use actix_web::{error::PayloadError, HttpRequest};
use bytes::Bytes;
use data_encoding::BASE64;
use flate2::read::ZlibDecoder;
use url::form_urlencoded;

use relay_statsd::metric;

use crate::body;
use crate::statsd::RelayHistograms;

/// Reads the body of a store request.
///
/// In addition to [`request_body`](crate::body::request_body), this also supports two additional
/// modes of sending encoded payloads:
///
///  - In query parameters of the HTTP request.
///  - As base64-encoded zlib compression without additional HTTP headers.
///
/// If the body exceeds the given `limit` during streaming or decompression, an error is returned.
pub async fn store_body<S>(req: &HttpRequest<S>, limit: usize) -> Result<Bytes, PayloadError> {
    if let Some(body) = data_from_querystring(req) {
        return decode_bytes(body.as_bytes(), limit);
    }

    let body = body::request_body(req, limit).await?;

    metric!(histogram(RelayHistograms::RequestSizeBytesRaw) = body.len() as u64);
    let decoded = decode_bytes(body, limit)?;
    metric!(histogram(RelayHistograms::RequestSizeBytesUncompressed) = decoded.len() as u64);

    Ok(decoded)
}

fn data_from_querystring<S>(req: &HttpRequest<S>) -> Option<Cow<'_, str>> {
    if req.method() != "GET" {
        return None;
    }

    let (_, value) = form_urlencoded::parse(req.query_string().as_bytes())
        .find(|(key, _)| key == "sentry_data")?;

    Some(value)
}

fn decode_bytes<B>(body: B, limit: usize) -> Result<Bytes, PayloadError>
where
    B: Into<Bytes> + AsRef<[u8]>,
{
    if body.as_ref().starts_with(b"{") {
        return Ok(body.into());
    }

    // TODO: Switch to a streaming decoder
    // see https://github.com/alicemaz/rust-base64/pull/56
    let binary_body = BASE64
        .decode(body.as_ref())
        .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
    if binary_body.starts_with(b"{") {
        return Ok(binary_body.into());
    }

    let mut decode_stream = ZlibDecoder::new(binary_body.as_slice()).take(limit as u64);
    let mut bytes = vec![];
    decode_stream.read_to_end(&mut bytes)?;

    Ok(Bytes::from(bytes))
}
