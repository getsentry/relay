///! Abstractions for dealing with either actix-web client structs or reqwest structs.
///!
///! All of it is implemented as enums because if they were traits, they'd have to be boxed to be
///! transferrable between actors. Trait objects in turn do not allow for consuming self, using
///! generic methods or referencing the Self type in return values, all of which is very useful to
///! do in builder types.
///!
///! Note: This literally does what the `http` crate is supposed to do. That crate has builder
///! objects and common request objects, it's just that nobody bothers to implement the conversion
///! logic.
use std::io;
use std::io::Write;

use actix_web::error::{JsonPayloadError, PayloadError};
use actix_web::http::StatusCode;
use actix_web::{Binary, Error as ActixError};
use brotli2::write::BrotliEncoder;
use failure::Fail;
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use futures::prelude::*;
use futures03::{FutureExt, TryFutureExt, TryStreamExt};
use serde::de::DeserializeOwned;

use ::actix::prelude::*;

use relay_config::HttpEncoding;

#[derive(Fail, Debug)]
pub enum HttpError {
    #[fail(display = "payload too large")]
    Overflow,
    #[fail(display = "could not send request")]
    Reqwest(#[cause] reqwest::Error),
    #[fail(display = "failed to create upstream request: {}", _0)]
    Actix(ActixError),
    #[fail(display = "failed to stream payload: {}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "could not parse json payload returned by upstream")]
    ActixJson(#[cause] JsonPayloadError),
    #[fail(display = "failed to receive response from upstream")]
    ActixPayload(#[cause] PayloadError),
}

impl HttpError {
    /// Returns `true` if the error indicates a network downtime.
    pub fn is_network_error(&self) -> bool {
        match self {
            HttpError::ActixPayload(_) | HttpError::Io(_) => true,

            // note: status codes are not handled here because we never call error_for_status. This
            // logic is part of upstream actor.
            HttpError::Reqwest(error) => error.is_timeout(),
            _ => false,
        }
    }
}

impl From<reqwest::Error> for HttpError {
    fn from(e: reqwest::Error) -> Self {
        HttpError::Reqwest(e)
    }
}

impl From<ActixError> for HttpError {
    fn from(e: ActixError) -> Self {
        HttpError::Actix(e)
    }
}

impl From<io::Error> for HttpError {
    fn from(e: io::Error) -> Self {
        HttpError::Io(e)
    }
}

impl From<JsonPayloadError> for HttpError {
    fn from(e: JsonPayloadError) -> Self {
        HttpError::ActixJson(e)
    }
}

impl From<PayloadError> for HttpError {
    fn from(e: PayloadError) -> Self {
        HttpError::ActixPayload(e)
    }
}

pub struct Request(pub reqwest::Request);

pub struct RequestBuilder {
    builder: reqwest::RequestBuilder,

    /// The content encoding that this builder object implements on top of reqwest, which does
    /// not support request encoding at all.
    http_encoding: HttpEncoding,
}

impl RequestBuilder {
    pub fn reqwest(builder: reqwest::RequestBuilder) -> Self {
        RequestBuilder {
            builder,

            // very few endpoints can actually deal with request body content-encoding. Outside of
            // store/envelope this is almost always identity because there's no way to do content
            // negotiation on request.
            http_encoding: HttpEncoding::Identity,
        }
    }

    pub fn finish(self) -> Result<Request, HttpError> {
        Ok(Request(self.builder.build()?))
    }

    /// Add a new header, not replacing existing ones.
    pub fn header(&mut self, key: impl AsRef<str>, value: impl AsRef<[u8]>) -> &mut Self {
        take_mut::take(&mut self.builder, |b| {
            b.header(key.as_ref(), value.as_ref())
        });
        self
    }

    pub fn body(mut self, body: Binary) -> Result<Request, HttpError> {
        // actix-web's Binary is used as argument here because the type can be constructed from
        // almost anything and then the actix-web codepath is minimally affected.
        //
        // Still it's not perfect as in the identity-encoding path we have some unnecessary copying
        // that is just to get around type conflicts. We cannot use Bytes here because we have a
        // version split between actix-web's Bytes dependency and reqwest's Bytes dependency. A
        // real zero-copy abstraction over both would force us to downgrade reqwest to a version
        // that uses Bytes 0.4.
        self.builder = match self.http_encoding {
            HttpEncoding::Identity => self.builder.body(body.as_ref().to_vec()),
            HttpEncoding::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body.as_ref())?;
                self.builder
                    .header("Content-Encoding", "deflate")
                    .body(encoder.finish()?)
            }
            HttpEncoding::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body.as_ref())?;
                self.builder
                    .header("Content-Encoding", "gzip")
                    .body(encoder.finish()?)
            }
            HttpEncoding::Br => {
                let mut encoder = BrotliEncoder::new(Vec::new(), 5);
                encoder.write_all(body.as_ref())?;
                self.builder
                    .header("Content-Encoding", "br")
                    .body(encoder.finish()?)
            }
        };

        self.finish()
    }

    pub fn content_encoding(&mut self, encoding: HttpEncoding) -> &mut Self {
        self.http_encoding = encoding;
        self
    }
}

pub struct Response(pub reqwest::Response);

impl Response {
    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.0.status().as_u16()).unwrap()
    }

    pub fn json<T: 'static + DeserializeOwned>(
        self,
        limit: usize,
    ) -> Box<dyn Future<Item = T, Error = HttpError>> {
        let future = self.bytes(limit).and_then(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|e| HttpError::ActixJson(JsonPayloadError::Deserialize(e)))
        });
        Box::new(future)
    }

    pub fn consume(mut self) -> ResponseFuture<Self, HttpError> {
        // Consume the request payload such that the underlying connection returns to a
        // "clean state".
        //
        // We do not understand if this is strictly necessary for reqwest. It was ported
        // from actix-web where it was clearly necessary to un-break keepalive connections,
        // but no testcase has been written for this and we are unsure on how to reproduce
        // outside of prod. I (markus) have not found code in reqwest that would explicitly
        // deal with this.
        Box::new(
            // Note: The reqwest codepath is impossible to write with streams due to
            // borrowing issues. You *have* to use `chunk()`.
            async move {
                while self.0.chunk().await?.is_some() {}
                Ok(self)
            }
            .boxed_local()
            .compat(),
        )
    }

    pub fn get_header(&self, key: impl AsRef<str>) -> Option<&[u8]> {
        Some(self.0.headers().get(key.as_ref())?.as_bytes())
    }

    pub fn get_all_headers(&self, key: impl AsRef<str>) -> Vec<&[u8]> {
        self.0
            .headers()
            .get_all(key.as_ref())
            .into_iter()
            .map(|value| value.as_bytes())
            .collect()
    }

    pub fn clone_headers(&self) -> Vec<(String, Vec<u8>)> {
        self.0
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_owned()))
            .collect()
    }

    pub fn bytes(self, limit: usize) -> ResponseFuture<Vec<u8>, HttpError> {
        Box::new(
            self.0
                .bytes_stream()
                .map_err(HttpError::Reqwest)
                .try_fold(
                    Vec::with_capacity(8192),
                    move |mut body, chunk| async move {
                        if (body.len() + chunk.len()) > limit {
                            Err(HttpError::Overflow)
                        } else {
                            body.extend_from_slice(&chunk);
                            Ok(body)
                        }
                    },
                )
                .boxed_local()
                .compat(),
        )
    }
}
