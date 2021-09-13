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

use failure::Fail;
use futures::prelude::*;
use futures03::{FutureExt, TryFutureExt, TryStreamExt};
use serde::de::DeserializeOwned;

use relay_config::HttpEncoding;

#[doc(inline)]
pub use reqwest::StatusCode;

#[derive(Fail, Debug)]
pub enum HttpError {
    #[fail(display = "payload too large")]
    Overflow,
    #[fail(display = "could not send request")]
    Reqwest(#[cause] reqwest::Error),
    #[fail(display = "failed to stream payload")]
    Io(#[cause] io::Error),
    #[fail(display = "failed to parse JSON response")]
    Json(#[cause] serde_json::Error),
    #[fail(display = "{}", _0)]
    Custom(Box<dyn Fail>),
}

impl HttpError {
    pub fn custom(error: impl Fail) -> Self {
        Self::Custom(Box::new(error))
    }

    /// Returns `true` if the error indicates a network downtime.
    pub fn is_network_error(&self) -> bool {
        match self {
            Self::Io(_) => true,
            // note: status codes are not handled here because we never call error_for_status. This
            // logic is part of upstream actor.
            Self::Reqwest(error) => error.is_timeout(),
            Self::Json(_) => false,
            HttpError::Overflow => false,
            Self::Custom(_) => false,
        }
    }
}

impl From<reqwest::Error> for HttpError {
    fn from(e: reqwest::Error) -> Self {
        HttpError::Reqwest(e)
    }
}

impl From<io::Error> for HttpError {
    fn from(e: io::Error) -> Self {
        HttpError::Io(e)
    }
}

pub struct Request(pub reqwest::Request);

pub struct RequestBuilder {
    builder: reqwest::RequestBuilder,
}

impl RequestBuilder {
    pub fn reqwest(builder: reqwest::RequestBuilder) -> Self {
        RequestBuilder { builder }
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

    /// Add an optional header, not replacing existing ones.
    ///
    /// If the value is `Some`, the header is added. If the value is `None`, headers are not
    /// changed.
    pub fn header_opt(
        &mut self,
        key: impl AsRef<str>,
        value: Option<impl AsRef<[u8]>>,
    ) -> &mut Self {
        if let Some(value) = value {
            take_mut::take(&mut self.builder, |b| {
                b.header(key.as_ref(), value.as_ref())
            });
        }
        self
    }

    pub fn body<B: AsRef<[u8]>>(mut self, body: B) -> Result<Request, HttpError> {
        self.builder = self.builder.body(body.as_ref().to_vec());
        self.finish()
    }

    pub fn content_encoding(&mut self, encoding: HttpEncoding) -> &mut Self {
        match encoding {
            HttpEncoding::Identity => self,
            HttpEncoding::Deflate => self.header("Content-Encoding", "deflate"),
            HttpEncoding::Gzip => self.header("Content-Encoding", "gzip"),
            HttpEncoding::Br => self.header("Content-Encoding", "br"),
        }
    }
}

pub struct Response(pub reqwest::Response);

impl Response {
    pub fn status(&self) -> StatusCode {
        self.0.status()
    }

    pub fn json<T: 'static + DeserializeOwned>(
        self,
        limit: usize,
    ) -> Box<dyn Future<Item = T, Error = HttpError>> {
        let future = self
            .bytes(limit)
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(HttpError::Json));
        Box::new(future)
    }

    pub fn consume(mut self) -> Box<dyn Future<Item = Self, Error = HttpError>> {
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

    pub fn bytes(self, limit: usize) -> Box<dyn Future<Item = Vec<u8>, Error = HttpError>> {
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
