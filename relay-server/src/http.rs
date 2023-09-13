//! Abstractions for dealing with HTTP clients.
//!
//! All of it is implemented as enums because if they were traits, they'd have to be boxed to be
//! transferrable between actors. Trait objects in turn do not allow for consuming self, using
//! generic methods or referencing the Self type in return values, all of which is very useful to
//! do in builder types.
//!
//! Note: This literally does what the `http` crate is supposed to do. That crate has builder
//! objects and common request objects, it's just that nobody bothers to implement the conversion
//! logic.
use std::io;

use relay_config::HttpEncoding;
use reqwest::header::{HeaderMap, HeaderValue};
pub use reqwest::StatusCode;
use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("payload too large")]
    Overflow,
    #[error("attempted to send upstream request without credentials configured")]
    NoCredentials,
    #[error("could not send request")]
    Reqwest(#[from] reqwest::Error),
    #[error("failed to stream payload")]
    Io(#[from] io::Error),
    #[error("failed to parse JSON response")]
    Json(#[from] serde_json::Error),
}

impl HttpError {
    /// Returns `true` if the error indicates a network downtime.
    pub fn is_network_error(&self) -> bool {
        match self {
            Self::Io(_) => true,
            // note: status codes are not handled here because we never call error_for_status. This
            // logic is part of upstream service.
            Self::Reqwest(error) => error.is_timeout(),
            Self::Json(_) => false,
            HttpError::Overflow => false,
            HttpError::NoCredentials => false,
        }
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
    pub fn header(mut self, key: impl AsRef<str>, value: impl AsRef<[u8]>) -> Self {
        self.builder = self.builder.header(key.as_ref(), value.as_ref());
        self
    }

    /// Add an optional header, not replacing existing ones.
    ///
    /// If the value is `Some`, the header is added. If the value is `None`, headers are not
    /// changed.
    pub fn header_opt(mut self, key: impl AsRef<str>, value: Option<impl AsRef<[u8]>>) -> Self {
        if let Some(value) = value {
            self.builder = self.builder.header(key.as_ref(), value.as_ref());
        }
        self
    }

    pub fn content_encoding(self, encoding: HttpEncoding) -> Self {
        self.header_opt("content-encoding", encoding.name())
    }

    pub fn body<B: AsRef<[u8]>>(mut self, body: B) -> Result<Request, HttpError> {
        // TODO: This clones data. Change the signature to require `Bytes` to prevent cloning.
        self.builder = self.builder.body(body.as_ref().to_vec());
        self.finish()
    }
}

pub struct Response(pub reqwest::Response);

impl Response {
    pub fn status(&self) -> StatusCode {
        self.0.status()
    }

    pub async fn consume(&mut self) -> Result<(), HttpError> {
        // Consume the request payload such that the underlying connection returns to a "clean
        // state" and can be reused by the client. This is explicitly required, see:
        // https://github.com/seanmonstar/reqwest/issues/1272#issuecomment-839813308
        while self.0.chunk().await?.is_some() {}
        Ok(())
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

    pub fn headers(&self) -> &HeaderMap<HeaderValue> {
        self.0.headers()
    }

    pub async fn bytes(self, limit: usize) -> Result<Vec<u8>, HttpError> {
        let Self(mut request) = self;

        let mut body = Vec::with_capacity(limit.min(8192));
        while let Some(chunk) = request.chunk().await? {
            if (body.len() + chunk.len()) > limit {
                return Err(HttpError::Overflow);
            }

            body.extend_from_slice(&chunk);
        }

        Ok(body)
    }

    pub async fn json<T>(self, limit: usize) -> Result<T, HttpError>
    where
        T: 'static + DeserializeOwned,
    {
        let bytes = self.bytes(limit).await?;
        serde_json::from_slice(&bytes).map_err(HttpError::Json)
    }
}
