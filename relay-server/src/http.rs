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

use actix_web::client::{ClientRequest, ClientRequestBuilder, ClientResponse};
use actix_web::http::{ContentEncoding, StatusCode};
use actix_web::{Binary, Error as ActixError, HttpMessage};
use brotli2::write::BrotliEncoder;
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use futures::{future, prelude::*};
use futures03::{FutureExt, TryFutureExt};
use serde::de::DeserializeOwned;

use ::actix::prelude::*;

use relay_config::HttpEncoding;

pub enum Request {
    Actix(ClientRequest),
    Reqwest(reqwest::Request),
}

pub enum RequestBuilder {
    Actix(ClientRequestBuilder),
    Reqwest {
        builder: reqwest::RequestBuilder,

        /// The content encoding that this builder object implements on top of reqwest, which does
        /// not support request encoding at all.
        http_encoding: HttpEncoding,
    },
}

impl RequestBuilder {
    pub fn reqwest(builder: reqwest::RequestBuilder) -> Self {
        RequestBuilder::Reqwest {
            builder,

            // very few endpoints can actually deal with request body content-encoding. Outside of
            // store/envelope this is almost always identity because there's no way to do content
            // negotiation on request.
            http_encoding: HttpEncoding::Identity,
        }
    }

    pub fn actix(builder: ClientRequestBuilder) -> Self {
        RequestBuilder::Actix(builder)
    }

    pub fn finish<E>(self) -> Result<Request, E>
    where
        E: From<reqwest::Error> + From<ActixError>,
    {
        match self {
            RequestBuilder::Actix(mut builder) => Ok(Request::Actix(builder.finish()?)),
            RequestBuilder::Reqwest {
                builder,
                http_encoding: _,
            } => Ok(Request::Reqwest(builder.build()?)),
        }
    }

    /// Add a new header, not replacing existing ones.
    pub fn header(&mut self, key: &str, value: &[u8]) -> &mut Self {
        match self {
            RequestBuilder::Actix(builder) => {
                builder.header(key, value);
            }
            RequestBuilder::Reqwest { builder, .. } => {
                take_mut::take(builder, |b| b.header(key, value))
            }
        }

        self
    }

    pub fn body<E>(self, body: Binary) -> Result<Request, E>
    where
        E: From<reqwest::Error> + From<ActixError> + From<io::Error>,
    {
        match self {
            RequestBuilder::Actix(mut builder) => Ok(Request::Actix(builder.body(body)?)),
            RequestBuilder::Reqwest {
                mut builder,
                http_encoding,
            } => {
                let body = match http_encoding {
                    HttpEncoding::Identity => {
                        builder = builder.header("Content-Encoding", "identity");
                        body.as_ref().to_vec()
                    }
                    HttpEncoding::Deflate => {
                        builder = builder.header("Content-Encoding", "deflate");
                        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                        encoder.write_all(body.as_ref())?;
                        encoder.finish().unwrap()
                    }
                    HttpEncoding::Gzip => {
                        builder = builder.header("Content-Encoding", "gzip");
                        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                        encoder.write_all(body.as_ref())?;
                        encoder.finish().unwrap()
                    }
                    HttpEncoding::Br => {
                        builder = builder.header("Content-Encoding", "br");
                        let mut encoder = BrotliEncoder::new(Vec::new(), 5);
                        encoder.write_all(body.as_ref())?;
                        encoder.finish().unwrap()
                    }
                };

                RequestBuilder::Reqwest {
                    builder: builder.body(body),
                    http_encoding,
                }
                .finish()
            }
        }
    }

    pub fn content_encoding(&mut self, encoding: HttpEncoding) -> &mut Self {
        match self {
            RequestBuilder::Actix(builder) => {
                let content_encoding = match encoding {
                    HttpEncoding::Identity => ContentEncoding::Identity,
                    HttpEncoding::Deflate => ContentEncoding::Deflate,
                    HttpEncoding::Gzip => ContentEncoding::Gzip,
                    HttpEncoding::Br => ContentEncoding::Br,
                };

                builder.content_encoding(content_encoding);
            }
            RequestBuilder::Reqwest {
                ref mut http_encoding,
                ..
            } => {
                *http_encoding = encoding;
            }
        }

        self
    }
}

#[allow(clippy::large_enum_variant)]
pub enum Response {
    Actix(ClientResponse),
    Reqwest(reqwest::Response),
}

impl Response {
    pub fn status(&self) -> StatusCode {
        match self {
            Response::Actix(response) => response.status(),
            Response::Reqwest(response) => {
                StatusCode::from_u16(response.status().as_u16()).unwrap()
            }
        }
    }

    pub fn json<T: 'static + DeserializeOwned, E>(
        self,
        limit: usize,
    ) -> Box<dyn Future<Item = T, Error = E>>
    where
        E: From<reqwest::Error> + From<actix_web::error::JsonPayloadError> + 'static,
    {
        // TODO: apply limit to reqwest
        match self {
            Response::Actix(response) => {
                let future = response.json().limit(limit).map_err(From::from);
                Box::new(future) as Box<dyn Future<Item = _, Error = _>>
            }
            Response::Reqwest(response) => {
                let future = response.json().boxed_local().compat().map_err(From::from);
                Box::new(future) as Box<dyn Future<Item = _, Error = _>>
            }
        }
    }

    pub fn consume<E>(self) -> ResponseFuture<Self, E>
    where
        E: From<actix_web::error::PayloadError> + 'static,
    {
        // consume response bodies to allow connection keep-alive
        match self {
            Response::Actix(ref response) => Box::new(
                response
                    .payload()
                    .for_each(|_| Ok(()))
                    .map(|_| self)
                    .map_err(From::from),
            ),
            Response::Reqwest(_) => {
                // This does not appear to be strictly necessary for reqwest to produce correct
                // behavior.
                Box::new(future::ok(self))
            }
        }
    }

    pub fn get_header(&self, key: &str) -> Option<&[u8]> {
        match self {
            Response::Actix(response) => Some(response.headers().get(key)?.as_bytes()),
            Response::Reqwest(response) => Some(response.headers().get(key)?.as_bytes()),
        }
    }

    pub fn get_all_headers(&self, key: &str) -> Vec<&[u8]> {
        match self {
            Response::Actix(response) => response
                .headers()
                .get_all(key)
                .into_iter()
                .map(|value| value.as_bytes())
                .collect(),
            Response::Reqwest(response) => response
                .headers()
                .get_all(key)
                .into_iter()
                .map(|value| value.as_bytes())
                .collect(),
        }
    }

    pub fn clone_headers(&self) -> Vec<(String, Vec<u8>)> {
        match self {
            Response::Actix(response) => response
                .headers()
                .iter()
                .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_owned()))
                .collect(),
            Response::Reqwest(response) => response
                .headers()
                .iter()
                .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_owned()))
                .collect(),
        }
    }

    pub fn bytes<E>(self, limit: usize) -> ResponseFuture<Vec<u8>, E>
    where
        E: From<actix_web::error::PayloadError> + From<reqwest::Error> + 'static,
    {
        // TODO: apply limit to reqwest
        match self {
            Response::Actix(response) => Box::new(
                response
                    .body()
                    .limit(limit)
                    .map(|x| x.to_vec())
                    .map_err(From::from),
            ) as Box<dyn Future<Item = _, Error = _>>,
            Response::Reqwest(response) => Box::new(
                response
                    .bytes()
                    .boxed_local()
                    .compat()
                    .map(|x| x.to_vec())
                    .map_err(From::from),
            ) as Box<dyn Future<Item = _, Error = _>>,
        }
    }
}
