///! Abstractions for dealing with either actix-web client structs or reqwest structs.
///!
///! All of it is implemented as enums because if they were traits, they'd have to be boxed to be
///! transferrable between actors. Trait objects in turn do not allow for consuming self, using
///! generic methods or referencing the Self type in return values, all of which is very useful to
///! do in builder types.
use actix_web::client::{ClientRequest, ClientRequestBuilder, ClientResponse};
use actix_web::http::{ContentEncoding, StatusCode};
use actix_web::{Binary, Error as ActixError, HttpMessage};
use futures::{future, prelude::*};
use futures03::{FutureExt, TryFutureExt};
use serde::de::DeserializeOwned;

use ::actix::prelude::*;

pub enum Request {
    Actix(ClientRequest),
    Reqwest(reqwest::Request),
}

pub enum RequestBuilder {
    Actix(ClientRequestBuilder),
    Reqwest(reqwest::RequestBuilder),
}

impl RequestBuilder {
    pub fn no_default_headers(&mut self) -> &mut Self {
        match self {
            RequestBuilder::Actix(builder) => {
                builder.no_default_headers();
            }
            RequestBuilder::Reqwest(_) => {
                // can only be set on client
            }
        }

        self
    }

    pub fn finish<E>(self) -> Result<Request, E>
    where
        E: From<reqwest::Error> + From<ActixError>,
    {
        match self {
            RequestBuilder::Actix(mut builder) => Ok(Request::Actix(builder.finish()?)),
            RequestBuilder::Reqwest(builder) => Ok(Request::Reqwest(builder.build()?)),
        }
    }

    pub fn header(&mut self, key: &str, value: &[u8]) -> &mut Self {
        match self {
            RequestBuilder::Actix(builder) => {
                builder.header(key, value);
            }
            RequestBuilder::Reqwest(builder) => take_mut::take(builder, |b| b.header(key, value)),
        }

        self
    }

    pub fn set_header(&mut self, key: &str, value: &[u8]) -> &mut Self {
        match self {
            RequestBuilder::Actix(builder) => {
                builder.set_header(key, value);
            }
            RequestBuilder::Reqwest(builder) => take_mut::take(builder, |b| b.header(key, value)),
        }

        self
    }

    pub fn body<E>(self, body: Binary) -> Result<Request, E>
    where
        E: From<reqwest::Error> + From<ActixError>,
    {
        match self {
            RequestBuilder::Actix(mut builder) => Ok(Request::Actix(builder.body(body)?)),
            RequestBuilder::Reqwest(builder) => {
                let body: reqwest::Body = match body {
                    Binary::Bytes(bytes) => bytes.to_vec().into(),
                    Binary::Slice(slice) => slice.into(),
                    Binary::SharedVec(vec) => vec.to_vec().into(),
                    Binary::SharedString(string) => string.as_bytes().to_owned().into(),
                };

                RequestBuilder::Reqwest(builder.body(body)).finish()
            }
        }
    }

    pub fn disable_decompress(&mut self) -> &mut Self {
        match self {
            RequestBuilder::Actix(builder) => {
                builder.disable_decompress();
            }
            RequestBuilder::Reqwest(_) => {
                // can only be set on client
            }
        }

        self
    }

    pub fn content_encoding(&mut self, encoding: ContentEncoding) -> &mut Self {
        match self {
            RequestBuilder::Actix(builder) => {
                builder.content_encoding(encoding);
            }
            RequestBuilder::Reqwest(_) => {
                // can only be set on client
            }
        }

        self
    }
}

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
                // TODO: is this necessary for reqwest?
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
                .map(|x| x.as_bytes())
                .collect(),
            Response::Reqwest(response) => response
                .headers()
                .get_all(key)
                .into_iter()
                .map(|x| x.as_bytes())
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
