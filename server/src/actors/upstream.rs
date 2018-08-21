//! This actor can be used for sending signed requests to the upstream relay.
use std::borrow::Cow;
use std::str;
use std::sync::Arc;

use actix::Actor;
use actix::Context;
use actix::Handler;
use actix::Message;
use actix::ResponseFuture;

use actix_web;
use actix_web::client::ClientRequest;
use actix_web::client::ClientResponse;
use actix_web::http::Method;
use actix_web::Body;
use actix_web::HttpMessage;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use http::header;

use futures::{future, Future, IntoFuture};

use semaphore_aorta::UpstreamDescriptor;
use semaphore_config::Credentials;
use semaphore_trove::AuthState;
use semaphore_trove::TroveState;

pub struct UpstreamRelay {
    credentials: Option<Credentials>,
    upstream: UpstreamDescriptor<'static>,
    // XXX: Only used for checking auth state. Should be removed once auth is done in this actor
    trove_state: Arc<TroveState>,
}

impl UpstreamRelay {
    pub fn new(
        credentials: Option<Credentials>,
        upstream: UpstreamDescriptor<'static>,
        trove_state: Arc<TroveState>,
    ) -> Self {
        UpstreamRelay {
            credentials,
            upstream,
            trove_state,
        }
    }

    fn assert_authenticated(&self) -> Result<&Credentials, SendRequestError> {
        if self.trove_state.auth_state() != AuthState::Registered {
            return Err(SendRequestError::NotAuthenticated);
        }

        self.credentials
            .as_ref()
            .ok_or(SendRequestError::NoCredentials)
    }
}

impl Actor for UpstreamRelay {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Upstream relay started");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Upstream relay stopped");
    }
}

#[derive(Fail, Debug)]
pub enum SendRequestError {
    #[fail(display = "attempted to send request while not yet authenticated")]
    NotAuthenticated,

    #[fail(display = "attempted to send upstream request without credentials configured")]
    NoCredentials,

    #[fail(display = "could not sign request with streaming body")]
    SignatureUnsupported,

    #[fail(display = "could not read json response")]
    InvalidJson(#[fail(cause)] actix_web::error::JsonPayloadError),

    #[fail(display = "failed to send request to upstream")]
    Other(actix_web::Error),
}

impl From<actix_web::Error> for SendRequestError {
    fn from(err: actix_web::Error) -> SendRequestError {
        SendRequestError::Other(err)
    }
}

impl From<actix_web::error::JsonPayloadError> for SendRequestError {
    fn from(err: actix_web::error::JsonPayloadError) -> SendRequestError {
        SendRequestError::InvalidJson(err)
    }
}

pub trait IntoClientRequest {
    type Result: FromClientResponse + 'static + Send;

    fn sign(&self) -> bool;

    fn into_client_request(
        self,
        upstream: &UpstreamDescriptor,
    ) -> Result<ClientRequest, actix_web::Error>;
}

pub trait FromClientResponse {
    type Item: Send;
    type Error: Into<SendRequestError>;
    type Future: IntoFuture<Item = Self::Item, Error = Self::Error>;

    fn from_client_response(response: ClientResponse) -> Self::Future;
}

// impl FromClientResponse for ClientResponse {
//     type Item = Self;
//     type Error = SendRequestError;
//     type Future = Result<ClientResponse, Self::Error>;

//     fn from_client_response(response: ClientResponse) -> Self::Future {
//         Ok(response)
//     }
// }

pub struct SendRequest<T>(pub T);

impl<T: IntoClientRequest> Message for SendRequest<T> {
    type Result = Result<<T::Result as FromClientResponse>::Item, SendRequestError>;
}

impl<T: IntoClientRequest> Handler<SendRequest<T>> for UpstreamRelay {
    type Result = ResponseFuture<<T::Result as FromClientResponse>::Item, SendRequestError>;

    fn handle(&mut self, message: SendRequest<T>, _context: &mut Context<Self>) -> Self::Result {
        let SendRequest(request) = message;
        let should_sign = request.sign();
        let mut client_request = tryf!(request.into_client_request(&self.upstream));

        let credentials = tryf!(self.assert_authenticated());
        client_request.headers_mut().append(
            "X-Sentry-Relay-Id",
            header::HeaderValue::from_str(&credentials.id.simple().to_string()).unwrap(),
        );

        if should_sign {
            let signature = match client_request.body() {
                Body::Binary(binary) => credentials.secret_key.sign(binary.as_ref()),
                Body::Empty => credentials.secret_key.sign(&[]),
                _ => return Box::new(future::err(SendRequestError::SignatureUnsupported)),
            };

            client_request.headers_mut().insert(
                "X-Sentry-Relay-Signature",
                header::HeaderValue::from_str(&signature).unwrap(),
            );
        }

        let future = client_request
            .send()
            .map_err(|e| SendRequestError::Other(actix_web::Error::from(e)))
            .and_then(|r| {
                T::Result::from_client_response(r)
                    .into_future()
                    .map_err(Into::into)
            });

        Box::new(future)
    }
}

pub trait UpstreamRequest: Serialize {
    type Result: DeserializeOwned + 'static + Send;

    fn path(&self) -> Cow<str>;

    fn method(&self) -> Method;
}

pub struct UpstreamResponse<T>(::std::marker::PhantomData<T>);

impl<T> FromClientResponse for UpstreamResponse<T>
where
    T: DeserializeOwned + 'static + Send,
{
    type Item = T;
    type Error = actix_web::error::JsonPayloadError;
    type Future = actix_web::dev::JsonBody<ClientResponse, T>;

    fn from_client_response(response: ClientResponse) -> Self::Future {
        response.json()
    }
}

impl<T> IntoClientRequest for T
where
    T: UpstreamRequest,
{
    type Result = UpstreamResponse<<Self as UpstreamRequest>::Result>;

    fn into_client_request(
        self,
        upstream: &UpstreamDescriptor,
    ) -> Result<ClientRequest, actix_web::Error> {
        ClientRequest::build()
            .method(self.method())
            .uri(upstream.get_url(self.path().as_ref()))
            .json(self)
    }

    fn sign(&self) -> bool {
        true
    }
}
