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
use actix_web::http::Method;
use actix_web::Binary;
use actix_web::Body;
use actix_web::HttpMessage;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use http::header;

use futures::Future;

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

    #[fail(display = "failed to send request to upstream")]
    Other(actix_web::Error),
}

impl From<actix_web::Error> for SendRequestError {
    fn from(err: actix_web::Error) -> SendRequestError {
        SendRequestError::Other(err)
    }
}

pub trait UpstreamRequest: Serialize {
    type Response: DeserializeOwned + 'static + Send;

    fn get_upstream_request_target(&self) -> (Method, Cow<str>);
}

pub struct SendRequest<T: UpstreamRequest>(pub T);

impl<T: UpstreamRequest> Message for SendRequest<T> {
    type Result = Result<<T as UpstreamRequest>::Response, SendRequestError>;
}

impl<T: UpstreamRequest> Handler<SendRequest<T>> for UpstreamRelay {
    type Result = ResponseFuture<<T as UpstreamRequest>::Response, SendRequestError>;
    fn handle(&mut self, msg: SendRequest<T>, _ctx: &mut Context<Self>) -> Self::Result {
        let credentials = tryf!(self.assert_authenticated());
        let (method, url) = {
            let (method, path) = msg.0.get_upstream_request_target();
            (method, self.upstream.get_url(&path))
        };
        let (json, signature) = credentials.secret_key.pack(msg.0);

        let request = tryf!(
            ClientRequest::build()
                .method(method)
                .uri(url)
                .header("X-Sentry-Relay-Id", format!("{}", credentials.id.simple()))
                .header("X-Sentry-Relay-Signature", signature)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_LENGTH, format!("{}", json.len()))
                .body(Body::Binary(Binary::Bytes(json.into())))
        );

        Box::new(
            request
                .send()
                .map_err(|e| actix_web::Error::from(e).into())
                .and_then(|response| {
                    response
                        .json()
                        .map_err(|e| actix_web::Error::from(e).into())
                }),
        )
    }
}
