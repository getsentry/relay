//! This actor can be used for sending signed requests to the upstream relay.
use std::borrow::Cow;
use std::str;

use actix::prelude::*;
use actix_web::client::{ClientRequest, ClientRequestBuilder, ClientResponse, SendRequestError};
use actix_web::http::{header, Method, StatusCode};
use actix_web::{error::JsonPayloadError, Error as ActixError, HttpMessage};
use chrono::Duration;
use futures::prelude::*;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use semaphore_common::{
    Credentials, RegisterChallenge, RegisterRequest, RegisterResponse, Registration,
    UpstreamDescriptor,
};

#[derive(Fail, Debug)]
pub enum UpstreamRequestError {
    #[fail(display = "attempted to send request while not yet authenticated")]
    NotAuthenticated,

    #[fail(display = "attempted to send upstream request without credentials configured")]
    NoCredentials,

    #[fail(display = "could not schedule request to upstream")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "could not parse json payload returned by upstream: {}", _0)]
    InvalidJson(#[cause] JsonPayloadError),

    #[fail(display = "could not send request to upstream: {}", _0)]
    SendFailed(#[cause] SendRequestError),

    #[fail(display = "failed to create upstream request: {}", _0)]
    BuildFailed(ActixError),

    #[fail(display = "upstream request returned error {}", _0)]
    ResponseError(StatusCode),
}

/// Represents the current auth state.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum AuthState {
    Unknown,
    RegisterRequestChallenge,
    RegisterChallengeResponse,
    Registered,
    Error,
}

impl AuthState {
    /// Returns true if the state is considered authenticated
    pub fn is_authenticated(self) -> bool {
        // XXX: the goal of auth state is that it also tracks auth
        // failures from queries.  Later we will need to
        // extend the states here for it.
        self == AuthState::Registered
    }
}

pub struct UpstreamRelay {
    credentials: Option<Credentials>,
    upstream: UpstreamDescriptor<'static>,
    auth_retry_interval: Duration,
    auth_state: AuthState,
}

impl UpstreamRelay {
    pub fn new(
        credentials: Option<Credentials>,
        upstream: UpstreamDescriptor<'static>,
        auth_retry_interval: Duration,
    ) -> Self {
        UpstreamRelay {
            credentials,
            upstream,
            auth_retry_interval,
            auth_state: AuthState::Unknown,
        }
    }

    fn assert_authenticated(&self) -> Result<(), UpstreamRequestError> {
        if !self.auth_state.is_authenticated() {
            Err(UpstreamRequestError::NotAuthenticated)
        } else {
            Ok(())
        }
    }

    fn authenticate(&mut self) -> ResponseActFuture<Self, (), ()> {
        let credentials = match self.credentials {
            Some(ref x) => x,
            None => {
                warn!("no credentials configured, not authenticating.");
                return Box::new(Ok(()).into_future().into_actor(self));
            }
        };

        info!("registering with upstream (upstream = {})", self.upstream);
        self.auth_state = AuthState::RegisterRequestChallenge;

        let request = RegisterRequest::new(&credentials.id, &credentials.public_key);

        let future = self
            .send_query(request)
            .into_actor(self)
            .and_then(|challenge, actor, _ctx| {
                info!("got register challenge (token = {})", challenge.token());
                actor.auth_state = AuthState::RegisterChallengeResponse;
                let challenge_response = challenge.create_response();

                info!("sending register challenge response");
                actor.send_query(challenge_response).into_actor(actor)
            })
            .map(|_registration, actor, _ctx| {
                info!("relay successfully registered with upstream");
                actor.auth_state = AuthState::Registered;
                ()
            })
            .map_err(|err, actor, ctx| {
                // XXX: do not schedule retries for fatal errors
                error!("authentication encountered error: {}", &err);
                info!(
                    "scheduling authentication retry in {} seconds",
                    actor.auth_retry_interval.num_seconds()
                );
                actor.auth_state = AuthState::Error;
                ctx.run_later(actor.auth_retry_interval.to_std().unwrap(), |actor, ctx| {
                    ctx.spawn(actor.authenticate());
                });
                ()
            });

        Box::new(future)
    }

    fn send_request<P, F>(
        &self,
        method: Method,
        path: P,
        build: F,
    ) -> ResponseFuture<ClientResponse, UpstreamRequestError>
    where
        F: FnOnce(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>,
        P: AsRef<str>,
    {
        let mut builder = ClientRequest::build();
        builder
            .method(method)
            .uri(self.upstream.get_url(path.as_ref()));

        if let Some(ref credentials) = self.credentials {
            builder.header("X-Sentry-Relay-Id", credentials.id.simple().to_string());
        }

        let request = tryf!(build(&mut builder).map_err(UpstreamRequestError::BuildFailed));
        let future = request
            .send()
            .map_err(UpstreamRequestError::SendFailed)
            .and_then(|response| match response.status() {
                code if !code.is_success() => Err(UpstreamRequestError::ResponseError(code)),
                _ => Ok(response),
            });

        Box::new(future)
    }

    fn send_query<Q: UpstreamQuery>(
        &self,
        query: Q,
    ) -> ResponseFuture<Q::Response, UpstreamRequestError> {
        let method = query.method();
        let path = query.path();

        let credentials = tryf!(
            self.credentials
                .as_ref()
                .ok_or(UpstreamRequestError::NoCredentials)
        );

        let (json, signature) = credentials.secret_key.pack(query);

        let future =
            self.send_request(method, path, |builder| {
                builder
                    .header("X-Sentry-Relay-Signature", signature)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(json)
            }).and_then(|r| r.json().map_err(UpstreamRequestError::InvalidJson));

        Box::new(future)
    }
}

impl Actor for UpstreamRelay {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("upstream relay started");
        ctx.spawn(self.authenticate());
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("upstream relay stopped");
    }
}

pub trait RequestBuilder: 'static {
    fn build_request(self, &mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>;
}

pub trait ResponseTransformer: 'static {
    type Result: 'static;

    fn transform_response(self, ClientResponse) -> Self::Result;
}

impl RequestBuilder for () {
    fn build_request(
        self,
        builder: &mut ClientRequestBuilder,
    ) -> Result<ClientRequest, ActixError> {
        builder.finish()
    }
}

impl<F> RequestBuilder for F
where
    F: FnOnce(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError> + 'static,
{
    fn build_request(
        self,
        builder: &mut ClientRequestBuilder,
    ) -> Result<ClientRequest, ActixError> {
        self(builder)
    }
}

impl ResponseTransformer for () {
    type Result = Result<(), UpstreamRequestError>;

    fn transform_response(self, _: ClientResponse) -> Self::Result {
        Ok(())
    }
}

impl<F, T: 'static> ResponseTransformer for F
where
    F: FnOnce(ClientResponse) -> T + 'static,
{
    type Result = T;

    fn transform_response(self, response: ClientResponse) -> Self::Result {
        self(response)
    }
}

pub struct SendRequest<B = (), T = ()> {
    method: Method,
    path: String,
    builder: B,
    transformer: T,
}

impl SendRequest {
    pub fn new<S: Into<String>>(method: Method, path: S) -> Self {
        SendRequest {
            method,
            path: path.into(),
            builder: Default::default(),
            transformer: Default::default(),
        }
    }

    pub fn post<S: Into<String>>(path: S) -> Self {
        Self::new(Method::POST, path)
    }
}

impl<B, T> SendRequest<B, T> {
    pub fn build<F>(self, callback: F) -> SendRequest<F, T>
    where
        F: FnOnce(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError> + 'static,
    {
        SendRequest {
            method: self.method,
            path: self.path,
            builder: callback,
            transformer: self.transformer,
        }
    }

    pub fn respond<F>(self, callback: F) -> SendRequest<B, F>
    where
        F: FnOnce(ClientResponse) -> T + 'static,
    {
        SendRequest {
            method: self.method,
            path: self.path,
            builder: self.builder,
            transformer: callback,
        }
    }
}

impl<B, R, T: 'static, E: 'static> Message for SendRequest<B, R>
where
    R: ResponseTransformer,
    R::Result: IntoFuture<Item = T, Error = E>,
{
    type Result = Result<T, E>;
}

impl<B, R, T: 'static, E: 'static> Handler<SendRequest<B, R>> for UpstreamRelay
where
    B: RequestBuilder,
    R: ResponseTransformer,
    R::Result: IntoFuture<Item = T, Error = E>,
    T: Send,
    E: From<UpstreamRequestError> + Send,
{
    type Result = ResponseFuture<T, E>;

    fn handle(&mut self, message: SendRequest<B, R>, _ctx: &mut Self::Context) -> Self::Result {
        let SendRequest {
            method,
            path,
            builder,
            transformer,
        } = message;

        Box::new(
            self.send_request(method, path, |b| builder.build_request(b))
                .from_err()
                .and_then(|r| transformer.transform_response(r)),
        )
    }
}

pub trait UpstreamQuery: Serialize {
    type Response: DeserializeOwned + 'static + Send;

    fn method(&self) -> Method;

    fn path(&self) -> Cow<'static, str>;
}

pub struct SendQuery<T: UpstreamQuery>(pub T);

impl<T: UpstreamQuery> Message for SendQuery<T> {
    type Result = Result<T::Response, UpstreamRequestError>;
}

impl<T: UpstreamQuery> Handler<SendQuery<T>> for UpstreamRelay {
    type Result = ResponseFuture<T::Response, UpstreamRequestError>;

    fn handle(&mut self, message: SendQuery<T>, _ctx: &mut Self::Context) -> Self::Result {
        tryf!(self.assert_authenticated());
        self.send_query(message.0)
    }
}

impl UpstreamQuery for RegisterRequest {
    type Response = RegisterChallenge;

    fn method(&self) -> Method {
        Method::POST
    }
    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/register/challenge/")
    }
}

impl UpstreamQuery for RegisterResponse {
    type Response = Registration;

    fn method(&self) -> Method {
        Method::POST
    }
    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/register/response/")
    }
}

pub struct IsAuthenticated;

impl Message for IsAuthenticated {
    type Result = bool;
}

impl Handler<IsAuthenticated> for UpstreamRelay {
    type Result = bool;

    fn handle(&mut self, _msg: IsAuthenticated, _ctx: &mut Self::Context) -> Self::Result {
        self.auth_state.is_authenticated()
    }
}
