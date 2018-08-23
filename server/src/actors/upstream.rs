//! This actor can be used for sending signed requests to the upstream relay.
use std::borrow::Cow;
use std::str;

use actix::fut::wrap_future;
use actix::Actor;
use actix::ActorFuture;
use actix::AsyncContext;
use actix::Context;
use actix::Handler;
use actix::MailboxError;
use actix::Message;
use actix::ResponseActFuture;
use actix::ResponseFuture;

use actix_web;
use actix_web::client::ClientRequest;
use actix_web::client::SendRequestError;
use actix_web::error::JsonPayloadError;
use actix_web::http::header;
use actix_web::http::Method;
use actix_web::HttpMessage;

use chrono::Duration;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use futures::Future;
use futures::IntoFuture;

use semaphore_aorta::RegisterChallenge;
use semaphore_aorta::RegisterRequest;
use semaphore_aorta::RegisterResponse;
use semaphore_aorta::Registration;
use semaphore_aorta::UpstreamDescriptor;
use semaphore_config::Credentials;

#[derive(Fail, Debug)]
pub enum UpstreamRequestError {
    #[fail(display = "attempted to send request while not yet authenticated")]
    NotAuthenticated,

    #[fail(display = "attempted to send upstream request without credentials configured")]
    NoCredentials,

    #[fail(display = "could not schedule request to upstream")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "could not parse json payload returned by upstream")]
    InvalidJson(#[cause] JsonPayloadError),

    #[fail(display = "could not send request to upstream")]
    SendFailed(#[cause] SendRequestError),

    #[fail(display = "failed to create upstream request: {}", _0)]
    BuildFailed(actix_web::Error),
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
                warn!("No credentials configured, not authenticating.");
                return Box::new(wrap_future(Ok(()).into_future()));
            }
        };

        info!("registering with upstream (upstream = {})", self.upstream);
        self.auth_state = AuthState::RegisterRequestChallenge;

        let request = RegisterRequest::new(&credentials.id, &credentials.public_key);
        let future = wrap_future::<_, Self>(self.do_send_query(request))
            .and_then(|challenge, actor, _ctx| {
                info!("got register challenge (token = {})", challenge.token());
                actor.auth_state = AuthState::RegisterChallengeResponse;
                let challenge_response = challenge.create_response();

                info!("sending register challenge response");
                wrap_future(actor.do_send_query(challenge_response))
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

    fn do_send_query<T: UpstreamQuery>(&self, query: T) -> <Self as Handler<SendQuery<T>>>::Result {
        let method = query.method();
        let url = self.upstream.get_url(&query.path());

        let credentials = tryf!(
            self.credentials
                .as_ref()
                .ok_or(UpstreamRequestError::NoCredentials)
        );

        let (json, signature) = credentials.secret_key.pack(query);

        let request = ClientRequest::build()
            .method(method)
            .uri(url)
            .header("X-Sentry-Relay-Id", credentials.id.simple().to_string())
            .header("X-Sentry-Relay-Signature", signature)
            .header(header::CONTENT_TYPE, "application/json")
            .body(json)
            .map_err(UpstreamRequestError::BuildFailed);

        let future = tryf!(request)
            .send()
            .map_err(UpstreamRequestError::SendFailed)
            .and_then(|response| response.json().map_err(UpstreamRequestError::InvalidJson));

        Box::new(future)
    }
}

impl Actor for UpstreamRelay {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Upstream relay started");
        ctx.spawn(self.authenticate());
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Upstream relay stopped");
    }
}

pub trait UpstreamQuery: Serialize {
    type Response: DeserializeOwned + 'static + Send;

    fn method(&self) -> Method;

    fn path(&self) -> Cow<str>;
}

pub struct SendQuery<T: UpstreamQuery>(pub T);

impl<T: UpstreamQuery> Message for SendQuery<T> {
    type Result = Result<T::Response, UpstreamRequestError>;
}

impl<T: UpstreamQuery> Handler<SendQuery<T>> for UpstreamRelay {
    type Result = ResponseFuture<T::Response, UpstreamRequestError>;

    fn handle(&mut self, message: SendQuery<T>, _ctx: &mut Context<Self>) -> Self::Result {
        tryf!(self.assert_authenticated());
        self.do_send_query(message.0)
    }
}

impl UpstreamQuery for RegisterRequest {
    type Response = RegisterChallenge;

    fn method(&self) -> Method {
        Method::POST
    }
    fn path(&self) -> Cow<str> {
        Cow::Borrowed("/api/0/relays/register/challenge/")
    }
}

impl UpstreamQuery for RegisterResponse {
    type Response = Registration;

    fn method(&self) -> Method {
        Method::POST
    }
    fn path(&self) -> Cow<str> {
        Cow::Borrowed("/api/0/relays/register/response/")
    }
}

pub struct IsAuthenticated;

impl Message for IsAuthenticated {
    type Result = bool;
}

impl Handler<IsAuthenticated> for UpstreamRelay {
    type Result = bool;

    fn handle(&mut self, _msg: IsAuthenticated, _ctx: &mut Context<Self>) -> Self::Result {
        self.auth_state.is_authenticated()
    }
}
