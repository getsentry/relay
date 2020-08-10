//! This actor can be used for sending signed requests to the upstream relay.
use std::borrow::Cow;
use std::collections::VecDeque;
use std::str;
use std::sync::Arc;

use ::actix::fut;
use ::actix::prelude::*;
use actix_web::client::{ClientRequest, ClientRequestBuilder, ClientResponse, SendRequestError};
use actix_web::error::{JsonPayloadError, PayloadError};
use actix_web::http::{header, Method, StatusCode};
use actix_web::{Error as ActixError, HttpMessage};
use failure::Fail;
use futures::{future, prelude::*};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use relay_auth::{RegisterChallenge, RegisterRequest, RegisterResponse, Registration};
use relay_common::{tryf, LogError, RetryBackoff};
use relay_config::{Config, RelayMode};
use relay_quotas::{
    DataCategories, QuotaScope, RateLimit, RateLimitScope, RateLimits, RetryAfter, Scoping,
};

use crate::utils::{self, ApiErrorResponse};
use futures::sync::{mpsc, oneshot};

#[derive(Fail, Debug)]
pub enum UpstreamRequestError {
    #[fail(display = "attempted to send request while not yet authenticated")]
    NotAuthenticated,

    #[fail(display = "attempted to send upstream request without credentials configured")]
    NoCredentials,

    #[fail(display = "could not parse json payload returned by upstream")]
    InvalidJson(#[cause] JsonPayloadError),

    #[fail(display = "could not send request to upstream")]
    SendFailed(#[cause] SendRequestError),

    #[fail(display = "failed to create upstream request: {}", _0)]
    BuildFailed(ActixError),

    #[fail(display = "failed to receive response from upstream")]
    PayloadFailed(#[cause] PayloadError),

    #[fail(display = "upstream requests rate limited")]
    RateLimited(UpstreamRateLimits),

    #[fail(display = "upstream request returned error {}", _0)]
    ResponseError(StatusCode, #[cause] ApiErrorResponse),

    #[fail(display = "channel closed")]
    ChannelClosed,
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

/// Rate limits returned by the upstream.
///
/// Upstream rate limits can come in two forms:
///  - `Retry-After` header with a generic timeout for all categories.
///  - `X-Sentry-Rate-Limits` header with fine-grained information on applied rate limits.
///
/// These limits do not carry scope information. Use `UpstreamRateLimits::scope` to attach scope
/// identifiers and return a fully populated `RateLimits` instance.
#[derive(Debug)]
pub struct UpstreamRateLimits {
    retry_after: RetryAfter,
    rate_limits: String,
}

impl UpstreamRateLimits {
    /// Creates an empty `UpstreamRateLimits` instance.
    fn new() -> Self {
        Self {
            retry_after: RetryAfter::from_secs(0),
            rate_limits: String::new(),
        }
    }

    /// Adds the `Retry-After` header to this rate limits instance.
    fn retry_after(mut self, header: Option<&str>) -> Self {
        if let Some(retry_after) = header.and_then(|s| s.parse().ok()) {
            self.retry_after = retry_after;
        }
        self
    }

    /// Adds the `X-Sentry-Rate-Limits` header to this instance.
    ///
    /// If multiple header values are given, this header should be joined. If the header is empty,
    /// an empty string should be passed.
    fn rate_limits(mut self, header: String) -> Self {
        self.rate_limits = header;
        self
    }

    /// Creates a scoped rate limit instance based on the provided `Scoping`.
    pub fn scope(self, scoping: &Scoping) -> RateLimits {
        // Try to parse the `X-Sentry-Rate-Limits` header in the most lenient way possible. If
        // anything goes wrong, skip over the invalid parts.
        let mut rate_limits = utils::parse_rate_limits(scoping, &self.rate_limits);

        // If there are no new-style rate limits in the header, fall back to the `Retry-After`
        // header. Create a default rate limit that only applies to the current data category at the
        // most specific scope (Key).
        if !rate_limits.is_limited() {
            rate_limits.add(RateLimit {
                categories: DataCategories::new(),
                scope: RateLimitScope::for_quota(scoping, QuotaScope::Key),
                reason_code: None,
                retry_after: self.retry_after,
            });
        }

        rate_limits
    }
}

/// Requests are queued and send to the HTTP connections according to their priorities
/// High priority messages are sent first and then, when no high priority message is pending,
/// low priority messages are sent. Within the same priority messages are sent FIFO.
pub enum RequestPriority {
    /// High priority, low volume messages (e.g. ProjectConfig, ProjectStates, Registration messages)
    High,
    /// Low priority, high volume messages (e.g. Events and Outcomes)
    Low,
}

/// An type implementing this trait is aware of its priority
/// This trait is implemented by messages that need to be sent via http
pub trait WithRequestPriority {
    fn priority(&self) -> RequestPriority;
}

/// UpstreamRequest objects are queued inside the Upstream actor.
/// The objects are transformed int HTTP requests, and send to upstream as HTTP connections
/// become available.
struct UpstreamRequest {
    response_sender: oneshot::Sender<Result<ClientResponse, UpstreamRequestError>>,
    method: Method,
    path: String,
    build: Box<dyn Send + FnOnce(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>>,
}

pub struct UpstreamRelay {
    // receiver queue
    pump_http_queue_notifications: mpsc::Receiver<()>,
    backoff: RetryBackoff,
    config: Arc<Config>,
    auth_state: AuthState,
    max_inflight_requests: usize,
    num_inflight_requests: usize,
    // high priority messages
    hp_messages: VecDeque<UpstreamRequest>,
    // low priority messages
    lp_messages: VecDeque<UpstreamRequest>,
}

impl UpstreamRelay {
    pub fn new(config: Arc<Config>) -> Self {
        //TODO discus size (probably shouldn't be very big, if notifications accumulate we
        // have a problem, unbounded probably a bad idea)
        let (sender, receiver) = mpsc::channel(1000);
        UpstreamRelay {
            pump_http_queue_notifications: receiver,
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            config,
            auth_state: AuthState::Unknown,
            // TODO get the real value from a config and use it with ClientConnector::limit
            max_inflight_requests: 100,
            num_inflight_requests: 0,
            hp_messages: VecDeque::new(),
            lp_messages: VecDeque::new(),
        }
    }

    fn assert_authenticated(&self) -> Result<(), UpstreamRequestError> {
        if !self.auth_state.is_authenticated() {
            Err(UpstreamRequestError::NotAuthenticated)
        } else {
            Ok(())
        }
    }
    fn send_http_request(&mut self, request: UpstreamRequest) {
        let UpstreamRequest {
            response_sender,
            method,
            path,
            build,
        } = request;

        let host_header = self
            .config
            .http_host_header()
            .unwrap_or_else(|| self.config.upstream_descriptor().host());

        let mut builder = ClientRequest::build();
        builder
            .method(method)
            .uri(self.config.upstream_descriptor().get_url(path.as_ref()))
            .set_header("Host", host_header);

        if let Some(ref credentials) = self.config.credentials() {
            builder.header("X-Sentry-Relay-Id", credentials.id.to_string());
        }

        //TODO this is work in progress
        let future = build(&mut builder)
            .map_err(|e| {
                response_sender.send(Err(UpstreamRequestError::BuildFailed(e)));
                ()
            })
            .map(|client_request| {
                self.num_inflight_requests += 1;
                client_request
                    .send()
                    // We currently use the main connection pool size limit to control how many events get
                    // sent out at once, and "queue" up the rest (queueing means that there are a lot of
                    // futures hanging around, waiting for an open connection). We need to adjust this
                    // timeout to prevent the queued events from timing out while waiting for a free
                    // connection in the pool.
                    //
                    // This may not be good enough in the long run. Right now, filling up the "request
                    // queue" means that requests unrelated to `store` (queries, proxied/forwarded requests)
                    // are blocked by store requests. Ideally, those requests would bypass this queue.
                    //
                    // Two options come to mind:
                    //   1. Have own connection pool for `store` requests
                    //   2. Buffer up/queue/synchronize events before creating the request
                    .wait_timeout(self.config.event_buffer_expiry())
                    // This is the timeout after wait + connect.
                    .timeout(self.config.http_timeout())
                    .conn_timeout(self.config.http_connection_timeout())
                    //.to_tracked()
                    //TODO send error into the channel
                    .map_err(UpstreamRequestError::SendFailed)
                    .and_then(|response| self.handle_response(response))
            });
        // TODO RaduW send to the channel on error and on resolve
        // TODO see what to do with the future (spawn it or return it to be spawned by caller)
        // TOOD this should be called from the queue emptying loop
    }

    /// Handles a response returned from the upstream.
    ///
    /// If the response indicates success via 2XX status codes, `Ok(response)` is returned. Otherwise,
    /// the response is consumed and an error is returned. Depending on the status code and details
    /// provided in the payload, one of the following errors can be returned:
    ///
    ///  1. `RateLimited` for a `429` status code.
    ///  2. `ResponseError` in all other cases.
    fn handle_response(
        &mut self,
        response: ClientResponse,
    ) -> ResponseFuture<ClientResponse, UpstreamRequestError> {
        let status = response.status();

        if status.is_success() {
            return Box::new(future::ok(response));
        }

        // At this point, we consume the ClientResponse. This means we need to consume the response
        // payload stream, regardless of the status code. Parsing the JSON body may fail, which is a
        // non-fatal failure as the upstream is not expected to always include a valid JSON response.
        let future = response.json().then(move |json_result| {
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                let headers = response.headers();
                let retry_after = headers
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok());

                let rate_limits = headers
                    .get_all(utils::RATE_LIMITS_HEADER)
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .join(", ");

                let upstream_limits = UpstreamRateLimits::new()
                    .retry_after(retry_after)
                    .rate_limits(rate_limits);
                //TODO send error into the channel
                Err(UpstreamRequestError::RateLimited(upstream_limits))
            } else {
                // Coerce the result into an empty `ApiErrorResponse` if parsing JSON did not succeed.
                let api_response = json_result.unwrap_or_default();
                Err(UpstreamRequestError::ResponseError(status, api_response))
            }
        });

        Box::new(future)
    }

    fn send_request<P, F>(
        &mut self,
        priority: RequestPriority,
        method: Method,
        path: P,
        build: F,
    ) -> ResponseFuture<ClientResponse, UpstreamRequestError>
    where
        F: 'static + Send + FnOnce(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>,
        P: AsRef<str>,
    {
        let (tx, rx) = oneshot::channel::<Result<ClientResponse, UpstreamRequestError>>();

        let request = UpstreamRequest {
            method,
            path: path.as_ref().to_owned(),
            response_sender: tx,
            build: Box::new(build),
        };

        match priority {
            RequestPriority::Low => self.lp_messages.push_front(request),
            RequestPriority::High => self.hp_messages.push_front(request),
        };

        let future = rx
            // map errors caused by the oneshot channel being closed (unlikely)
            .map_err(|_| UpstreamRequestError::ChannelClosed)
            //unwrap the result (this is how we transport the http failure through the channel)
            .and_then(|result| result);

        Box::new(future)
    }

    fn send_query<Q: UpstreamQuery>(
        &mut self,
        query: Q,
    ) -> ResponseFuture<Q::Response, UpstreamRequestError> {
        let method = query.method();
        let path = query.path();
        let priority = query.priority();

        let credentials = tryf!(self
            .config
            .credentials()
            .ok_or(UpstreamRequestError::NoCredentials));

        let (json, signature) = credentials.secret_key.pack(query);

        let max_response_size = self.config.max_api_payload_size();

        let future = self
            .send_request(priority, method, path, |builder| {
                builder
                    .header("X-Sentry-Relay-Signature", signature)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(json)
            })
            .and_then(move |r| {
                r.json()
                    .limit(max_response_size)
                    .map_err(UpstreamRequestError::InvalidJson)
            });

        Box::new(future)
    }
}

impl Actor for UpstreamRelay {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        log::info!("upstream relay started");

        self.backoff.reset();

        if self.config.relay_mode() == RelayMode::Managed {
            context.notify(Authenticate);
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("upstream relay stopped");
    }
}

struct Authenticate;

impl Message for Authenticate {
    type Result = Result<(), ()>;
}

impl Handler<Authenticate> for UpstreamRelay {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, _msg: Authenticate, _ctx: &mut Self::Context) -> Self::Result {
        let credentials = match self.config.credentials() {
            Some(x) => x,
            None => return Box::new(fut::err(())),
        };

        log::info!(
            "registering with upstream ({})",
            self.config.upstream_descriptor()
        );

        self.auth_state = AuthState::RegisterRequestChallenge;
        let request = RegisterRequest::new(&credentials.id, &credentials.public_key);

        let future = self
            .send_query(request)
            .into_actor(self)
            .and_then(|challenge, slf, _ctx| {
                log::debug!("got register challenge (token = {})", challenge.token());
                slf.auth_state = AuthState::RegisterChallengeResponse;
                let challenge_response = challenge.create_response();

                log::debug!("sending register challenge response");
                slf.send_query(challenge_response).into_actor(slf)
            })
            .map(|_, slf, _ctx| {
                log::debug!("relay successfully registered with upstream");
                slf.auth_state = AuthState::Registered;
            })
            .map_err(|err, slf, ctx| {
                log::error!("authentication encountered error: {}", LogError(&err));
                slf.auth_state = AuthState::Error;

                // Do not retry client errors including authentication failures since client errors
                // are usually permanent. This allows the upstream to reject unsupported Relays
                // without infinite retries.
                let should_retry = match err {
                    UpstreamRequestError::ResponseError(code, _) => !code.is_client_error(),
                    _ => true,
                };

                if should_retry {
                    let interval = slf.backoff.next_backoff();
                    log::debug!(
                        "scheduling authentication retry in {} seconds",
                        interval.as_secs()
                    );

                    ctx.notify_later(Authenticate, interval);
                }
            });

        Box::new(future)
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

/// Message send to drive the HttpMessage queue
struct PumpHttpMessageQueue;

impl Message for PumpHttpMessageQueue {
    type Result = ();
}

impl Handler<PumpHttpMessageQueue> for UpstreamRelay {
    type Result = ();

    fn handle(&mut self, _msg: PumpHttpMessageQueue, _ctx: &mut Self::Context) -> Self::Result {
        while self.num_inflight_requests < self.max_inflight_requests {
            if let Some(msg) = self.hp_messages.pop_back() {
                self.send_http_request(msg);
            } else if let Some(msg) = self.lp_messages.pop_back() {
                self.send_http_request(msg);
            }
        }
    }
}

pub trait RequestBuilder: 'static {
    fn build_request(self, _: &mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>;
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

pub struct SendRequest<B = ()> {
    method: Method,
    path: String,
    builder: B,
}

impl SendRequest {
    pub fn new<S: Into<String>>(method: Method, path: S) -> Self {
        SendRequest {
            method,
            path: path.into(),
            builder: (),
        }
    }

    pub fn post<S: Into<String>>(path: S) -> Self {
        Self::new(Method::POST, path)
    }
}

impl<B> SendRequest<B> {
    pub fn build<F>(self, callback: F) -> SendRequest<F>
    where
        F: FnOnce(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError> + 'static,
    {
        SendRequest {
            method: self.method,
            path: self.path,
            builder: callback,
        }
    }
}

impl<B> Message for SendRequest<B> {
    type Result = Result<(), UpstreamRequestError>;
}

impl<B> Handler<SendRequest<B>> for UpstreamRelay
where
    B: RequestBuilder + Send,
{
    type Result = ResponseFuture<(), UpstreamRequestError>;

    fn handle(&mut self, message: SendRequest<B>, ctx: &mut Self::Context) -> Self::Result {
        let SendRequest {
            method,
            path,
            builder,
        } = message;

        let ret_val = Box::new(
            self.send_request(RequestPriority::Low, method, path, |b| {
                builder.build_request(b)
            })
            .from_err()
            .and_then(|_| Ok(())),
        );

        ctx.notify(PumpHttpMessageQueue);
        ret_val
    }
}

/// Stream handler for the mpsc tracked future stream
impl StreamHandler<(), ()> for UpstreamRelay {
    /// handle notifications received from the tracked future stream
    fn handle(&mut self, item: (), ctx: &mut Self::Context) {
        // an HTTP request has finished ... pump the message queue
        ctx.notify(PumpHttpMessageQueue)
    }
}

pub trait UpstreamQuery: Serialize + WithRequestPriority {
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

    fn handle(&mut self, message: SendQuery<T>, ctx: &mut Self::Context) -> Self::Result {
        tryf!(self.assert_authenticated());
        let ret_val = self.send_query(message.0);
        ctx.notify(PumpHttpMessageQueue);
        ret_val
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

impl WithRequestPriority for RegisterRequest {
    fn priority(&self) -> RequestPriority {
        RequestPriority::High
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
impl WithRequestPriority for RegisterResponse {
    fn priority(&self) -> RequestPriority {
        RequestPriority::High
    }
}
