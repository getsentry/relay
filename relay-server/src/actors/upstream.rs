//! This module implements the `UpstreamRelay` actor that can be used for sending requests to the
//! upstream relay via HTTP.
//!
//! The actor handles two main types of messages plus some internal messages
//!   * messages that use Relay authentication
//!
//!     These are requests for data originating inside Relay ( either requests for some
//!     configuration data or outcome results being passed to the upstream server)
//!
//!   * messages that do no use Relay authentication
//!
//!     These are messages for requests that originate as user sent events and use whatever
//!     authentication headers were provided by the original request.
//!
//!  * messages used internally by Relay
//!
//!    These are messages that Relay sends in order to coordinate its work and do not result
//!    directly in a HTTP message being send to the upstream server.
//!
use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::str;
use std::sync::Arc;
use std::time::Instant;

use ::actix::fut;
use ::actix::prelude::*;
use actix_web::client::{ClientRequest, ClientRequestBuilder, ClientResponse, SendRequestError};
use actix_web::error::{JsonPayloadError, PayloadError};
use actix_web::http::{header, Method, StatusCode};
use actix_web::{Error as ActixError, HttpMessage};
use failure::Fail;
use futures::{future, prelude::*, sync::oneshot};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use relay_auth::{RegisterChallenge, RegisterRequest, RegisterResponse, Registration};
use relay_common::{metric, tryf, LogError, RetryBackoff};
use relay_config::{Config, RelayMode};
use relay_quotas::{
    DataCategories, QuotaScope, RateLimit, RateLimitScope, RateLimits, RetryAfter, Scoping,
};

use crate::metrics::RelayHistograms;
use crate::utils::{self, ApiErrorResponse, IntoTracked, TrackedFutureFinished};

#[derive(Fail, Debug)]
pub enum UpstreamRequestError {
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

impl UpstreamRequestError {
    fn is_network_error(&self) -> bool {
        match self {
            Self::SendFailed(_) | Self::PayloadFailed(_) => true,
            Self::ResponseError(code, _) => matches!(code.as_u16(), 502 | 503 | 504),
            _ => false,
        }
    }
}

/// Represents the current auth state.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum AuthState {
    Unknown,
    Registered,
    Error,
}

/// Where should a request be enqueue (to the front or the back)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum EnqueuePosition {
    Front,
    Back,
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
#[derive(Debug, Clone, Copy)]
pub enum RequestPriority {
    /// Immediate request that bypasses queueing and authentication (e.g. Authentication).
    Immediate,
    /// High priority, low volume messages (e.g. ProjectConfig, ProjectStates, Registration messages).
    High,
    /// Low priority, high volume messages (e.g. Events and Outcomes).
    Low,
}

impl RequestPriority {
    fn name(&self) -> &'static str {
        match self {
            RequestPriority::Immediate => "immediate",
            RequestPriority::High => "high",
            RequestPriority::Low => "low",
        }
    }
}

impl fmt::Display for RequestPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// UpstreamRequest objects are queued inside the Upstream actor.
/// The objects are transformed int HTTP requests, and send to upstream as HTTP connections
/// become available.
struct UpstreamRequest {
    /// handle priority for the request
    priority: RequestPriority,
    /// should the request be retried in case of network error
    retry: bool,
    /// one-shot channel to be notified when the request is done (i.e. it is either
    /// successful or it is failed but we are not going to retry it)
    response_sender: oneshot::Sender<Result<ClientResponse, UpstreamRequestError>>,
    /// http method
    method: Method,
    /// request url
    path: String,
    /// request build function
    build: Box<dyn FnMut(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>>,
}

pub struct UpstreamRelay {
    backoff: RetryBackoff,
    first_error: Option<Instant>,
    config: Arc<Config>,
    auth_state: AuthState,
    max_inflight_requests: usize,
    num_inflight_requests: usize,
    // high priority request queue
    high_prio_requests: VecDeque<UpstreamRequest>,
    // low priority request queue
    low_prio_requests: VecDeque<UpstreamRequest>,
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
            Err(UpstreamRequestError::RateLimited(upstream_limits))
        } else {
            // Coerce the result into an empty `ApiErrorResponse` if parsing JSON did not succeed.
            let api_response = json_result.unwrap_or_default();
            Err(UpstreamRequestError::ResponseError(status, api_response))
        }
    });

    Box::new(future)
}

impl UpstreamRelay {
    pub fn new(config: Arc<Config>) -> Self {
        UpstreamRelay {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            max_inflight_requests: config.max_concurrent_requests(),
            auth_state: AuthState::Unknown,
            num_inflight_requests: 0,
            high_prio_requests: VecDeque::new(),
            low_prio_requests: VecDeque::new(),
            first_error: None,
            config,
        }
    }

    fn is_ready(&self) -> bool {
        match self.auth_state {
            // Relays that have auth. errors cannot send messages
            AuthState::Error => false,
            // non managed mode Relays do not authenticate (and are ready immediately)
            AuthState::Unknown => self.config.relay_mode() != RelayMode::Managed,
            // all good
            AuthState::Registered => true,
        }
    }

    fn handle_network_error(&mut self, ctx: &mut Context<Self>) {
        let now = Instant::now();
        let first_error = *self.first_error.get_or_insert(now);
        if !self.backoff.started() {
            // there is no re-authentication scheduled, schedule one now
            ctx.notify_later(Authenticate, self.backoff.next_backoff());
        }
        if first_error + self.config.http_auth_grace_period() < now {
            self.auth_state = AuthState::Error;
        }
    }

    fn send_request(&mut self, mut request: UpstreamRequest, ctx: &mut Context<Self>) {
        let host_header = self
            .config
            .http_host_header()
            .unwrap_or_else(|| self.config.upstream_descriptor().host());

        let mut builder = ClientRequest::build();
        builder
            .method(request.method.clone())
            .uri(
                self.config
                    .upstream_descriptor()
                    .get_url(request.path.as_ref()),
            )
            .set_header("Host", host_header);

        if let Some(ref credentials) = self.config.credentials() {
            builder.header("X-Sentry-Relay-Id", credentials.id.to_string());
        }

        //try to build a ClientRequest
        let client_request = match (request.build)(&mut builder) {
            Err(e) => {
                request
                    .response_sender
                    .send(Err(UpstreamRequestError::BuildFailed(e)))
                    .ok();
                return;
            }
            Ok(client_request) => client_request,
        };

        // we are about to send a HTTP message keep track of requests in flight
        self.num_inflight_requests += 1;

        client_request
            .send()
            .wait_timeout(self.config.event_buffer_expiry())
            .conn_timeout(self.config.http_connection_timeout())
            // This is the timeout after wait + connect.
            .timeout(self.config.http_timeout())
            .track(ctx.address().recipient())
            .map_err(UpstreamRequestError::SendFailed)
            .and_then(handle_response)
            .into_actor(self)
            .then(|send_result, slf, ctx| {
                slf.handle_http_response_status(request, send_result, ctx)
                    .into_actor(slf)
            })
            .spawn(ctx);
    }

    /// Checks what happened to the request and takes appropriate action
    ///
    /// - if the request was sent, notify the response sender
    /// - if the request was not send:
    ///     - if it was a network error schedule a retry
    ///     - if it was a non recoverable error, notify the response sender with the error
    fn handle_http_response_status(
        &mut self,
        request: UpstreamRequest,
        send_result: Result<ClientResponse, UpstreamRequestError>,
        ctx: &mut Context<Self>,
    ) -> impl Future<Item = (), Error = ()> {
        if let Err(err) = send_result {
            if err.is_network_error() {
                self.handle_network_error(ctx);
                if request.retry {
                    self.enqueue(request, ctx, EnqueuePosition::Back);
                    return futures::future::failed(());
                }
            }
            // we only retry network errors, forward this error and finish
            request.response_sender.send(Err(err)).ok();
        } else {
            // reset any previously failed status
            // TODO should we reset on success or on anything that is not a network error ?
            self.first_error = None;
            // success forward the result and finish
            request.response_sender.send(send_result).ok();
        }

        futures::future::ok(())
    }

    fn enqueue(
        &mut self,
        request: UpstreamRequest,
        ctx: &mut Context<Self>,
        position: EnqueuePosition,
    ) {
        let push = match position {
            EnqueuePosition::Front => VecDeque::push_front,
            EnqueuePosition::Back => VecDeque::push_back,
        };
        let queue: Option<_>;

        match request.priority {
            RequestPriority::Immediate => {
                // Immediate is special and bypasses the queue. Directly send the request and return
                // the response channel rather than waiting for `PumpHttpMessageQueue`.
                self.send_request(request, ctx);
                return;
            }
            RequestPriority::Low => {
                queue = Some(&mut self.low_prio_requests);
            }
            RequestPriority::High => {
                queue = Some(&mut self.high_prio_requests);
            }
        };

        if let Some(queue) = queue {
            let name = request.priority.name();
            push(queue, request);
            metric!(
                histogram(RelayHistograms::UpstreamMessageQueueSize) = queue.len() as u64,
                priority = name
            );
        }
    }

    fn enqueue_request<P, F>(
        &mut self,
        priority: RequestPriority,
        retry: bool,
        method: Method,
        path: P,
        build: F,
        ctx: &mut Context<Self>,
    ) -> ResponseFuture<ClientResponse, UpstreamRequestError>
    where
        F: 'static + FnMut(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>,
        P: AsRef<str>,
    {
        let (tx, rx) = oneshot::channel::<Result<ClientResponse, UpstreamRequestError>>();

        let future = rx
            // map errors caused by the oneshot channel being closed (unlikely)
            .map_err(|_| UpstreamRequestError::ChannelClosed)
            //unwrap the result (this is how we transport the http failure through the channel)
            .and_then(|result| result);

        self.enqueue(
            UpstreamRequest {
                priority,
                retry,
                method,
                path: path.as_ref().to_owned(),
                response_sender: tx,
                build: Box::new(build),
            },
            ctx,
            EnqueuePosition::Front,
        );

        Box::new(future)
    }

    fn enqueue_query<Q: UpstreamQuery>(
        &mut self,
        query: Q,
        ctx: &mut Context<Self>,
    ) -> ResponseFuture<Q::Response, UpstreamRequestError> {
        let method = query.method();
        let path = query.path();
        let priority = Q::priority();
        let retry = Q::retry();

        let credentials = tryf!(self
            .config
            .credentials()
            .ok_or(UpstreamRequestError::NoCredentials));

        let (json, signature) = credentials.secret_key.pack(query);
        let json = Arc::new(json);

        let max_response_size = self.config.max_api_payload_size();

        let future = self
            .enqueue_request(
                priority,
                retry,
                method,
                path,
                move |builder| {
                    builder
                        .header("X-Sentry-Relay-Signature", signature.as_str())
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(json.clone())
                },
                ctx,
            )
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

/// The `Authenticate` message is sent to the UpstreamRelay at Relay startup and coordinates the
/// authentication of the current Relay with the upstream server.
///
/// Any message the requires Relay authentication (i.e. SendQuery<T> messages) will be send only
/// after Relay has successfully authenticated with the upstream server (i.e. an Authenticate
/// message was successfully handled).
///
/// **Note:** Relay has retry functionality, outside this actor, that periodically sends Authenticate
/// messages until successful Authentication with the upstream server was achieved.
impl Handler<Authenticate> for UpstreamRelay {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, _msg: Authenticate, ctx: &mut Self::Context) -> Self::Result {
        let credentials = match self.config.credentials() {
            Some(x) => x,
            None => return Box::new(fut::err(())),
        };

        log::info!(
            "registering with upstream ({})",
            self.config.upstream_descriptor()
        );

        let request = RegisterRequest::new(&credentials.id, &credentials.public_key);
        let interval = self.backoff.next_backoff();

        let future = self
            .enqueue_query(request, ctx)
            .into_actor(self)
            .and_then(|challenge, slf, ctx| {
                log::debug!("got register challenge (token = {})", challenge.token());
                let challenge_response = challenge.create_response();

                log::debug!("sending register challenge response");
                slf.enqueue_query(challenge_response, ctx).into_actor(slf)
            })
            .map(|_, slf, ctx| {
                log::info!("relay successfully registered with upstream");
                slf.auth_state = AuthState::Registered;

                slf.backoff.reset();
                slf.first_error = None;

                ctx.notify_later(Authenticate, slf.config.http_auth_interval());

                // Resume sending queued requests if we suspended due to dropped authentication
                ctx.notify(PumpHttpMessageQueue);
            })
            .map_err(move |err, slf, ctx| {
                log::error!("authentication encountered error: {}", LogError(&err));

                if err.is_network_error() {
                    slf.handle_network_error(ctx);
                } else {
                    slf.auth_state = AuthState::Error;
                }

                // Do not retry client errors including authentication failures since client errors
                // are usually permanent. This allows the upstream to reject unsupported Relays
                // without infinite retries.
                let should_retry = match err {
                    UpstreamRequestError::ResponseError(code, _) => !code.is_client_error(),
                    _ => true,
                };

                if should_retry {
                    // move interval up
                    //let interval = slf.backoff.next_backoff();
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

/// The `IsAuthenticated` message is an internal Relay message that is used to query the current
/// state of authentication with the upstream sever.
///
/// Currently it is only used by the HealthCheck actor.
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

/// The `PumpHttpMessageQueue` is an internal Relay message that is used to drive the
/// HttpMessageQueue. Requests that need to be sent over http are placed on queues with
/// various priorities. At various points in time (when events are added to the queue or
/// when HTTP ClientConnector finishes dealing with an HTTP request) `PumpHttpMessageQueue`
/// messages are sent in order to take messages waiting in the queues and send them over
/// HTTP.
///
/// `PumpHttpMessageQueue` will end up sending messages over HTTP only when there are free
/// connections available.
impl Handler<PumpHttpMessageQueue> for UpstreamRelay {
    type Result = ();

    fn handle(&mut self, _msg: PumpHttpMessageQueue, ctx: &mut Self::Context) -> Self::Result {
        // Skip sending requests while not ready. As soon as the Upstream becomes ready through
        // authentication, `PumpHttpMessageQueue` will be emitted again.
        if !self.is_ready() {
            return;
        }

        while self.num_inflight_requests < self.max_inflight_requests {
            if let Some(msg) = self.high_prio_requests.pop_back() {
                self.send_request(msg, ctx);
            } else if let Some(msg) = self.low_prio_requests.pop_back() {
                self.send_request(msg, ctx);
            } else {
                break; // no more messages to send at this time stop looping
            }
        }
    }
}

pub trait RequestBuilder: 'static {
    fn build_request(&mut self, _: &mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>;
}

impl RequestBuilder for () {
    fn build_request(
        &mut self,
        builder: &mut ClientRequestBuilder,
    ) -> Result<ClientRequest, ActixError> {
        builder.finish()
    }
}

impl<F> RequestBuilder for F
where
    F: FnMut(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError> + 'static,
{
    fn build_request(
        &mut self,
        builder: &mut ClientRequestBuilder,
    ) -> Result<ClientRequest, ActixError> {
        self(builder)
    }
}

pub struct SendRequest<B = ()> {
    method: Method,
    path: String,
    builder: B,
    retry: bool,
}

impl SendRequest {
    pub fn new<S: Into<String>>(method: Method, path: S) -> Self {
        SendRequest {
            method,
            path: path.into(),
            builder: (),
            retry: true,
        }
    }

    pub fn post<S: Into<String>>(path: S) -> Self {
        Self::new(Method::POST, path)
    }
}

impl<B> SendRequest<B> {
    pub fn build<F>(self, callback: F) -> SendRequest<F>
    where
        F: FnMut(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError> + 'static,
    {
        SendRequest {
            method: self.method,
            path: self.path,
            retry: self.retry,
            builder: callback,
        }
    }
}

impl<B> Message for SendRequest<B> {
    type Result = Result<(), UpstreamRequestError>;
}

/// SendRequest<B> messages represent external messages that need to be sent to the upstream server
/// and do not use Relay authentication.
///
/// The handler adds the message to one of the message queues and tries to advance the processing
/// by sending a `PumpHttpMessageQueue`.
impl<B> Handler<SendRequest<B>> for UpstreamRelay
where
    B: RequestBuilder + Send,
{
    type Result = ResponseFuture<(), UpstreamRequestError>;

    fn handle(&mut self, message: SendRequest<B>, ctx: &mut Self::Context) -> Self::Result {
        let SendRequest {
            method,
            path,
            mut builder,
            retry,
        } = message;

        let ret_val = Box::new(
            self.enqueue_request(
                RequestPriority::Low,
                retry,
                method,
                path,
                move |b| builder.build_request(b),
                ctx,
            )
            .and_then(|client_response| {
                client_response
                    .payload()
                    .for_each(|_| Ok(()))
                    .map_err(UpstreamRequestError::PayloadFailed)
            }),
        );

        ctx.notify(PumpHttpMessageQueue);
        ret_val
    }
}

/// This handler handles messages that mark the end of an http request future.
/// The handler decrements the counter of in-flight HTTP requests (since one was just
/// finished) and tries to pump the http message queue by sending a `PumpHttpMessageQueue`
///
/// Every future representing an HTTP message sent by the ClientConnector is wrapped so that when
/// it finishes or it is dropped a message is sent back to the actor to notify it that a http connection
/// was freed.
///
/// **Note:** An alternative, simpler, implementation would have been to increment the in-flight
/// requests counter just before sending an http message and to decrement it when the future
/// representing the sent message completes (on the future .then() method).
/// While this approach would have simplified the design, no need for wrapping, no need for
/// the mpsc channel or this handler, it would have not dealt with dropped futures.
/// Weather the added complexity of this design is justified by being able to handle dropped
/// futures is not clear to me (RaduW) at this moment.
impl Handler<TrackedFutureFinished> for UpstreamRelay {
    type Result = ();
    /// handle notifications received from the tracked future stream
    fn handle(&mut self, _item: TrackedFutureFinished, ctx: &mut Self::Context) {
        // an HTTP request has finished update the inflight requests and pump the message queue
        self.num_inflight_requests -= 1;
        ctx.notify(PumpHttpMessageQueue)
    }
}

pub trait UpstreamQuery: Serialize {
    type Response: DeserializeOwned + 'static + Send;

    fn method(&self) -> Method;

    fn path(&self) -> Cow<'static, str>;

    fn priority() -> RequestPriority {
        RequestPriority::Low
    }
    fn retry() -> bool {
        false
    }
}

pub struct SendQuery<T: UpstreamQuery>(pub T);

impl<T: UpstreamQuery> Message for SendQuery<T> {
    type Result = Result<T::Response, UpstreamRequestError>;
}

/// SendQuery<T> messages represent messages that need to be sent to the upstream server
/// and use Relay authentication.
///
/// The handler ensures that Relay is authenticated with the upstream server, adds the message
/// to one of the message queues and tries to advance the processing by
/// sending a `PumpHttpMessageQueue`.
impl<T: UpstreamQuery> Handler<SendQuery<T>> for UpstreamRelay {
    type Result = ResponseFuture<T::Response, UpstreamRequestError>;

    fn handle(&mut self, message: SendQuery<T>, ctx: &mut Self::Context) -> Self::Result {
        let ret_val = self.enqueue_query(message.0, ctx);
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
    fn priority() -> RequestPriority {
        RequestPriority::Immediate
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
    fn priority() -> RequestPriority {
        RequestPriority::Immediate
    }
}
