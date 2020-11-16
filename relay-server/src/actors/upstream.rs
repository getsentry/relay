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

use crate::metrics::{RelayHistograms, RelayTimers};
use crate::utils::{self, ApiErrorResponse, IntoTracked, RelayErrorAction, TrackedFutureFinished};

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

    fn is_permanent_rejection(&self) -> bool {
        if let Self::ResponseError(status_code, response) = self {
            return *status_code == StatusCode::FORBIDDEN
                && response.relay_action() == RelayErrorAction::Stop;
        }
        false
    }
}

/// Represents the current auth state.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum AuthState {
    /// Relay is not authenticated and authentication has not started.
    Unknown,

    /// Relay is not authenticated and authentication is in progress.
    Registering,

    /// The connection is healthy and authenticated in managed mode.
    Registered,

    /// Relay is authenticated and renewing the registration lease. During this process, Relay
    /// remains authenticated, unless an error occurs.
    Renewing,

    /// Authentication has been permanently denied by the Upstream. Do not attempt to retry.
    Denied,
}

impl AuthState {
    /// Returns true if the state is considered authenticated.
    pub fn is_authenticated(self) -> bool {
        matches!(self, AuthState::Registered | AuthState::Renewing)
    }
}

/// The position for enqueueing an upstream request.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum EnqueuePosition {
    Front,
    Back,
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

/// Priority of an upstream request for queueing.
///
/// Requests are queued and send to the HTTP connections according to their priorities
/// High priority messages are sent first and then, when no high priority message is pending,
/// low priority messages are sent. Within the same priority messages are sent FIFO.
#[derive(Clone, Copy, Debug)]
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

/// Upstream request objects queued inside the `Upstream` actor.
///
/// The objects are transformed int HTTP requests, and sent to upstream as HTTP connections
/// become available.
struct UpstreamRequest {
    config: UpstreamRequestConfig,
    /// One-shot channel to be notified when the request is done.
    ///
    /// The request is either successful or it has failed but we are not going to retry it.
    response_sender: oneshot::Sender<Result<ClientResponse, UpstreamRequestError>>,
    /// Http method.
    method: Method,
    /// Request URL.
    path: String,
    /// Request build function.
    build: Box<dyn FnMut(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError>>,
    /// Number of times this request was already sent
    previous_retries: u32,
    /// When the last sending attempt started
    send_start: Option<Instant>,
}
impl UpstreamRequest {
    pub fn route_name(&self) -> &'static str {
        if self.path.contains("/outcomes/") {
            "outcomes"
        } else if self.path.contains("/envelope/") {
            "envelope"
        } else if self.path.contains("/projectids/") {
            "project_ids"
        } else if self.path.contains("/projectconfigs/") {
            "project_configs"
        } else if self.path.contains("/publickeys/") {
            "public_keys"
        } else if self.path.contains("/challenge/") {
            "challenge"
        } else if self.path.contains("/response/") {
            "response"
        } else if self.path.contains("/live/") {
            "check_live"
        } else {
            "unknown"
        }
    }

    pub fn retries_bucket(&self) -> &'static str {
        match self.previous_retries {
            0 => "0",
            1 => "1",
            2 => "2",
            3..=10 => "few",
            _ => "many",
        }
    }
}

pub struct UpstreamRelay {
    /// backoff policy for the registration messages
    auth_backoff: RetryBackoff,
    auth_state: AuthState,
    /// backoff policy for the network outage message
    outage_backoff: RetryBackoff,
    /// from this instant forward we only got network errors on all our http requests
    /// (any request that is sent without causing a network error resets this back to None)
    first_error: Option<Instant>,
    max_inflight_requests: usize,
    num_inflight_requests: usize,
    high_prio_requests: VecDeque<UpstreamRequest>,
    low_prio_requests: VecDeque<UpstreamRequest>,
    config: Arc<Config>,
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
    update_rate_limits: bool,
) -> ResponseFuture<ClientResponse, UpstreamRequestError> {
    let status = response.status();

    if !update_rate_limits || status.is_success() {
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
    /// Creates a new `UpstreamRelay` instance.
    pub fn new(config: Arc<Config>) -> Self {
        UpstreamRelay {
            auth_backoff: RetryBackoff::new(config.http_max_retry_interval()),
            auth_state: AuthState::Unknown,
            outage_backoff: RetryBackoff::new(config.http_max_retry_interval()),
            max_inflight_requests: config.max_concurrent_requests(),
            num_inflight_requests: 0,
            high_prio_requests: VecDeque::new(),
            low_prio_requests: VecDeque::new(),
            first_error: None,
            config,
        }
    }

    /// Predicate, checks if a Relay performs authentication.
    fn should_authenticate(&self) -> bool {
        // only managed mode relays perform authentication
        self.config.relay_mode() == RelayMode::Managed
    }

    /// Predicate, checks if a Relay does re-authentication.
    fn should_renew_auth(&self) -> bool {
        self.renew_auth_interval().is_some()
    }

    /// Returns the interval at which this Relay should renew authentication.
    fn renew_auth_interval(&self) -> Option<std::time::Duration> {
        // only relays that authenticate also re-authenticate
        let should_renew_auth = self.should_authenticate()
            // processing relays do NOT re-authenticate
            && !self.config.processing_enabled()
            // the upstream did not ban us explicitly from trying to re-authenticate
            && self.auth_state != AuthState::Denied;

        if should_renew_auth {
            // only relays the have a configured auth-interval reauthenticate
            self.config.http_auth_interval()
        } else {
            None
        }
    }

    /// Predicate, checks if we are in an network outage situation.
    fn is_network_outage(&self) -> bool {
        self.outage_backoff.started()
    }

    /// Returns an error message if an authentication is prohibited in this state and
    /// None if it can authenticate.
    fn get_auth_state_error(&self) -> Option<&'static str> {
        if !self.should_authenticate() {
            Some("Upstream actor trying to authenticate although it is not supposed to.")
        } else if self.auth_state == AuthState::Registered && !self.should_renew_auth() {
            Some("Upstream actor trying to re-authenticate although it is not supposed to.")
        } else if self.auth_state == AuthState::Denied {
            Some("Upstream actor trying to authenticate after authentication was denied.")
        } else {
            // Ok to authenticate
            None
        }
    }

    /// Returns `true` if the connection is ready to send requests to the upstream.
    fn is_ready(&self) -> bool {
        if self.is_network_outage() {
            return false;
        }

        match self.auth_state {
            // Relays that have auth errors cannot send messages
            AuthState::Registering | AuthState::Denied => false,
            // Non-managed mode Relays do not authenticate and are ready immediately
            AuthState::Unknown => !self.should_authenticate(),
            // All good in managed mode
            AuthState::Registered | AuthState::Renewing => true,
        }
    }

    /// Called when a message to the upstream goes through without a network error.
    fn reset_network_error(&mut self) {
        self.first_error = None;
        self.outage_backoff.reset();
    }

    /// Records an occurrence of a network error.
    ///
    /// If the network errors persist throughout the http outage grace period, an outage is
    /// triggered, which results in halting all network requests and starting a reconnect loop.
    fn handle_network_error(&mut self, ctx: &mut Context<Self>) {
        let now = Instant::now();
        let first_error = *self.first_error.get_or_insert(now);

        // Only take action if we exceeded the grace period.
        if first_error + self.config.http_outage_grace_period() > now {
            return;
        }

        if !self.outage_backoff.started() {
            ctx.notify_later(CheckUpstreamConnection, self.outage_backoff.next_backoff());
        }
    }

    fn send_request(&mut self, mut request: UpstreamRequest, ctx: &mut Context<Self>) {
        let uri = self
            .config
            .upstream_descriptor()
            .get_url(request.path.as_ref());

        let host_header = self
            .config
            .http_host_header()
            .unwrap_or_else(|| self.config.upstream_descriptor().host());

        let mut builder = ClientRequest::build();
        builder
            .method(request.method.clone())
            .uri(uri)
            .set_header("Host", host_header);

        if let Some(ref credentials) = self.config.credentials() {
            builder.set_header("X-Sentry-Relay-Id", credentials.id.to_string());
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

        let update_rate_limits = request.config.update_rate_limits;

        request.send_start = Some(Instant::now());
        client_request
            .send()
            .wait_timeout(self.config.event_buffer_expiry())
            .conn_timeout(self.config.http_connection_timeout())
            // This is the timeout after wait + connect.
            .timeout(self.config.http_timeout())
            .track(ctx.address().recipient())
            .map_err(UpstreamRequestError::SendFailed)
            .and_then(move |response| handle_response(response, update_rate_limits))
            .into_actor(self)
            .then(|send_result, slf, ctx| {
                slf.handle_http_response(request, send_result, ctx);
                fut::ok(())
            })
            .spawn(ctx);
    }

    /// Adds a metric for the upstream request.
    fn meter_result(
        request: &UpstreamRequest,
        send_result: &Result<ClientResponse, UpstreamRequestError>,
    ) {
        let sc: StatusCode;
        let (status_code, result) = match send_result {
            Ok(ref client_response) => {
                sc = client_response.status();
                (sc.as_str(), "success")
            }
            Err(UpstreamRequestError::ResponseError(status_code, _)) => {
                (status_code.as_str(), "response_error")
            }
            Err(UpstreamRequestError::PayloadFailed(_)) => ("-", "payload_failed"),
            Err(UpstreamRequestError::SendFailed(_)) => ("-", "send_failed"),
            Err(UpstreamRequestError::RateLimited(_)) => ("_", "rate_limited"),
            Err(UpstreamRequestError::InvalidJson(_)) => ("_", "invalid_json"),

            Err(UpstreamRequestError::NoCredentials)
            | Err(UpstreamRequestError::ChannelClosed)
            | Err(UpstreamRequestError::BuildFailed(_)) => {
                // these are not errors caused when sending to upstream so we don't need to log anything
                log::error!("meter_result called for unsupported error");
                return;
            }
        };

        if let Some(send_start) = request.send_start {
            metric!(
                timer(RelayTimers::UpstreamRequestsDuration) = send_start.elapsed(),
                result = result,
                status_code = status_code,
                route = request.route_name(),
                retries = request.retries_bucket(),
            )
        }

        metric!(
            histogram(RelayHistograms::UpstreamRetries) = request.previous_retries.into(),
            result = result,
            status_code = status_code,
            route = request.route_name(),
        );
    }

    /// Checks the result of an upstream request and takes appropriate action.
    ///
    /// 1. If the request was sent, notify the response sender.
    /// 2. If the error is non-recoverable, notify the response sender.
    /// 3. If the request can be retried, schedule a retry.
    /// 4. Otherwise, ensure an authentication request is scheduled.
    fn handle_http_response(
        &mut self,
        mut request: UpstreamRequest,
        send_result: Result<ClientResponse, UpstreamRequestError>,
        ctx: &mut Context<Self>,
    ) {
        UpstreamRelay::meter_result(&request, &send_result);
        if matches!(send_result, Err(ref err) if err.is_network_error()) {
            self.handle_network_error(ctx);

            if request.config.retry {
                request.previous_retries += 1;
                return self.enqueue(request, ctx, EnqueuePosition::Back);
            }
        } else {
            // we managed a request without a network error, reset the first time we got a network
            // error and resume sending events.
            self.reset_network_error();
        }

        request.response_sender.send(send_result).ok();
    }

    /// Enqueues a request and ensures that the message queue advances.
    fn enqueue(
        &mut self,
        request: UpstreamRequest,
        ctx: &mut Context<Self>,
        position: EnqueuePosition,
    ) {
        let name = request.config.priority.name();
        let queue = match request.config.priority {
            // Immediate is special and bypasses the queue. Directly send the request and return
            // the response channel rather than waiting for `PumpHttpMessageQueue`.
            RequestPriority::Immediate => return self.send_request(request, ctx),
            RequestPriority::Low => &mut self.low_prio_requests,
            RequestPriority::High => &mut self.high_prio_requests,
        };

        match position {
            EnqueuePosition::Front => queue.push_front(request),
            EnqueuePosition::Back => queue.push_back(request),
        }

        metric!(
            histogram(RelayHistograms::UpstreamMessageQueueSize) = queue.len() as u64,
            priority = name
        );

        ctx.notify(PumpHttpMessageQueue);
    }

    fn enqueue_request<P, F>(
        &mut self,
        config: UpstreamRequestConfig,
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

        let request = UpstreamRequest {
            config,
            method,
            path: path.as_ref().to_owned(),
            response_sender: tx,
            build: Box::new(build),
            previous_retries: 0,
            send_start: None,
        };

        self.enqueue(request, ctx, EnqueuePosition::Front);

        let future = rx
            // map errors caused by the oneshot channel being closed (unlikely)
            .map_err(|_| UpstreamRequestError::ChannelClosed)
            // unwrap the result (this is how we transport the http failure through the channel)
            .and_then(|result| result);

        Box::new(future)
    }

    fn enqueue_query<Q: UpstreamQuery>(
        &mut self,
        query: Q,
        ctx: &mut Context<Self>,
    ) -> ResponseFuture<Q::Response, UpstreamRequestError> {
        let method = query.method();
        let path = query.path();
        let config = UpstreamRequestConfig {
            retry: Q::retry(),
            update_rate_limits: Q::update_rate_limits(),
            priority: Q::priority(),
        };

        let credentials = tryf!(self
            .config
            .credentials()
            .ok_or(UpstreamRequestError::NoCredentials));

        let (json, signature) = credentials.secret_key.pack(query);
        let json = Arc::new(json);

        let max_response_size = self.config.max_api_payload_size();

        let future = self
            .enqueue_request(
                config,
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

        self.auth_backoff.reset();
        self.outage_backoff.reset();

        if self.should_authenticate() {
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
        // detect incorrect authentication requests, if we detect them we have a programming error
        if let Some(auth_state_error) = self.get_auth_state_error() {
            log::error!("{}", auth_state_error);
            return Box::new(fut::err(()));
        }

        let credentials = match self.config.credentials() {
            Some(x) => x,
            None => return Box::new(fut::err(())),
        };

        log::info!(
            "registering with upstream ({})",
            self.config.upstream_descriptor()
        );

        self.auth_state = if self.auth_state.is_authenticated() {
            AuthState::Renewing
        } else {
            AuthState::Registering
        };

        let request = RegisterRequest::new(&credentials.id, &credentials.public_key);
        let interval = self.auth_backoff.next_backoff();

        let future = self
            .enqueue_query(request, ctx)
            .into_actor(self)
            .and_then(|challenge, slf, ctx| {
                log::debug!("got register challenge (token = {})", challenge.token());
                let challenge_response = challenge.into_response();

                log::debug!("sending register challenge response");
                slf.enqueue_query(challenge_response, ctx).into_actor(slf)
            })
            .map(|_, slf, ctx| {
                log::info!("relay successfully registered with upstream");
                slf.auth_state = AuthState::Registered;
                slf.auth_backoff.reset();

                if let Some(renew_interval) = slf.renew_auth_interval() {
                    ctx.notify_later(Authenticate, renew_interval);
                }

                // Resume sending queued requests if we suspended due to dropped authentication
                ctx.notify(PumpHttpMessageQueue);
            })
            .map_err(move |err, slf, ctx| {
                log::error!("authentication encountered error: {}", LogError(&err));

                if err.is_permanent_rejection() {
                    slf.auth_state = AuthState::Denied;
                    return;
                }

                // If the authentication request fails due to any reason other than a network error,
                // go back to `Registering` which indicates that this Relay is not authenticated.
                // Note that network errors are handled separately by the generic response handler.
                if !err.is_network_error() {
                    slf.auth_state = AuthState::Registering;
                }

                // Even on network errors, retry authentication independently.
                log::debug!(
                    "scheduling authentication retry in {} seconds",
                    interval.as_secs()
                );
                ctx.notify_later(Authenticate, interval);
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

        // we are authenticated and there is no network outage, go ahead with the messages
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

/// Checks the status of the network connection with the upstream server
struct CheckUpstreamConnection;

impl Message for CheckUpstreamConnection {
    type Result = ();
}

impl Handler<CheckUpstreamConnection> for UpstreamRelay {
    type Result = ();

    fn handle(&mut self, _msg: CheckUpstreamConnection, ctx: &mut Self::Context) -> Self::Result {
        self.enqueue_request(
            UpstreamRequestConfig {
                priority: RequestPriority::Immediate,
                retry: false,
                update_rate_limits: false,
            },
            Method::GET,
            "/api/0/relays/live/",
            ClientRequestBuilder::finish,
            ctx,
        )
        .and_then(|client_response| {
            // consume response bodies to ensure the connection remains usable.
            client_response
                .payload()
                .for_each(|_| Ok(()))
                .map_err(UpstreamRequestError::PayloadFailed)
        })
        .into_actor(self)
        .then(|result, slf, ctx| {
            if matches!(result, Err(err) if err.is_network_error()) {
                // still network error, schedule another attempt
                ctx.notify_later(CheckUpstreamConnection, slf.outage_backoff.next_backoff());
            } else {
                // resume normal messages
                ctx.notify(PumpHttpMessageQueue);
            }
            fut::ok(())
        })
        .spawn(ctx);
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

pub trait ResponseTransformer: 'static {
    type Result: 'static;

    fn transform_response(self, _: ClientResponse) -> Self::Result;
}

impl ResponseTransformer for () {
    type Result = ResponseFuture<(), UpstreamRequestError>;

    fn transform_response(self, response: ClientResponse) -> Self::Result {
        // consume response bodies to allow connection keep-alive
        let future = response
            .payload()
            .for_each(|_| Ok(()))
            .map_err(UpstreamRequestError::PayloadFailed);

        Box::new(future)
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
    config: UpstreamRequestConfig,
}

struct UpstreamRequestConfig {
    /// Queueing priority for the request.
    priority: RequestPriority,
    /// Should the request be retried in case of network error.
    retry: bool,
    /// Should 429s be honored within the upstream.
    update_rate_limits: bool,
}

impl SendRequest {
    pub fn new<S: Into<String>>(method: Method, path: S) -> Self {
        SendRequest {
            method,
            path: path.into(),
            builder: (),
            transformer: (),
            config: UpstreamRequestConfig {
                priority: RequestPriority::Low,
                retry: true,
                update_rate_limits: true,
            },
        }
    }

    pub fn post<S: Into<String>>(path: S) -> Self {
        Self::new(Method::POST, path)
    }
}

impl<B, T> SendRequest<B, T> {
    pub fn build<F>(self, callback: F) -> SendRequest<F, T>
    where
        F: FnMut(&mut ClientRequestBuilder) -> Result<ClientRequest, ActixError> + 'static,
    {
        SendRequest {
            method: self.method,
            path: self.path,
            builder: callback,
            transformer: self.transformer,
            config: self.config,
        }
    }

    #[inline]
    pub fn retry(mut self, should_retry: bool) -> Self {
        self.config.retry = should_retry;
        self
    }

    #[inline]
    pub fn update_rate_limits(mut self, should_update_rate_limits: bool) -> Self {
        self.config.update_rate_limits = should_update_rate_limits;
        self
    }

    #[allow(dead_code)]
    pub fn transform<R, F>(self, callback: F) -> SendRequest<B, F>
    where
        F: FnOnce(ClientResponse) -> R,
    {
        SendRequest {
            method: self.method,
            path: self.path,
            builder: self.builder,
            transformer: callback,
            config: self.config,
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

// impl<B> Message for SendRequest<B> {
//     type Result = Result<(), UpstreamRequestError>;
// }

/// SendRequest<B> messages represent external messages that need to be sent to the upstream server
/// and do not use Relay authentication.
///
/// The handler adds the message to one of the message queues.
impl<B, R, T: 'static, E: 'static> Handler<SendRequest<B, R>> for UpstreamRelay
where
    B: RequestBuilder + Send,
    R: ResponseTransformer,
    R::Result: IntoFuture<Item = T, Error = E>,
    T: Send,
    E: From<UpstreamRequestError> + Send,
{
    type Result = ResponseFuture<T, E>;

    fn handle(&mut self, message: SendRequest<B, R>, ctx: &mut Self::Context) -> Self::Result {
        let SendRequest {
            method,
            path,
            mut builder,
            transformer,
            config,
        } = message;

        let future = self
            .enqueue_request(config, method, path, move |b| builder.build_request(b), ctx)
            .from_err()
            .and_then(move |r| transformer.transform_response(r));

        Box::new(future)
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

    /// The HTTP method of the request.
    fn method(&self) -> Method;

    /// The path relative to the upstream.
    fn path(&self) -> Cow<'static, str>;

    /// Whether this request should retry on network errors.
    fn retry() -> bool;

    /// Whether 429s should be honored by the upstream actor.
    fn update_rate_limits() -> bool {
        true
    }

    /// The queueing priority of the request. Defaults to `Low`.
    fn priority() -> RequestPriority {
        RequestPriority::Low
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
/// to one of the message queues.
impl<T: UpstreamQuery> Handler<SendQuery<T>> for UpstreamRelay {
    type Result = ResponseFuture<T::Response, UpstreamRequestError>;

    fn handle(&mut self, message: SendQuery<T>, ctx: &mut Self::Context) -> Self::Result {
        self.enqueue_query(message.0, ctx)
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

    fn retry() -> bool {
        false
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

    fn retry() -> bool {
        false
    }
}
