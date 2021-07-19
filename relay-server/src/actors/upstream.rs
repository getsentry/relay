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
use actix_web::http::{header, Method};
use failure::Fail;
use futures::{future, prelude::*, sync::oneshot};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use relay_auth::{RegisterChallenge, RegisterRequest, RegisterResponse, Registration};
use relay_common::{metric, tryf, RetryBackoff};
use relay_config::{Config, RelayMode};
use relay_log::{self, LogError};
use relay_quotas::{
    DataCategories, QuotaScope, RateLimit, RateLimitScope, RateLimits, RetryAfter, Scoping,
};

use crate::http::{HttpError, Request, RequestBuilder, Response, StatusCode};
use crate::metrics::{RelayHistograms, RelayTimers};
use crate::utils::{self, ApiErrorResponse, IntoTracked, RelayErrorAction, TrackedFutureFinished};

#[derive(Fail, Debug)]
pub enum UpstreamRequestError {
    #[fail(display = "attempted to send upstream request without credentials configured")]
    NoCredentials,

    /// As opposed to HTTP variant this contains all network errors.
    #[fail(display = "could not send request to upstream")]
    SendFailed(#[cause] reqwest::Error),

    /// Likely a bad HTTP status code or unparseable response.
    #[fail(display = "could not send request")]
    Http(#[cause] HttpError),

    #[fail(display = "upstream requests rate limited")]
    RateLimited(UpstreamRateLimits),

    #[fail(display = "upstream request returned error {}", _0)]
    ResponseError(StatusCode, #[cause] ApiErrorResponse),

    #[fail(display = "channel closed")]
    ChannelClosed,
}

impl UpstreamRequestError {
    /// Returns `true` if the error indicates a network downtime.
    fn is_network_error(&self) -> bool {
        match self {
            Self::SendFailed(_) => true,
            Self::ResponseError(code, _) => matches!(code.as_u16(), 502 | 503 | 504),
            Self::Http(http) => http.is_network_error(),
            _ => false,
        }
    }

    /// Returns `true` if the upstream has permanently rejected this Relay.
    ///
    /// This Relay should cease communication with the upstream and may shut down.
    fn is_permanent_rejection(&self) -> bool {
        match self {
            Self::ResponseError(status_code, response) => {
                *status_code == StatusCode::FORBIDDEN
                    && response.relay_action() == RelayErrorAction::Stop
            }
            _ => false,
        }
    }

    /// Returns `true` if the request was received by the upstream.
    ///
    /// Despite resulting in an error, the server has received and acknowledged the request. This
    /// includes rate limits (status code 429), and bad payloads (4XX), but not network errors
    /// (502-504).
    pub fn is_received(&self) -> bool {
        match self {
            // Rate limits are a special case of `ResponseError(429, _)`.
            Self::RateLimited(_) => true,
            // Everything except network errors indicates the upstream has handled this request.
            Self::ResponseError(_, _) | Self::Http(_) => !self.is_network_error(),
            // Remaining kinds indicate a failure to send the request.
            Self::NoCredentials | Self::SendFailed(_) | Self::ChannelClosed => false,
        }
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

pub trait RequestBuilderTransformer: 'static + Send {
    fn build_request(&mut self, _: RequestBuilder) -> Result<Request, UpstreamRequestError>;
}

impl RequestBuilderTransformer for () {
    fn build_request(&mut self, builder: RequestBuilder) -> Result<Request, UpstreamRequestError> {
        builder.finish().map_err(UpstreamRequestError::Http)
    }
}

impl<F> RequestBuilderTransformer for F
where
    F: FnMut(RequestBuilder) -> Result<Request, UpstreamRequestError> + Send + 'static,
{
    fn build_request(&mut self, builder: RequestBuilder) -> Result<Request, UpstreamRequestError> {
        self(builder)
    }
}

/// Handles a response returned from the upstream.
///
/// If the response indicates success via 2XX status codes, `Ok(response)` is returned. Otherwise,
/// the response is consumed and an error is returned. If intercept_status_errors is set to true,
/// depending on the status code and details provided in the payload, one
/// of the following errors is returned:
///
///  1. `RateLimited` for a `429` status code.
///  2. `ResponseError` in all other cases.
fn handle_response(
    response: Response,
    intercept_status_errors: bool,
    max_response_size: usize,
) -> ResponseFuture<Response, UpstreamRequestError> {
    let status = response.status();

    if !intercept_status_errors || status.is_success() {
        return Box::new(future::ok(response));
    }

    let upstream_limits = if status == StatusCode::TOO_MANY_REQUESTS {
        let retry_after = response
            .get_header(header::RETRY_AFTER)
            .and_then(|v| str::from_utf8(v).ok());

        let rate_limits = response
            .get_all_headers(utils::RATE_LIMITS_HEADER)
            .iter()
            .filter_map(|v| str::from_utf8(v).ok())
            .join(", ");

        let upstream_limits = UpstreamRateLimits::new()
            .retry_after(retry_after)
            .rate_limits(rate_limits);

        Some(upstream_limits)
    } else {
        None
    };

    // At this point, we consume the Response. This means we need to consume the response
    // payload stream, regardless of the status code. Parsing the JSON body may fail, which is a
    // non-fatal failure as the upstream is not expected to always include a valid JSON response.
    let future = response
        .json(max_response_size)
        .then(move |json_result: Result<_, HttpError>| {
            if let Some(upstream_limits) = upstream_limits {
                Err(UpstreamRequestError::RateLimited(upstream_limits))
            } else {
                // Coerce the result into an empty `ApiErrorResponse` if parsing JSON did not succeed.
                let api_response = json_result.unwrap_or_default();
                Err(UpstreamRequestError::ResponseError(status, api_response))
            }
        });

    Box::new(future)
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
    high_prio_requests: VecDeque<EnqueuedRequest>,
    low_prio_requests: VecDeque<EnqueuedRequest>,
    config: Arc<Config>,
    reqwest_client: reqwest::Client,
    /// "reqwest runtime" as this tokio runtime is currently only spawned such that reqwest can
    /// run.
    reqwest_runtime: tokio::runtime::Runtime,
}

impl UpstreamRelay {
    /// Creates a new `UpstreamRelay` instance.
    pub fn new(config: Arc<Config>) -> Self {
        let reqwest_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let reqwest_client = reqwest::ClientBuilder::new()
            .connect_timeout(config.http_connection_timeout())
            .timeout(config.http_timeout())
            // In actix-web client this option could be set on a per-request basis.  In reqwest
            // this option can only be set per-client. For non-forwarded upstream requests that is
            // desirable, so we have it enabled.
            //
            // In the forward endpoint, this means that content negotiation is done twice, and the
            // response body is first decompressed by reqwest, then re-compressed by actix-web.
            .gzip(true)
            .trust_dns(true)
            .build()
            .unwrap();

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
            reqwest_runtime,

            reqwest_client,
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
        if self.outage_backoff.started() {
            relay_log::info!("Recovering from network outage.")
        }

        self.first_error = None;
        self.outage_backoff.reset();
    }

    fn upstream_connection_check(&mut self, ctx: &mut Context<Self>) {
        let next_backoff = self.outage_backoff.next_backoff();
        relay_log::warn!(
            "Network outage, scheduling another check in {:?}",
            next_backoff
        );
        ctx.notify_later(CheckUpstreamConnection::new(), next_backoff);
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
            self.upstream_connection_check(ctx);
        }
    }

    fn send_request(&mut self, mut request: EnqueuedRequest, ctx: &mut Context<Self>) {
        let uri = self
            .config
            .upstream_descriptor()
            .get_url(request.request.path().as_ref());

        let host_header = self
            .config
            .http_host_header()
            .unwrap_or_else(|| self.config.upstream_descriptor().host());

        let method =
            reqwest::Method::from_bytes(request.request.method().as_ref().as_bytes()).unwrap();

        let builder = self.reqwest_client.request(method, uri);
        let mut builder = RequestBuilder::reqwest(builder);

        builder.header("Host", host_header.as_bytes());

        if request.request.set_relay_id() {
            if let Some(ref credentials) = self.config.credentials() {
                builder.header("X-Sentry-Relay-Id", credentials.id.to_string().as_bytes());
            }
        }

        //try to build a ClientRequest
        let client_request = match request.request.build(builder) {
            Err(e) => {
                request
                    .request
                    .respond(Err(UpstreamRequestError::Http(e)))
                    .into_actor(self)
                    .spawn(ctx);
                return;
            }
            Ok(client_request) => client_request,
        };

        // we are about to send a HTTP message keep track of requests in flight
        self.num_inflight_requests += 1;

        let intercept_status_errors = request.request.intercept_status_errors();

        let send_start = Instant::now();

        let client = self.reqwest_client.clone();

        let (tx, rx) = oneshot::channel();

        self.reqwest_runtime.spawn(async move {
            let res = client
                .execute(client_request.0)
                .await
                .map_err(UpstreamRequestError::SendFailed);
            tx.send(res)
        });

        let future = rx
            .map_err(|_| UpstreamRequestError::ChannelClosed)
            .flatten()
            .map(Response);

        let max_response_size = self.config.max_api_payload_size();

        future
            .track(ctx.address().recipient())
            .and_then(move |response| {
                handle_response(response, intercept_status_errors, max_response_size)
            })
            .into_actor(self)
            .then(move |send_result, slf, ctx| {
                slf.handle_http_response(send_start, request, send_result, ctx);
                fut::ok(())
            })
            .spawn(ctx);
    }

    /// Adds a metric for the upstream request.
    fn meter_result(
        send_start: Instant,
        request: &EnqueuedRequest,
        send_result: &Result<Response, UpstreamRequestError>,
    ) {
        let sc;
        let sc2;

        let (status_code, result) = match send_result {
            Ok(ref client_response) => {
                sc = client_response.status();
                (sc.as_str(), "success")
            }
            Err(UpstreamRequestError::ResponseError(status_code, _)) => {
                (status_code.as_str(), "response_error")
            }
            Err(UpstreamRequestError::Http(HttpError::Io(_))) => ("-", "payload_failed"),
            Err(UpstreamRequestError::Http(HttpError::Json(_))) => ("-", "invalid_json"),
            Err(UpstreamRequestError::Http(HttpError::Reqwest(error))) => {
                sc2 = error.status();
                (
                    sc2.as_ref().map(|x| x.as_str()).unwrap_or("-"),
                    "reqwest_error",
                )
            }
            Err(UpstreamRequestError::Http(HttpError::Custom(_))) => ("-", "internal"),

            Err(UpstreamRequestError::SendFailed(_)) => ("-", "send_failed"),
            Err(UpstreamRequestError::RateLimited(_)) => ("-", "rate_limited"),
            Err(UpstreamRequestError::NoCredentials)
            | Err(UpstreamRequestError::ChannelClosed)
            | Err(UpstreamRequestError::Http(HttpError::Overflow)) => {
                // these are not errors caused when sending to upstream so we don't need to log anything
                relay_log::error!("meter_result called for unsupported error");
                return;
            }
        };

        metric!(
            timer(RelayTimers::UpstreamRequestsDuration) = send_start.elapsed(),
            result = result,
            status_code = status_code,
            route = request.route_name(),
            retries = request.retries_bucket(),
        );

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
        send_start: Instant,
        mut request: EnqueuedRequest,
        send_result: Result<Response, UpstreamRequestError>,
        ctx: &mut Context<Self>,
    ) {
        UpstreamRelay::meter_result(send_start, &request, &send_result);
        if matches!(send_result, Err(ref err) if err.is_network_error()) {
            self.handle_network_error(ctx);

            if request.request.retry() {
                request.previous_retries += 1;
                return self.enqueue(request, ctx, EnqueuePosition::Back);
            }
        } else {
            // we managed a request without a network error, reset the first time we got a network
            // error and resume sending events.
            self.reset_network_error();
        }

        request
            .request
            .respond(send_result)
            .into_actor(self)
            .spawn(ctx);
    }

    /// Enqueues a request and ensures that the message queue advances.
    fn enqueue(
        &mut self,
        request: EnqueuedRequest,
        ctx: &mut Context<Self>,
        position: EnqueuePosition,
    ) {
        let name = request.request.priority().name();
        let queue = match request.request.priority() {
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

    fn enqueue_query<Q: 'static + UpstreamQuery>(
        &mut self,
        query: Q,
        ctx: &mut Context<Self>,
    ) -> ResponseFuture<Q::Response, UpstreamRequestError> {
        let credentials = tryf!(self
            .config
            .credentials()
            .ok_or(UpstreamRequestError::NoCredentials));

        let (body, signature) = credentials.secret_key.pack(&query);

        let max_response_size = self.config.max_api_payload_size();

        let (tx, rx) = oneshot::channel();

        let upstream_request = UpstreamQueryRequest {
            query,
            body,
            signature,
            response_sender: Some(tx),
        };

        self.enqueue(
            EnqueuedRequest {
                request: Box::new(upstream_request),
                previous_retries: 0,
            },
            ctx,
            EnqueuePosition::Front,
        );

        let future = rx
            .map_err(|_| UpstreamRequestError::ChannelClosed)
            .and_then(|result| result);

        let future = future.and_then(move |r| {
            r.json(max_response_size)
                .map_err(UpstreamRequestError::Http)
        });

        Box::new(future)
    }
}

impl Actor for UpstreamRelay {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        relay_log::info!("upstream relay started");

        self.auth_backoff.reset();
        self.outage_backoff.reset();

        if self.should_authenticate() {
            context.notify(Authenticate);
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("upstream relay stopped");
    }
}

impl Supervised for UpstreamRelay {}

impl SystemService for UpstreamRelay {}

impl Default for UpstreamRelay {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
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
            relay_log::error!("{}", auth_state_error);
            return Box::new(fut::err(()));
        }

        let credentials = match self.config.credentials() {
            Some(x) => x,
            None => return Box::new(fut::err(())),
        };

        relay_log::info!(
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
                relay_log::debug!("got register challenge (token = {})", challenge.token());
                let challenge_response = challenge.into_response();

                relay_log::debug!("sending register challenge response");
                slf.enqueue_query(challenge_response, ctx).into_actor(slf)
            })
            .map(|_, slf, ctx| {
                relay_log::info!("relay successfully registered with upstream");
                slf.auth_state = AuthState::Registered;
                slf.auth_backoff.reset();

                if let Some(renew_interval) = slf.renew_auth_interval() {
                    ctx.notify_later(Authenticate, renew_interval);
                }

                // Resume sending queued requests if we suspended due to dropped authentication
                ctx.notify(PumpHttpMessageQueue);
            })
            .map_err(move |err, slf, ctx| {
                relay_log::error!("authentication encountered error: {}", LogError(&err));

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
                relay_log::debug!(
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

pub struct IsNetworkOutage;

impl Message for IsNetworkOutage {
    type Result = bool;
}

/// The `IsNetworkOutage` message is an internal Relay message that is used to
/// query the current state of network connection with the upstream server.
///
/// Currently it is only used by the HealthCheck actor to emit the
/// `upstream.network_outage` metric.
impl Handler<IsNetworkOutage> for UpstreamRelay {
    type Result = bool;

    fn handle(&mut self, _msg: IsNetworkOutage, _ctx: &mut Self::Context) -> Self::Result {
        self.is_network_outage()
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
struct CheckUpstreamConnection {
    response_sender: Option<oneshot::Sender<Result<Response, UpstreamRequestError>>>,
}

impl CheckUpstreamConnection {
    fn new() -> Self {
        CheckUpstreamConnection {
            response_sender: None,
        }
    }
    fn from_sender(sender: oneshot::Sender<Result<Response, UpstreamRequestError>>) -> Self {
        CheckUpstreamConnection {
            response_sender: Some(sender),
        }
    }
}

impl UpstreamRequest for CheckUpstreamConnection {
    fn method(&self) -> Method {
        Method::GET
    }

    fn path(&self) -> Cow<'_, str> {
        Cow::Borrowed("/api/0/relays/live/")
    }

    fn retry(&self) -> bool {
        false
    }

    fn priority(&self) -> RequestPriority {
        RequestPriority::Immediate
    }
    fn set_relay_id(&self) -> bool {
        true
    }

    fn intercept_status_errors(&self) -> bool {
        true
    }

    fn build(&mut self, builder: RequestBuilder) -> Result<Request, HttpError> {
        builder.finish()
    }

    fn respond(
        &mut self,
        response: Result<Response, UpstreamRequestError>,
    ) -> ResponseFuture<(), ()> {
        let sender = self.response_sender.take();
        match response {
            Ok(response) => {
                let fut =
                    response
                        .consume()
                        .map_err(UpstreamRequestError::Http)
                        .then(move |resp| {
                            sender.map(|sender| sender.send(resp).ok());
                            Ok(())
                        });
                Box::new(fut)
            }
            Err(err) => {
                sender.map(|sender| sender.send(Err(err)));
                Box::new(future::err(()))
            }
        }
    }
}

impl Message for CheckUpstreamConnection {
    type Result = ();
}

impl Handler<CheckUpstreamConnection> for UpstreamRelay {
    type Result = ();

    fn handle(&mut self, _msg: CheckUpstreamConnection, ctx: &mut Self::Context) -> Self::Result {
        let (tx, rx) = oneshot::channel();

        //create a request with sender (so we can send back termination info)
        let request = Box::new(CheckUpstreamConnection::from_sender(tx));

        self.enqueue(
            EnqueuedRequest {
                request,
                previous_retries: 0,
            },
            ctx,
            EnqueuePosition::Front,
        );

        let future = rx
            // map errors caused by the oneshot channel being closed (unlikely)
            .map_err(|_| UpstreamRequestError::ChannelClosed)
            // unwrap the result (this is how we transport the http failure through the channel)
            .and_then(|result| result);

        future
            .into_actor(self)
            .then(|result, slf, ctx| {
                if matches!(result, Err(err) if err.is_network_error()) {
                    // still network error, schedule another attempt
                    slf.upstream_connection_check(ctx);
                } else {
                    // resume normal messages
                    ctx.notify(PumpHttpMessageQueue);
                }
                fut::ok(())
            })
            .spawn(ctx);
    }
}

pub trait ResponseTransformer: 'static {
    type Result: 'static + IntoFuture;

    fn transform_response(self, _: Response) -> Self::Result;
}

impl ResponseTransformer for () {
    type Result = ResponseFuture<(), UpstreamRequestError>;

    fn transform_response(self, response: Response) -> Self::Result {
        Box::new(
            response
                .consume()
                .map(|_| ())
                .map_err(UpstreamRequestError::Http),
        )
    }
}

impl<F, T> ResponseTransformer for F
where
    F: 'static + FnOnce(Response) -> T,
    T: 'static + IntoFuture,
{
    type Result = T;

    fn transform_response(self, response: Response) -> Self::Result {
        self(response)
    }
}

/// TODO: Doc
pub trait UpstreamRequest: Send {
    ///type Response: Send + 'static;

    /// The HTTP method of the request.
    fn method(&self) -> Method;

    /// The path relative to the upstream.
    fn path(&self) -> Cow<'_, str>;

    /// Whether this request should retry on network errors.
    fn retry(&self) -> bool {
        true
    }

    /// The queueing priority of the request. Defaults to `Low`.
    fn priority(&self) -> RequestPriority {
        RequestPriority::Low
    }

    /// TODO: Doc
    fn intercept_status_errors(&self) -> bool {
        true
    }

    /// TODO: Doc
    fn set_relay_id(&self) -> bool {
        true
    }

    /// TODO: Doc
    fn build(&mut self, builder: RequestBuilder) -> Result<Request, HttpError>;

    /// TODO doc
    fn respond(
        &mut self,
        response: Result<Response, UpstreamRequestError>,
    ) -> ResponseFuture<(), ()> {
        //just consumes the response in case it is not needed
        match response {
            Ok(response) => Box::new(response.consume().map(|_| ()).map_err(|e| {
                relay_log::error!("failed to consume response: {}", LogError(&e));
            })),
            Err(_) => Box::new(futures::future::err(())),
        }
    }
}

struct EnqueuedRequest {
    request: Box<dyn UpstreamRequest>,
    previous_retries: u32,
}

impl EnqueuedRequest {
    fn route_name(&self) -> &'static str {
        if self.request.path().contains("/outcomes/") {
            "outcomes"
        } else if self.request.path().contains("/envelope/") {
            "envelope"
        } else if self.request.path().contains("/projectids/") {
            "project_ids"
        } else if self.request.path().contains("/projectconfigs/") {
            "project_configs"
        } else if self.request.path().contains("/publickeys/") {
            "public_keys"
        } else if self.request.path().contains("/challenge/") {
            "challenge"
        } else if self.request.path().contains("/response/") {
            "response"
        } else if self.request.path().contains("/live/") {
            "check_live"
        } else {
            "unknown"
        }
    }

    fn retries_bucket(&self) -> &'static str {
        match self.previous_retries {
            0 => "0",
            1 => "1",
            2 => "2",
            3..=10 => "few",
            _ => "many",
        }
    }
}

pub struct SendRequest<T: UpstreamRequest>(pub T);

impl<T> Message for SendRequest<T>
where
    T: UpstreamRequest,
{
    type Result = ();
}

impl<T> Handler<SendRequest<T>> for UpstreamRelay
where
    T: UpstreamRequest + 'static,
{
    type Result = ();
    fn handle(&mut self, msg: SendRequest<T>, ctx: &mut Self::Context) -> Self::Result {
        self.enqueue(
            EnqueuedRequest {
                request: Box::new(msg.0),
                previous_retries: 0,
            },
            ctx,
            EnqueuePosition::Front,
        );
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

pub trait UpstreamQuery: Serialize + Send + 'static {
    type Response: DeserializeOwned + 'static + Send;

    /// The HTTP method of the request.
    fn method(&self) -> Method;

    /// The path relative to the upstream.
    fn path(&self) -> Cow<'static, str>;

    /// Whether this request should retry on network errors.
    fn retry() -> bool;

    /// The queueing priority of the request. Defaults to `Low`.
    fn priority() -> RequestPriority {
        RequestPriority::Low
    }
}

pub struct SendQuery<T: UpstreamQuery>(pub T);

impl<T: UpstreamQuery> Message for SendQuery<T> {
    type Result = Result<T::Response, UpstreamRequestError>;
}

struct UpstreamQueryRequest<T: UpstreamQuery> {
    query: T,
    body: Vec<u8>,
    signature: String,
    response_sender: Option<oneshot::Sender<Result<Response, UpstreamRequestError>>>,
}

impl<T: UpstreamQuery> UpstreamQueryRequest<T> {
    /// Helper function to use the response sender
    fn send_response(&mut self, response: Result<Response, UpstreamRequestError>) {
        if let Some(response_sender) = self.response_sender.take() {
            response_sender
                .send(response)
                .map_err(|_| {
                    relay_log::error!("failed to send response through oneshot channel");
                })
                .ok();
        } else {
            relay_log::error!("response_sender already used");
        }
    }
}

impl<T: UpstreamQuery> UpstreamRequest for UpstreamQueryRequest<T> {
    fn method(&self) -> Method {
        self.query.method()
    }

    fn path(&self) -> Cow<'_, str> {
        self.query.path()
    }

    fn build(&mut self, mut builder: RequestBuilder) -> Result<Request, HttpError> {
        builder.header(
            "X-Sentry-Relay-Signature",
            self.signature.as_str().as_bytes(),
        );
        builder.header(header::CONTENT_TYPE, b"application/json");
        builder.body(&self.body)
    }

    fn retry(&self) -> bool {
        T::retry()
    }

    fn priority(&self) -> RequestPriority {
        T::priority()
    }

    fn respond(
        &mut self,
        response: Result<Response, UpstreamRequestError>,
    ) -> ResponseFuture<(), ()> {
        let result = if response.is_ok() {
            future::ok(())
        } else {
            future::err(())
        };
        self.send_response(response);
        Box::new(result)
    }
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
