use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use itertools::Itertools;
use relay_log::LogError;
use reqwest::header;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::mpsc;
use tokio::time::Instant;

use relay_auth::{RegisterChallenge, RegisterRequest, RegisterResponse, Registration};
use relay_common::RetryBackoff;
use relay_config::{Config, Credentials, RelayMode};
use relay_quotas::{
    DataCategories, QuotaScope, RateLimit, RateLimitScope, RateLimits, RetryAfter, Scoping,
};
use relay_system::{
    Addr, AsyncResponse, FromMessage, Interface, MessageResponse, NoResponse, Sender, Service,
};

use crate::http::{HttpError, Request, RequestBuilder, Response, StatusCode};
use crate::service::REGISTRY;
use crate::statsd::{RelayHistograms, RelayTimers};
use crate::utils::{self, ApiErrorResponse, RelayErrorAction};

pub use reqwest::Method;

/// Rate limits returned by the upstream.
///
/// Upstream rate limits can come in two forms:
///  - `Retry-After` header with a generic timeout for all categories.
///  - `X-Sentry-Rate-Limits` header with fine-grained information on applied rate limits.
///
/// These limits do not carry scope information. Use `UpstreamRateLimits::scope` to attach scope
/// identifiers and return a fully populated `RateLimits` instance.
#[derive(Debug, Clone)]
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

/// TODO(ja): Doc
#[derive(Debug, thiserror::Error)]
pub enum UpstreamRequestError {
    #[error("attempted to send upstream request without credentials configured")]
    NoCredentials,

    /// As opposed to HTTP variant this contains all network errors.
    #[error("could not send request to upstream")]
    SendFailed(#[from] reqwest::Error),

    /// Likely a bad HTTP status code or unparseable response.
    #[error("could not send request")]
    Http(#[source] HttpError),

    #[error("upstream requests rate limited")]
    RateLimited(UpstreamRateLimits),

    #[error("upstream request returned error {0}")]
    ResponseError(StatusCode, #[source] ApiErrorResponse),

    #[error("channel closed")]
    ChannelClosed,

    #[error("upstream permanently denied authentication")]
    AuthDenied,
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
            Self::NoCredentials | Self::SendFailed(_) | Self::ChannelClosed | Self::AuthDenied => {
                false
            }
        }
    }
}

impl From<HttpError> for UpstreamRequestError {
    fn from(error: HttpError) -> Self {
        match error {
            HttpError::NoCredentials => Self::NoCredentials,
            other => Self::Http(other),
        }
    }
}

/// TODO(ja): Doc
///
/// The `IsAuthenticated` message is an internal Relay message that is used to query the current
/// state of authentication with the upstream sever.
///
/// Currently it is only used by the HealthCheck actor.
#[derive(Debug)]
pub struct IsAuthenticated;

/// TODO(ja): Doc
///
/// The `IsNetworkOutage` message is an internal Relay message that is used to
/// query the current state of network connection with the upstream server.
///
/// Currently it is only used by the HealthCheck actor to emit the
/// `upstream.network_outage` metric.
#[derive(Debug)]
pub struct IsNetworkOutage;

/// Priority of an upstream request for queueing.
///
/// Requests are queued and send to the HTTP connections according to their priorities
/// High priority messages are sent first and then, when no high priority message is pending,
/// low priority messages are sent. Within the same priority messages are sent FIFO.
#[derive(Clone, Copy, Debug)]
pub enum RequestPriority {
    /// High priority, low volume messages (e.g. ProjectConfig, ProjectStates, Registration messages).
    High,
    /// Low priority, high volume messages (e.g. Events and Outcomes).
    Low,
}

impl RequestPriority {
    fn name(&self) -> &'static str {
        match self {
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

/// Represents an HTTP request to be sent to the upstream.
pub trait UpstreamRequest: Send + Sync + fmt::Debug {
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

    /// True if normal error processing should occur, false if
    /// errors from the upstream should not be processed and returned as is
    /// in the response.
    fn intercept_status_errors(&self) -> bool {
        true
    }

    /// If set to True it will add the X-Sentry-Relay-Id header to the request
    ///
    /// This should be done (only) for calls to endpoints that use Relay authentication.
    fn set_relay_id(&self) -> bool {
        true
    }

    /// Called whenever the request will be send over HTTP (possible multiple times)
    fn build(&mut self, config: &Config, builder: RequestBuilder) -> Result<Request, HttpError>;

    /// Called when the HTTP request completes, either with success or an error that will not
    /// be retried.
    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>;
}

/// TODO(ja): Doc
#[derive(Debug)]
pub struct SendRequest<T: UpstreamRequest>(pub T);

/// TODO(ja): Doc
pub trait UpstreamQuery: Serialize + Send + Sync + fmt::Debug {
    type Response: DeserializeOwned + Send;

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

/// TODO(ja): Doc
type QuerySender<T> = Sender<Result<<T as UpstreamQuery>::Response, UpstreamRequestError>>;

/// TODO(ja): Doc
#[derive(Debug)]
struct UpstreamQueryRequest<T: UpstreamQuery> {
    query: T,
    compiled: Option<(Vec<u8>, String)>,
    max_response_size: usize,
    sender: QuerySender<T>,
}

impl<T> UpstreamQueryRequest<T>
where
    T: UpstreamQuery + 'static,
{
    fn new(query: T, sender: QuerySender<T>) -> Self {
        Self {
            query,
            compiled: None,
            max_response_size: 0,
            sender,
        }
    }
}

impl<T> UpstreamRequest for UpstreamQueryRequest<T>
where
    T: UpstreamQuery + 'static,
{
    fn retry(&self) -> bool {
        T::retry()
    }

    fn priority(&self) -> RequestPriority {
        T::priority()
    }

    fn intercept_status_errors(&self) -> bool {
        true
    }

    fn set_relay_id(&self) -> bool {
        true
    }

    fn method(&self) -> Method {
        self.query.method()
    }

    fn path(&self) -> Cow<'_, str> {
        self.query.path()
    }

    fn build(
        &mut self,
        config: &Config,
        mut builder: RequestBuilder,
    ) -> Result<Request, HttpError> {
        let credentials = config.credentials().ok_or(HttpError::NoCredentials)?;
        let (body, signature) = self
            .compiled
            .get_or_insert_with(|| credentials.secret_key.pack(&self.query));

        // TODO(ja): Doc why we do this or add config to `respond`.
        self.max_response_size = config.max_api_payload_size();

        relay_statsd::metric!(
            histogram(RelayHistograms::UpstreamQueryBodySize) = body.len() as u64
        );

        builder.header("X-Sentry-Relay-Signature", signature.as_bytes());
        builder.header(header::CONTENT_TYPE, b"application/json");
        builder.body(&body)
    }

    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let result = match result {
                Ok(response) => response
                    .json(self.max_response_size)
                    .await
                    .map_err(UpstreamRequestError::Http),
                Err(error) => Err(error),
            };

            self.sender.send(result)
        })
    }
}

/// TODO(ja): Doc
#[derive(Debug)]
pub struct SendQuery<T: UpstreamQuery>(pub T);

/// TODO(ja): Doc
#[derive(Debug)]
pub enum UpstreamRelay {
    IsAuthenticated(IsAuthenticated, Sender<bool>),
    IsNetworkOutage(IsNetworkOutage, Sender<bool>),
    SendRequest(Box<dyn UpstreamRequest>),
}

impl UpstreamRelay {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().upstream_relay.clone()
    }
}

impl Interface for UpstreamRelay {}

impl FromMessage<IsAuthenticated> for UpstreamRelay {
    type Response = AsyncResponse<bool>;

    fn from_message(message: IsAuthenticated, sender: Sender<bool>) -> Self {
        Self::IsAuthenticated(message, sender)
    }
}

impl FromMessage<IsNetworkOutage> for UpstreamRelay {
    type Response = AsyncResponse<bool>;

    fn from_message(message: IsNetworkOutage, sender: Sender<bool>) -> Self {
        Self::IsNetworkOutage(message, sender)
    }
}

impl<T> FromMessage<SendRequest<T>> for UpstreamRelay
where
    T: UpstreamRequest + 'static,
{
    type Response = NoResponse;

    fn from_message(message: SendRequest<T>, _: ()) -> Self {
        let SendRequest(request) = message;
        Self::SendRequest(Box::new(request))
    }
}

impl<T> FromMessage<SendQuery<T>> for UpstreamRelay
where
    T: UpstreamQuery + 'static,
{
    type Response = AsyncResponse<Result<T::Response, UpstreamRequestError>>;

    fn from_message(message: SendQuery<T>, sender: QuerySender<T>) -> Self {
        let SendQuery(query) = message;
        Self::SendRequest(Box::new(UpstreamQueryRequest::new(query, sender)))
    }
}

/// Adds a metric for the upstream request.
fn emit_response_metrics(
    send_start: Instant,
    request: &dyn UpstreamRequest,
    send_result: &Result<Response, UpstreamRequestError>,
    attempt: usize,
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

        Err(UpstreamRequestError::SendFailed(_)) => ("-", "send_failed"),
        Err(UpstreamRequestError::RateLimited(_)) => ("-", "rate_limited"),
        Err(UpstreamRequestError::NoCredentials)
        | Err(UpstreamRequestError::ChannelClosed)
        | Err(UpstreamRequestError::AuthDenied)
        | Err(UpstreamRequestError::Http(HttpError::Overflow))
        | Err(UpstreamRequestError::Http(HttpError::NoCredentials)) => {
            // these are not errors caused when sending to upstream so we don't need to log anything
            relay_log::error!("meter_result called for unsupported error");
            return;
        }
    };

    // TODO(ja): Move this to the trait
    let path = request.path();
    let route_name = if path.contains("/outcomes/") {
        "outcomes"
    } else if path.contains("/envelope/") {
        "envelope"
    } else if path.contains("/projectids/") {
        "project_ids"
    } else if path.contains("/projectconfigs/") {
        "project_configs"
    } else if path.contains("/publickeys/") {
        "public_keys"
    } else if path.contains("/challenge/") {
        "challenge"
    } else if path.contains("/response/") {
        "response"
    } else if path.contains("/live/") {
        "check_live"
    } else {
        "unknown"
    };

    relay_statsd::metric!(
        timer(RelayTimers::UpstreamRequestsDuration) = send_start.elapsed(),
        result = result,
        status_code = status_code,
        route = route_name,
        retries = match attempt {
            0 => "0",
            1 => "1",
            2 => "2",
            3..=10 => "few",
            _ => "many",
        },
    );

    relay_statsd::metric!(
        histogram(RelayHistograms::UpstreamRetries) = attempt as u64,
        result = result,
        status_code = status_code,
        route = route_name,
    );
}

/// Checks the status of the network connection with the upstream server
#[derive(Debug)]
struct GetHealthCheck;

impl UpstreamRequest for GetHealthCheck {
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
        unreachable!("sent directly to client")
    }

    fn set_relay_id(&self) -> bool {
        true
    }

    fn intercept_status_errors(&self) -> bool {
        true
    }

    fn build(&mut self, _: &Config, builder: RequestBuilder) -> Result<Request, HttpError> {
        builder.finish()
    }

    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async {
            if let Ok(mut response) = result {
                response.consume().await.ok();
            }
        })
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
        unreachable!("sent directly to client")
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
        unreachable!("sent directly to client")
    }

    fn retry() -> bool {
        false
    }
}

#[derive(Debug)]
struct SharedClient {
    config: Arc<Config>,
    reqwest: reqwest::Client,
}

impl SharedClient {
    fn build(config: Arc<Config>) -> Self {
        let reqwest = reqwest::ClientBuilder::new()
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

        Self { config, reqwest }
    }

    /// TODO(ja): Doc
    fn build_request(
        &self,
        request: &mut dyn UpstreamRequest,
    ) -> Result<reqwest::Request, UpstreamRequestError> {
        tokio::task::block_in_place(|| {
            let url = self
                .config
                .upstream_descriptor()
                .get_url(request.path().as_ref());

            let host_header = self
                .config
                .http_host_header()
                .unwrap_or_else(|| self.config.upstream_descriptor().host());

            let method = request.method();

            let mut builder = RequestBuilder::reqwest(self.reqwest.request(method, url));
            builder.header("Host", host_header.as_bytes());

            if request.set_relay_id() {
                if let Some(credentials) = self.config.credentials() {
                    builder.header("X-Sentry-Relay-Id", credentials.id.to_string());
                }
            }

            match request.build(&self.config, builder) {
                Ok(Request(client_request)) => Ok(client_request),
                Err(e) => Err(e.into()),
            }
        })
    }

    /// Handles an HTTP response returned from the upstream.
    ///
    /// If the response indicates success via 2XX status codes, `Ok(response)` is returned.
    /// Otherwise, the response is consumed and an error is returned. If `intercept_status_errors`
    /// is set to `true` on the request, depending on the status code and details provided in the
    /// payload, one of the following errors is returned:
    ///
    ///  1. `RateLimited` for a `429` status code.
    ///  2. `ResponseError`  in all other cases, containing the status and details.
    async fn transform_response(
        &self,
        request: &dyn UpstreamRequest,
        response: Response,
    ) -> Result<Response, UpstreamRequestError> {
        let status = response.status();

        if !request.intercept_status_errors() || status.is_success() {
            return Ok(response);
        }

        let upstream_limits = if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .get_header(header::RETRY_AFTER)
                .and_then(|v| std::str::from_utf8(v).ok());

            let rate_limits = response
                .get_all_headers(utils::RATE_LIMITS_HEADER)
                .iter()
                .filter_map(|v| std::str::from_utf8(v).ok())
                .join(", "); // TODO(ja): Avoid this stringify roundtrip

            let upstream_limits = UpstreamRateLimits::new()
                .retry_after(retry_after)
                .rate_limits(rate_limits);

            Some(upstream_limits)
        } else {
            None // TODO(ja): Check if we still need to consume responses
        };

        // At this point, we consume the Response. This means we need to consume the response
        // payload stream, regardless of the status code. Parsing the JSON body may fail, which is a
        // non-fatal failure as the upstream is not expected to always include a valid JSON
        // response.
        let json_result = response.json(self.config.max_api_payload_size()).await;

        if let Some(upstream_limits) = upstream_limits {
            Err(UpstreamRequestError::RateLimited(upstream_limits))
        } else {
            // Coerce the result into an empty `ApiErrorResponse` if parsing JSON did not succeed.
            let api_response = json_result.unwrap_or_default();
            Err(UpstreamRequestError::ResponseError(status, api_response))
        }
    }

    /// TODO(ja): Doc
    async fn send(
        &self,
        request: &mut dyn UpstreamRequest,
    ) -> Result<Response, UpstreamRequestError> {
        let send_start = Instant::now();

        let client_request = self.build_request(request)?;
        let response = self.reqwest.execute(client_request).await?;
        let result = self.transform_response(request, Response(response)).await;

        emit_response_metrics(send_start, request, &result, 1); // TODO(ja): continue logging attempts?
        result
    }

    /// TODO(ja): Doc
    pub async fn send_query<T>(&self, query: T) -> Result<T::Response, UpstreamRequestError>
    where
        T: UpstreamQuery + 'static,
    {
        let (sender, receiver) = AsyncResponse::channel();

        let mut request = Box::new(UpstreamQueryRequest::new(query, sender));
        let result = self.send(request.as_mut()).await;
        request.respond(result).await;

        receiver
            .await
            .unwrap_or(Err(UpstreamRequestError::ChannelClosed))
    }
}

/// The position for enqueueing an upstream request.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum EnqueuePosition {
    Front,
    Back,
}

#[derive(Debug)]
struct UpstreamQueue {
    high: VecDeque<Box<dyn UpstreamRequest>>,
    low: VecDeque<Box<dyn UpstreamRequest>>,
}

impl UpstreamQueue {
    pub fn new() -> Self {
        Self {
            high: VecDeque::new(),
            low: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.high.len() + self.low.len()
    }

    fn push(&mut self, request: Box<dyn UpstreamRequest>, position: EnqueuePosition) {
        let priority = request.priority();

        // We can ignore send errors here. Once the channel closes, we drop all incoming requests
        // here. The receiving end of the request will be notified of the drop if they are waiting
        // for it.
        let queue = match priority {
            RequestPriority::High => &mut self.high,
            RequestPriority::Low => &mut self.low,
        };

        match position {
            EnqueuePosition::Front => queue.push_front(request),
            EnqueuePosition::Back => queue.push_back(request),
        }

        // TODO(ja): Make this a gauge
        // TODO(ja): Measure queue saturation
        relay_statsd::metric!(
            histogram(RelayHistograms::UpstreamMessageQueueSize) = self.len() as u64,
            priority = priority.name(),
        );
    }

    pub fn enqueue(&mut self, request: Box<dyn UpstreamRequest>) {
        self.push(request, EnqueuePosition::Back)
    }

    pub fn place_back(&mut self, request: Box<dyn UpstreamRequest>) {
        self.push(request, EnqueuePosition::Front)
    }

    pub fn dequeue(&mut self) -> Option<Box<dyn UpstreamRequest>> {
        self.high.pop_front().or_else(|| self.low.pop_front())
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
    /// TODO(ja): Doc
    pub fn init(config: &Config) -> Self {
        match config.relay_mode() {
            RelayMode::Managed => AuthState::Unknown,
            _ => AuthState::Registered,
        }
    }

    /// Returns true if the state is considered authenticated.
    pub fn is_authenticated(self) -> bool {
        matches!(self, AuthState::Registered | AuthState::Renewing)
    }
}

/// TODO(ja): Doc
#[derive(Clone, Copy, Debug)]
enum RequestStatus {
    /// TODO(ja): Doc
    Dropped,
    /// TODO(ja): Doc
    Completed,
}

/// TODO(ja): Doc
#[derive(Debug)]
enum Action {
    /// TODO(ja): Doc
    Retry(Box<dyn UpstreamRequest>),
    // TODO(ja): More distinct naming Action::Complete vs RequestStatus::Completed.
    /// TODO(ja): Doc
    Complete(RequestStatus),
    /// TODO(ja): Doc
    Connected,
    /// TODO(ja): Doc
    UpdateAuth(AuthState),
}

type ActionTx = mpsc::UnboundedSender<Action>;

/// TODO(ja): Doc
#[derive(Debug)]
struct AuthMonitor {
    config: Arc<Config>,
    client: Arc<SharedClient>,
    state: AuthState,
    tx: ActionTx,
}

impl AuthMonitor {
    /// Returns the interval at which this Relay should renew authentication.
    fn renew_auth_interval(&self) -> Option<std::time::Duration> {
        if self.config.processing_enabled() {
            // processing relays do NOT re-authenticate
            None
        } else {
            // only relays the have a configured auth-interval reauthenticate
            self.config.http_auth_interval()
        }
    }

    // TODO(ja): Doc
    fn send_state(&mut self, state: AuthState) -> Result<(), UpstreamRequestError> {
        self.state = state;
        self.tx
            .send(Action::UpdateAuth(state))
            .map_err(|_| UpstreamRequestError::ChannelClosed)
    }

    // TODO(ja): Doc
    async fn authenticate(
        &mut self,
        credentials: &Credentials,
    ) -> Result<(), UpstreamRequestError> {
        relay_log::info!(
            "registering with upstream ({})",
            self.config.upstream_descriptor()
        );

        self.send_state(if self.state.is_authenticated() {
            AuthState::Renewing
        } else {
            AuthState::Registering
        })?;

        let request = RegisterRequest::new(&credentials.id, &credentials.public_key);
        let challenge = self.client.send_query(request).await?;
        relay_log::debug!("got register challenge (token = {})", challenge.token());

        let response = challenge.into_response();
        relay_log::debug!("sending register challenge response");
        self.client.send_query(response).await?;

        relay_log::info!("relay successfully registered with upstream");
        self.send_state(AuthState::Registered)?;

        Ok(())
    }

    /// TODO(ja): Doc
    pub async fn run(mut self) {
        if self.config.relay_mode() != RelayMode::Managed {
            return;
        }

        let config = self.config.clone();
        let Some(credentials) = config.credentials() else {
            // This is checked during setup by `check_config` and should never happen.
            relay_log::error!("authentication called without credentials");
            return;
        };

        let mut backoff = RetryBackoff::new(self.config.http_max_retry_interval());

        loop {
            match self.authenticate(credentials).await {
                Ok(_) => {
                    backoff.reset();

                    match self.renew_auth_interval() {
                        Some(interval) => tokio::time::sleep(interval).await,
                        None => return,
                    }
                }
                Err(err) => {
                    relay_log::error!("authentication encountered error: {}", LogError(&err));

                    // ChannelClosed indicates that there are no more listeners, so we stop
                    // authenticating.
                    if let UpstreamRequestError::ChannelClosed = err {
                        return;
                    }

                    if err.is_permanent_rejection() {
                        self.send_state(AuthState::Denied).ok();
                        return;
                    }

                    // If the authentication request fails due to any reason other than a network
                    // error, go back to `Registering` which indicates that this Relay is not
                    // authenticated. Note that network errors are handled separately by the generic
                    // response handler.
                    if !err.is_network_error() && self.send_state(AuthState::Registering).is_err() {
                        return;
                    }

                    // Even on network errors, retry authentication independently.
                    let backoff = backoff.next_backoff();
                    relay_log::debug!(
                        "scheduling authentication retry in {} seconds",
                        backoff.as_secs()
                    );
                    tokio::time::sleep(backoff).await;
                }
            };
        }
    }
}

#[derive(Clone, Debug)]
struct Connector {
    config: Arc<Config>,
    client: Arc<SharedClient>,
}

impl Connector {
    pub async fn connect(self, tx: ActionTx) {
        let mut backoff = RetryBackoff::new(self.config.http_max_retry_interval());

        loop {
            let next_backoff = backoff.next_backoff();
            relay_log::warn!(
                "Network outage, scheduling another check in {:?}",
                next_backoff
            );

            tokio::time::sleep(next_backoff).await;
            match self.client.send(&mut GetHealthCheck).await {
                Ok(_) => break,
                Err(e) if !e.is_network_error() => break,
                Err(_) => continue,
            }
        }

        tx.send(Action::Connected).ok();
    }
}

#[derive(Debug)]
enum ConnectionState {
    Connected,
    Interrupted(Instant),
    Reconnecting(tokio::task::JoinHandle<()>),
}

#[derive(Debug)]
struct ConnectionMonitor {
    state: ConnectionState,
    connector: Connector,
}

impl ConnectionMonitor {
    pub fn new(config: Arc<Config>, client: Arc<SharedClient>) -> Self {
        Self {
            state: ConnectionState::Connected,
            connector: Connector { config, client },
        }
    }

    fn clean_state(&mut self) -> &ConnectionState {
        if let ConnectionState::Reconnecting(ref task) = self.state {
            if task.is_finished() {
                self.state = ConnectionState::Connected;
            }
        }

        &self.state
    }

    pub fn is_stable(&mut self) -> bool {
        match self.clean_state() {
            ConnectionState::Connected => true,
            ConnectionState::Interrupted(_) => true,
            ConnectionState::Reconnecting(_) => false,
        }
    }

    pub fn is_outage(&mut self) -> bool {
        !self.is_stable()
    }

    pub fn notify_error(&mut self, return_tx: ActionTx) {
        let now = Instant::now();

        let first_error = match self.clean_state() {
            ConnectionState::Connected => now,
            ConnectionState::Interrupted(first) => *first,
            ConnectionState::Reconnecting(_) => return,
        };

        self.state = ConnectionState::Interrupted(first_error);

        // Only take action if we exceeded the grace period.
        if first_error + self.connector.config.http_outage_grace_period() <= now {
            let task = tokio::spawn(self.connector.clone().connect(return_tx));
            self.state = ConnectionState::Reconnecting(task);
        }
    }

    pub fn reset_error(&mut self) {
        if let ConnectionState::Reconnecting(ref task) = self.state {
            task.abort();
        }

        self.state = ConnectionState::Connected;
    }
}

/// TODO(ja): Doc
#[derive(Debug)]
struct Broker {
    client: Arc<SharedClient>,
    queue: UpstreamQueue,
    auth_state: AuthState,
    conn: ConnectionMonitor,
    permits: usize,
    action_tx: ActionTx,
}

impl Broker {
    async fn next_request(&mut self) -> Option<Box<dyn UpstreamRequest>> {
        if self.permits == 0 || self.conn.is_outage() || !self.auth_state.is_authenticated() {
            return None;
        }

        let item = self.queue.dequeue()?;
        self.permits -= 1;
        Some(item)
    }

    async fn enqueue(&mut self, request: Box<dyn UpstreamRequest>) {
        if let AuthState::Denied = self.auth_state {
            // This respond is near-instant because it should just send the error into the request's
            // response channel. We do not expect that this blocks the broker.
            request.respond(Err(UpstreamRequestError::AuthDenied)).await;
        } else {
            self.queue.enqueue(request);
        }
    }

    async fn handle_message(&mut self, message: UpstreamRelay) {
        match message {
            UpstreamRelay::IsAuthenticated(_, sender) => {
                sender.send(self.auth_state.is_authenticated())
            }
            UpstreamRelay::IsNetworkOutage(_, sender) => sender.send(self.conn.is_outage()),
            UpstreamRelay::SendRequest(request) => self.enqueue(request).await,
        }
    }

    fn execute(&self, mut request: Box<dyn UpstreamRequest>) {
        let client = self.client.clone();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let result = client.send(request.as_mut()).await;

            let status = match result {
                Err(ref err) if err.is_network_error() => RequestStatus::Dropped,
                _ => RequestStatus::Completed,
            };

            match status {
                RequestStatus::Dropped if request.retry() => {
                    // attempt += 1; // TODO(ja)
                    // TODO(ja): Sent to `handle_result`.
                    action_tx.send(Action::Retry(request)).ok();
                }
                _ => request.respond(result).await,
            }

            // TODO(ja): Sent to `handle_result`.
            action_tx.send(Action::Complete(status)).ok();
        });
    }

    fn complete(&mut self, status: RequestStatus) {
        self.permits += 1;

        match status {
            RequestStatus::Dropped => self.conn.notify_error(self.action_tx.clone()),
            RequestStatus::Completed => self.conn.reset_error(),
        }
    }

    fn handle_action(&mut self, action: Action) {
        match action {
            Action::Retry(request) => self.queue.place_back(request),
            Action::Complete(status) => self.complete(status),
            Action::Connected => self.conn.reset_error(),
            Action::UpdateAuth(state) => self.auth_state = state,
        }
    }
}

#[derive(Debug)]
pub struct UpstreamRelayService {
    config: Arc<Config>,
}

impl UpstreamRelayService {
    /// Creates a new `UpstreamRelay` instance.
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

impl Service for UpstreamRelayService {
    type Interface = UpstreamRelay;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self { config } = self;

        let client = Arc::new(SharedClient::build(config.clone()));

        // Channel for internal actions.
        let (action_tx, mut action_rx) = mpsc::unbounded_channel();

        // Spawn a background check for authentication, along with a handle for the broker.
        let auth = AuthMonitor {
            config: config.clone(),
            client: client.clone(),
            state: AuthState::Unknown,
            tx: action_tx.clone(),
        };

        tokio::spawn(auth.run());

        // Main broker.
        let mut broker = Broker {
            client: client.clone(),
            queue: UpstreamQueue::new(),
            auth_state: AuthState::init(&config),
            conn: ConnectionMonitor::new(config.clone(), client),
            permits: config.max_concurrent_requests(),
            action_tx,
        };

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    Some(action) = action_rx.recv() => broker.handle_action(action),
                    Some(request) = broker.next_request() => broker.execute(request),
                    Some(message) = rx.recv() => broker.handle_message(message).await,

                    else => break,
                }
            }
        });
    }
}
