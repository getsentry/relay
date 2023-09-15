//! Service to communicate with the upstream.
//!
//! Most importantly, this module declares the [`UpstreamRelay`] service and its main implementation
//! [`UpstreamRelayService`] along with messages to communicate with the service. Please look at
//! service-level docs for more information.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use relay_auth::{RegisterChallenge, RegisterRequest, RegisterResponse, Registration};
use relay_config::{Config, Credentials, RelayMode};
use relay_quotas::{
    DataCategories, QuotaScope, RateLimit, RateLimitScope, RateLimits, ReasonCode, RetryAfter,
    Scoping,
};
use relay_system::{
    AsyncResponse, FromMessage, Interface, MessageResponse, NoResponse, Sender, Service,
};
use reqwest::header;
pub use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::http::{HttpError, Request, RequestBuilder, Response, StatusCode};
use crate::statsd::{RelayHistograms, RelayTimers};
use crate::utils::{self, ApiErrorResponse, RelayErrorAction, RetryBackoff};

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
        // One example of such a generic rate limit is the anti-abuse nginx layer used by SaaS.
        if !rate_limits.is_limited() {
            rate_limits.add(RateLimit {
                categories: DataCategories::new(),
                scope: RateLimitScope::for_quota(scoping, QuotaScope::Key),
                reason_code: Some(ReasonCode::new("generic")),
                retry_after: self.retry_after,
            });
        }
        rate_limits
    }
}

/// An error returned from [`SendRequest`] and [`SendQuery`].
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
    /// Returns the status code of the HTTP request sent to the upstream.
    ///
    /// If this error is the result of sending a request to the upstream, this method returns `Some`
    /// with the status code. If the request could not be made or the error originates elsewhere,
    /// this returns `None`.
    fn status_code(&self) -> Option<StatusCode> {
        match self {
            UpstreamRequestError::ResponseError(code, _) => Some(*code),
            UpstreamRequestError::Http(HttpError::Reqwest(e)) => e.status(),
            _ => None,
        }
    }

    /// Returns `true` if the error indicates a network downtime.
    fn is_network_error(&self) -> bool {
        match self {
            Self::SendFailed(_) => true,
            Self::ResponseError(code, _) => matches!(code.as_u16(), 502..=504),
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

    /// Returns a categorized description of the error.
    ///
    /// This is used for metrics and logging.
    fn description(&self) -> &'static str {
        match self {
            UpstreamRequestError::NoCredentials => "credentials",
            UpstreamRequestError::SendFailed(_) => "send_failed",
            UpstreamRequestError::Http(HttpError::Io(_)) => "payload_failed",
            UpstreamRequestError::Http(HttpError::Json(_)) => "invalid_json",
            UpstreamRequestError::Http(HttpError::Reqwest(_)) => "reqwest_error",
            UpstreamRequestError::Http(HttpError::Overflow) => "overflow",
            UpstreamRequestError::Http(HttpError::NoCredentials) => "no_credentials",
            UpstreamRequestError::RateLimited(_) => "rate_limited",
            UpstreamRequestError::ResponseError(_, _) => "response_error",
            UpstreamRequestError::ChannelClosed => "channel_closed",
            UpstreamRequestError::AuthDenied => "auth_denied",
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

/// Checks the authentication state with the upstream.
///
/// In static and proxy mode, Relay does not require authentication and `IsAuthenticated` always
/// returns `true`. Otherwise, this message retrieves the current state of authentication:
///
/// - Initially, Relay is unauthenticated until it has established connection.
/// - If this Relay is not known by the upstream, it remains unauthenticated indefinitely.
/// - Once Relay has registered, this message reports `true`.
/// - In periodic intervals Relay re-authenticates, which may drop authentication temporarily.
#[derive(Debug)]
pub struct IsAuthenticated;

/// Returns whether Relay is in an outage state.
///
/// On repeated failure to submit requests to the upstream, the upstream service moves into an
/// outage state. During this phase, no requests or retries are performed and all newly submitted
/// [`SendRequest`] and [`SendQuery`] messages are put into the queue. Once connection is
/// reestablished, requests resume in FIFO order.
///
/// This message resolves to `true` if Relay is in outage mode and `false` if the service is
/// performing regular operation.
#[derive(Debug)]
pub struct IsNetworkOutage;

/// Priority of an upstream request.
///
/// See [`UpstreamRequest::priority`] for more information.
#[derive(Clone, Copy, Debug)]
pub enum RequestPriority {
    /// High priority, low volume messages (e.g. ProjectConfig, ProjectStates, Registration messages).
    High,
    /// Low priority, high volume messages (e.g. Events and Outcomes).
    Low,
}

impl RequestPriority {
    /// The name of the priority for logging and metrics.
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

/// Represents a generic HTTP request to be sent to the upstream.
pub trait UpstreamRequest: Send + Sync + fmt::Debug {
    /// The HTTP method of the request.
    fn method(&self) -> Method;

    /// The path relative to the upstream.
    fn path(&self) -> Cow<'_, str>;

    /// Whether this request should retry on network errors.
    ///
    /// Defaults to `true` and should be disabled if there is an external retry mechanism. Note that
    /// failures other than network errors will **not** be retried.
    fn retry(&self) -> bool {
        true
    }

    /// The queueing priority of the request.
    ///
    ///  - High priority requests are always sent and retried first.
    ///  - Low priority requests are sent if no high-priority messages are pending in the queue.
    ///    This also applies to retries: A low-priority message is only sent if there are no
    ///    high-priority requests waiting.
    ///
    /// Within the same priority, requests are delivered in FIFO order.
    ///
    /// Defaults to [`Low`](RequestPriority::Low).
    fn priority(&self) -> RequestPriority {
        RequestPriority::Low
    }

    /// Controls whether request errors should be intercepted.
    ///
    /// By default, error codes from responses will be intercepted and returned as
    /// [`UpstreamRequestError`]. This also includes parsing of the request body for diagnostics.
    /// Return `false` to disable this behavior and receive the verbatim response.
    fn intercept_status_errors(&self) -> bool {
        true
    }

    /// Add the `X-Sentry-Relay-Id` header to the outgoing request.
    ///
    /// This header is used for authentication with the upstream and should be enabled only for
    /// endpoints that require it.
    ///
    /// Defaults to `true`.
    fn set_relay_id(&self) -> bool {
        true
    }

    /// Returns the name of the logical route.
    ///
    /// This is used for internal metrics and logging. Other than the path, this cannot contain
    /// dynamic elements and should be globally unique.
    fn route(&self) -> &'static str;

    /// Callback to build the outgoing web request.
    ///
    /// This callback populates the initialized request with headers and a request body. To send an
    /// empty request without additional headers, call [`RequestBuilder::finish`] directly.
    ///
    /// Note that this function can be called repeatedly if [`retry`](UpstreamRequest::retry)
    /// returns `true`. This function should therefore not move out of the request struct, but can
    /// use it to memoize heavy computation.
    fn build(&mut self, config: &Config, builder: RequestBuilder) -> Result<Request, HttpError>;

    /// Callback to complete an HTTP request.
    ///
    /// This callback receives the response or error. At time of invocation, the response body has
    /// not been consumed. The response body or derived information can then be sent into a channel.
    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>;
}

/// Sends a [request](UpstreamRequest) to the upstream and resolves the response.
///
/// This message is fire-and-forget. The result of sending the request is passed to the
/// [`UpstreamRequest::respond`] method, which can be used to process it and send it to a dedicated
/// channel.
#[derive(Debug)]
pub struct SendRequest<T: UpstreamRequest>(pub T);

/// Higher-level version of an [`UpstreamRequest`] with JSON request and response payloads.
///
/// The struct that implements the `UpstreamQuery` type has to be serializable and will be used as
/// JSON request body. The response body struct is declared via the associated `Response` type.
pub trait UpstreamQuery: Serialize + Send + Sync + fmt::Debug {
    /// The response type that will be deserialized from successful queries.
    type Response: DeserializeOwned + Send;

    /// The HTTP method of the query.
    fn method(&self) -> Method;

    /// The path relative to the upstream.
    fn path(&self) -> Cow<'static, str>;

    /// Whether this query should retry on network errors.
    ///
    /// This should be disabled if there is an external retry mechnism. Note that failures other
    /// than network errors will **not** be retried.
    fn retry() -> bool;

    /// The queueing priority of the query.
    ///
    ///  - High priority queries are always sent and retried first.
    ///  - Low priority queries are sent if no high-priority messages are pending in the queue.
    ///    This also applies to retries: A low-priority message is only sent if there are no
    ///    high-priority requests waiting.
    ///
    /// Within the same priority, queries are delivered in FIFO order.
    ///
    /// Defaults to [`Low`](RequestPriority::Low).
    fn priority() -> RequestPriority {
        RequestPriority::Low
    }

    /// Returns the name of the logical route.
    ///
    /// This is used for internal metrics and logging. Other than the path, this cannot contain
    /// dynamic elements and should be globally unique.
    fn route(&self) -> &'static str;
}

/// Transmitting end of the return channel for [`UpstreamQuery`].
type QuerySender<T> = Sender<Result<<T as UpstreamQuery>::Response, UpstreamRequestError>>;

/// Memoized implementation of [`UpstreamRequest`] for an [`UpstreamQuery`].
///
/// This can be used to send queries as requests to the upstream. The request wraps an internal
/// channel to send responses to.
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
    /// Wraps the given `query` in an [`UpstreamQuery`] implementation.
    pub fn new(query: T, sender: QuerySender<T>) -> Self {
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

    fn route(&self) -> &'static str {
        self.query.route()
    }

    fn build(&mut self, config: &Config, builder: RequestBuilder) -> Result<Request, HttpError> {
        // Memoize the serialized body and signature for retries.
        let credentials = config.credentials().ok_or(HttpError::NoCredentials)?;
        let (body, signature) = self
            .compiled
            .get_or_insert_with(|| credentials.secret_key.pack(&self.query));

        // This config attribute is needed during `respond`, which does not have access to the
        // config. For this reason, we need to store it on the request struct.
        self.max_response_size = config.max_api_payload_size();

        relay_statsd::metric!(
            histogram(RelayHistograms::UpstreamQueryBodySize) = body.len() as u64
        );

        builder
            .header("X-Sentry-Relay-Signature", signature.as_bytes())
            .header(header::CONTENT_TYPE, b"application/json")
            .body(&body)
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

/// Sends a [query](UpstreamQuery) to the upstream and resolves the response.
///
/// The result of the query is resolved asynchronously as response to the message. The query will
/// still be performed even if the response is not awaited.
#[derive(Debug)]
pub struct SendQuery<T: UpstreamQuery>(pub T);

/// Communication with the upstream via HTTP.
///
/// This service can send two main types of requests to the upstream, which can in turn be a Relay
/// or the Sentry webserver:
///
///  - [`SendRequest`] sends a plain HTTP request to the upstream that can be configured and handled
///    freely. Request implementations specify their priority and whether they should be retried
///    automatically by the upstream service.
///  - [`SendQuery`] sends a higher-level request with a standardized JSON body and resolves to a
///    JSON response. The upstream service will automatically sign the message with its private key
///    for authentication.
///
/// The upstream is also responsible to maintain the connection with the upstream. There are two
/// main messages to inquire about the connection state:
///
///  - [`IsAuthenticated`]
///  - [`IsNetworkOutage`]
#[derive(Debug)]
pub enum UpstreamRelay {
    /// Checks the authentication state with the upstream.
    IsAuthenticated(IsAuthenticated, Sender<bool>),
    /// Returns whether Relay is in an outage state.
    IsNetworkOutage(IsNetworkOutage, Sender<bool>),
    /// Sends a [request](SendRequest) or [query](SendQuery) to the upstream.
    SendRequest(Box<dyn UpstreamRequest>),
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

/// Captures statsd metrics for a completed upstream request.
fn emit_response_metrics(
    send_start: Instant,
    entry: &Entry,
    send_result: &Result<Response, UpstreamRequestError>,
) {
    let description = match send_result {
        Ok(_) => "success",
        Err(e) => e.description(),
    };
    let status_code = match send_result {
        Ok(ref response) => Some(response.status()),
        Err(ref error) => error.status_code(),
    };
    let status_str = status_code.as_ref().map(|c| c.as_str()).unwrap_or("-");

    relay_statsd::metric!(
        timer(RelayTimers::UpstreamRequestsDuration) = send_start.elapsed(),
        result = description,
        status_code = status_str,
        route = entry.request.route(),
        retries = match entry.retries {
            0 => "0",
            1 => "1",
            2 => "2",
            3..=10 => "few",
            _ => "many",
        },
    );

    relay_statsd::metric!(
        histogram(RelayHistograms::UpstreamRetries) = entry.retries as u64,
        result = description,
        status_code = status_str,
        route = entry.request.route(),
    );
}

/// Checks the status of the network connection with the upstream server.
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

    fn route(&self) -> &'static str {
        "check_live"
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

    fn route(&self) -> &'static str {
        "challenge"
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

    fn route(&self) -> &'static str {
        "response"
    }
}

/// A shared, asynchronous client to build and execute requests.
///
/// The main way to send a request through this client is [`send`](Self::send).
///
/// This instance holds a shared reference internally and can be cloned directly, so it does not
/// have to be placed in an `Arc`.
#[derive(Debug, Clone)]
struct SharedClient {
    config: Arc<Config>,
    reqwest: reqwest::Client,
}

impl SharedClient {
    /// Creates a new `SharedClient` instance.
    pub fn build(config: Arc<Config>) -> Self {
        let reqwest = reqwest::ClientBuilder::new()
            .connect_timeout(config.http_connection_timeout())
            .timeout(config.http_timeout())
            // In the forward endpoint, this means that content negotiation is done twice, and the
            // response body is first decompressed by the client, then re-compressed by the server.
            .gzip(true)
            // Enables async resolver through the `trust-dns-resolver` crate, which uses an LRU cache for the resolved entries.
            // This helps to limit the amount of requests made to upstream DNS server (important
            // for K8s infrastructure).
            .trust_dns(true)
            .build()
            .unwrap();

        Self { config, reqwest }
    }

    /// Builds the request in a non-blocking fashion.
    ///
    /// This creates the request, adds internal headers, and invokes [`UpstreamRequest::build`]. The
    /// build is invoked in a non-blocking fashion internally, so it can be called from an
    /// asynchronous runtime.
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

            let mut builder = RequestBuilder::reqwest(self.reqwest.request(request.method(), url))
                .header("Host", host_header.as_bytes());

            if request.set_relay_id() {
                if let Some(credentials) = self.config.credentials() {
                    builder = builder.header("X-Sentry-Relay-Id", credentials.id.to_string());
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
    ///  2. `ResponseError` in all other cases, containing the status and details.
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

    /// Builds and sends a request to the upstream, returning either a response or the error.
    pub async fn send(
        &self,
        request: &mut dyn UpstreamRequest,
    ) -> Result<Response, UpstreamRequestError> {
        let client_request = self.build_request(request)?;
        let response = self.reqwest.execute(client_request).await?;
        self.transform_response(request, Response(response)).await
    }

    /// Convenience method to send a query to the upstream and await the result.
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

/// An upstream request enqueued in the [`UpstreamQueue`].
///
/// This is the primary type with which requests are passed around the service.
#[derive(Debug)]
struct Entry {
    /// The inner request.
    pub request: Box<dyn UpstreamRequest>,
    /// The number of retries.
    ///
    /// This starts with `0` and is incremented every time a request is placed back into the queue
    /// following a network error.
    pub retries: usize,
}

impl Entry {
    /// Creates a pristine queue `Entry`.
    pub fn new(request: Box<dyn UpstreamRequest>) -> Self {
        Self {
            request,
            retries: 0,
        }
    }
}

/// Queue utility for the [`UpstreamRelayService`].
///
/// Requests are queued and delivered according to their [`UpstreamRequest::priority`]. This queue
/// is synchronous and managed by the [`UpstreamBroker`].
#[derive(Debug)]
struct UpstreamQueue {
    /// High priority queue.
    high: VecDeque<Entry>,
    /// Low priority queue.
    low: VecDeque<Entry>,
    /// Low priority retry queue.
    retry_high: VecDeque<Entry>,
    /// Low priority retry queue.
    retry_low: VecDeque<Entry>,
    /// Retries should not be dequeued before this instant.
    ///
    /// This retry increments by a [constant factor][`Self::RETRY_AFTER`]
    /// instead of backoff, since it only kicks in for a short period of time
    /// before Relay gets into network outage mode (see [`IsNetworkOutage`]).
    next_retry: Instant,
}

impl UpstreamQueue {
    /// Time to wait before retrying another request from the retry queue.
    const RETRY_AFTER: Duration = Duration::from_millis(500);

    /// Creates an empty upstream queue.
    pub fn new() -> Self {
        Self {
            high: VecDeque::new(),
            low: VecDeque::new(),
            retry_high: VecDeque::new(),
            retry_low: VecDeque::new(),
            next_retry: Instant::now(),
        }
    }

    /// Returns the number of entries in the queue.
    pub fn len(&self) -> usize {
        self.high.len() + self.low.len() + self.retry_high.len() + self.retry_low.len()
    }

    /// Places an entry at the back of the queue.
    ///
    /// Since entries are dequeued in FIFO order, this entry will be dequeued
    /// last within its priority class; see
    /// [`dequeue`][`UpstreamQueue::dequeue`] for more details.
    pub fn enqueue(&mut self, entry: Entry) {
        let priority = entry.request.priority();
        match priority {
            RequestPriority::High => self.high.push_back(entry),
            RequestPriority::Low => self.low.push_back(entry),
        }
        relay_statsd::metric!(
            histogram(RelayHistograms::UpstreamMessageQueueSize) = self.len() as u64,
            priority = priority.name(),
            attempt = "first"
        );
    }

    /// Places an entry in the retry queue.
    ///
    /// If the entry is high priority it's placed at the front of the queue, or
    /// at the back if it's low priority. Entries in the retry queue are
    /// dequeued from front to back.
    ///
    /// It also schedules the next retry time, based on the retry back off. The
    /// retry queue is not dequeued until the next retry has elapsed.
    pub fn retry(&mut self, entry: Entry) {
        let priority = entry.request.priority();
        match priority {
            RequestPriority::High => self.retry_high.push_back(entry),
            RequestPriority::Low => self.retry_low.push_back(entry),
        };
        relay_statsd::metric!(
            histogram(RelayHistograms::UpstreamMessageQueueSize) = self.len() as u64,
            priority = priority.name(),
            attempt = "retry"
        );

        self.next_retry = Instant::now() + Self::RETRY_AFTER;
    }

    /// Dequeues the entry with highest priority.
    ///
    /// Highest priority entry is determined by (1) request priority and (2)
    /// retries first.
    pub fn dequeue(&mut self) -> Option<Entry> {
        let now = Instant::now();

        if !self.retry_high.is_empty() && self.next_retry <= now {
            return self.retry_high.pop_front();
        }
        if !self.high.is_empty() {
            return self.high.pop_front();
        }
        if !self.retry_low.is_empty() && self.next_retry <= now {
            return self.retry_low.pop_front();
        }
        self.low.pop_front()
    }

    /// Resets the retry, if started.
    pub fn retry_backoff_reset(&mut self) {
        let now = Instant::now();
        if self.next_retry > now {
            self.next_retry = now;
        }
    }
}

/// Possible authentication states for Relay.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum AuthState {
    /// Relay is not authenticated and authentication has not started.
    Unknown,

    /// Relay is not authenticated and authentication is in progress.
    Registering,

    /// The connection is healthy and authenticated in managed mode.
    ///
    /// This state is also used as default for Relays that do not require authentication based on
    /// their configuration.
    Registered,

    /// Relay is authenticated and renewing the registration lease. During this process, Relay
    /// remains authenticated, unless an error occurs.
    Renewing,

    /// Authentication has been permanently denied by the Upstream. Do not attempt to retry.
    Denied,
}

impl AuthState {
    /// Returns the initial `AuthState` based on configuration.
    ///
    /// - Relays in managed mode require authentication. The state is set to `AuthState::Unknown`.
    /// - Other Relays do not require authentication. The state is set to `AuthState::Registered`.
    pub fn init(config: &Config) -> Self {
        match config.relay_mode() {
            RelayMode::Managed => AuthState::Unknown,
            _ => AuthState::Registered,
        }
    }

    /// Returns `true` if the state is considered authenticated.
    pub fn is_authenticated(self) -> bool {
        matches!(self, AuthState::Registered | AuthState::Renewing)
    }
}

/// Indicates whether an request was sent to the upstream.
#[derive(Clone, Copy, Debug)]
enum RequestOutcome {
    /// The request was dropped due to a network outage.
    Dropped,
    /// The request was received by the upstream.
    ///
    /// This does not automatically mean that the request was successfully accepted. It could also
    /// have been rate limited or rejected as invalid.
    Received,
}

/// Internal message of the upstream's [`UpstreamBroker`].
///
/// These messages are used to serialize state mutations to the broker's internals. They are emitted
/// by the auth monitor, connection monitor, and internal tasks for request handling.
#[derive(Debug)]
enum Action {
    /// A dropped needs to be retried.
    ///
    /// The entry is placed on the front of the [`UpstreamQueue`].
    Retry(Entry),
    /// Notifies a request has been completed successfully to upstream.
    ///
    /// Successful requests reset the retry backoff.
    SuccessfulRequest,
    /// Notifies completion of a request with a given outcome.
    ///
    /// Dropped request that need retries will additionally invoke the [`Retry`](Self::Retry)
    /// action.
    Complete(RequestOutcome),
    /// Previously lost connection has been regained.
    ///
    /// This message is delivered to the [`ConnectionMonitor`] instance.
    Connected,
    /// The auth monitor indicates a change in the authentication state.
    ///
    /// The new auth state is mirrored in an internal field for immediate access.
    UpdateAuth(AuthState),
}

type ActionTx = mpsc::UnboundedSender<Action>;

/// Service that establishes and maintains authentication.
///
/// In regular intervals, the service checks for authentication and notifies the upstream if the key
/// is no longer valid. This allows Sentry to reject registered Relays during runtim without
/// restarts.
///
/// The monitor updates subscribers via the an `Action` channel of all changes to the authentication
/// states.
#[derive(Debug)]
struct AuthMonitor {
    config: Arc<Config>,
    client: SharedClient,
    state: AuthState,
    tx: ActionTx,
}

impl AuthMonitor {
    /// Returns the interval at which this Relay should renew authentication.
    ///
    /// Returns `Some` if authentication should be retried. Returns `None` if authentication is
    /// permanent.
    fn renew_auth_interval(&self) -> Option<std::time::Duration> {
        if self.config.processing_enabled() {
            // processing relays do NOT re-authenticate
            None
        } else {
            // only relays that have a configured auth-interval reauthenticate
            self.config.http_auth_interval()
        }
    }

    /// Updates the monitor's internal state and subscribers.
    fn send_state(&mut self, state: AuthState) -> Result<(), UpstreamRequestError> {
        self.state = state;
        self.tx
            .send(Action::UpdateAuth(state))
            .map_err(|_| UpstreamRequestError::ChannelClosed)
    }

    /// Performs a single authentication pass.
    ///
    /// Authentication consists of an initial request, a challenge, and a signed response including
    /// the challenge. Throughout this sequence, the auth monitor transitions the authentication
    /// state. If authentication succeeds, the state is set to [`AuthState::Registered`] at the end.
    ///
    /// If any of the requests fail, this method returns an `Err` and leaves the last authentication
    /// state in place.
    async fn authenticate(
        &mut self,
        credentials: &Credentials,
    ) -> Result<(), UpstreamRequestError> {
        relay_log::info!(
            descriptor = %self.config.upstream_descriptor(),
            "registering with upstream"
        );

        self.send_state(if self.state.is_authenticated() {
            AuthState::Renewing
        } else {
            AuthState::Registering
        })?;

        let request = RegisterRequest::new(&credentials.id, &credentials.public_key);
        let challenge = self.client.send_query(request).await?;
        relay_log::debug!(token = challenge.token(), "got register challenge");

        let response = challenge.into_response();
        relay_log::debug!("sending register challenge response");
        self.client.send_query(response).await?;

        relay_log::info!("relay successfully registered with upstream");
        self.send_state(AuthState::Registered)?;

        Ok(())
    }

    /// Starts the authentication monitor's cycle.
    ///
    /// Authentication starts immediately and then enters a loop of recurring reauthentication until
    /// one of the following conditions is met:
    ///
    ///  - Authentication is not required based on the Relay's mode configuration.
    ///  - The upstream responded with a permanent rejection (auth denied).
    ///  - All subscibers have shut down and the action channel is closed.
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
                    relay_log::error!(
                        error = &err as &dyn std::error::Error,
                        "authentication encountered error"
                    );

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
                    // authenticated, in case the state was `Renewing` before.
                    if !err.is_network_error() {
                        self.send_state(AuthState::Registering).ok();
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

/// Internal state of the [`ConnectionMonitor`].
#[derive(Debug)]
enum ConnectionState {
    /// The connection is healthy.
    Connected,

    /// Network errors have been observed during the grace period.
    ///
    /// The connection is still considered healthy and requests should be made to the upstream.
    Interrupted(Instant),

    /// The connection is interrupted and reconnection is in progress.
    ///
    /// If the task has finished, connection should be considered `Connected`.
    Reconnecting(tokio::task::JoinHandle<()>),
}

/// Maintains outage state of the connection to the upstream.
///
///  Use [`notify_error`](Self::notify_error) and [`reset_error`](Self::reset_error) to inform the
/// monitor of successful and failed requests. If errors persist throughout a grace period, the
/// monitor spawns a background task to re-establish connections. During this period,
/// [`is_stable`](Self::is_stable) returns `false` and no other requests should be made to the
/// upstream.
///
/// This state is synchronous and managed by the [`UpstreamBroker`].
#[derive(Debug)]
struct ConnectionMonitor {
    state: ConnectionState,
    client: SharedClient,
}

impl ConnectionMonitor {
    /// Creates a new `ConnectionMonitor` in connected state.
    pub fn new(client: SharedClient) -> Self {
        Self {
            state: ConnectionState::Connected,
            client,
        }
    }

    /// Resets `Reconnecting` if the connection task has completed.
    fn clean_state(&mut self) -> &ConnectionState {
        if let ConnectionState::Reconnecting(ref task) = self.state {
            if task.is_finished() {
                self.state = ConnectionState::Connected;
            }
        }

        &self.state
    }

    /// Returns `true` if the connection is not in outage state.
    pub fn is_stable(&mut self) -> bool {
        match self.clean_state() {
            ConnectionState::Connected => true,
            ConnectionState::Interrupted(_) => true,
            ConnectionState::Reconnecting(_) => false,
        }
    }

    /// Returns `true` if the connection is in outage state.
    pub fn is_outage(&mut self) -> bool {
        !self.is_stable()
    }

    /// Performs connection attempts with exponential backoff until successful.
    async fn connect(client: SharedClient, tx: ActionTx) {
        let mut backoff = RetryBackoff::new(client.config.http_max_retry_interval());

        loop {
            let next_backoff = backoff.next_backoff();
            relay_log::warn!("network outage, scheduling another check in {next_backoff:?}");

            tokio::time::sleep(next_backoff).await;
            match client.send(&mut GetHealthCheck).await {
                // All errors that are not connection errors are considered a successful attempt
                Err(e) if e.is_network_error() => continue,
                _ => break,
            }
        }

        tx.send(Action::Connected).ok();
    }

    /// Notifies the monitor of a request that resulted in a network error.
    ///
    /// This starts a grace period if not already started. If a prior grace period has been
    /// exceeded, the monitor spawns a background job to reestablish connection and notifies the
    /// given `return_tx` on success.
    ///
    /// This method does not block.
    pub fn notify_error(&mut self, return_tx: &ActionTx) {
        let now = Instant::now();

        let first_error = match self.clean_state() {
            ConnectionState::Connected => now,
            ConnectionState::Interrupted(first) => *first,
            ConnectionState::Reconnecting(_) => return,
        };

        self.state = ConnectionState::Interrupted(first_error);

        // Only take action if we exceeded the grace period.
        if first_error + self.client.config.http_outage_grace_period() <= now {
            let return_tx = return_tx.clone();
            let task = tokio::spawn(Self::connect(self.client.clone(), return_tx));
            self.state = ConnectionState::Reconnecting(task);
        }
    }

    /// Notifies the monitor of a request that was delivered to the upstream.
    ///
    /// Resets the outage grace period and aborts connect background tasks.
    pub fn reset_error(&mut self) {
        if let ConnectionState::Reconnecting(ref task) = self.state {
            task.abort();
        }

        self.state = ConnectionState::Connected;
    }
}

/// Main broker of the [`UpstreamRelayService`].
///
/// This handles incoming public messages, internal actions, and maintains the upstream queue.
#[derive(Debug)]
struct UpstreamBroker {
    client: SharedClient,
    queue: UpstreamQueue,
    auth_state: AuthState,
    conn: ConnectionMonitor,
    permits: usize,
    action_tx: ActionTx,
}

impl UpstreamBroker {
    /// Returns the next entry from the queue if the upstream is in a healthy state.
    ///
    /// This returns `None` in any of the following conditions:
    ///  - Maximum request concurrency has been reached. A slot will be reclaimed through
    ///    [`Action::Complete`].
    ///  - The connection is in outage state and all outgoing requests are suspended. Outage state
    ///    will be reset through [`Action::Connected`].
    ///  - Relay is not authenticated, including failed renewals. Auth state will be updated through
    ///    [`Action::UpdateAuth`].
    ///  - The request queue is empty. New requests will be added through [`SendRequest`] or
    ///    [`SendQuery`] in the main message loop.
    async fn next_request(&mut self) -> Option<Entry> {
        if self.permits == 0 || self.conn.is_outage() || !self.auth_state.is_authenticated() {
            return None;
        }

        let entry = self.queue.dequeue()?;
        self.permits -= 1;
        Some(entry)
    }

    /// Attempts to place a new request into the queue.
    ///
    /// If authentication is permanently denied, the request will be failed immediately. In all
    /// other cases, the request is enqueued and will wait for submission.
    async fn enqueue(&mut self, request: Box<dyn UpstreamRequest>) {
        if let AuthState::Denied = self.auth_state {
            // This respond is near-instant because it should just send the error into the request's
            // response channel. We do not expect that this blocks the broker.
            request.respond(Err(UpstreamRequestError::AuthDenied)).await;
        } else {
            self.queue.enqueue(Entry::new(request));
        }
    }

    /// Handler of the main message loop.
    async fn handle_message(&mut self, message: UpstreamRelay) {
        match message {
            UpstreamRelay::IsAuthenticated(_, sender) => {
                sender.send(self.auth_state.is_authenticated())
            }
            UpstreamRelay::IsNetworkOutage(_, sender) => sender.send(self.conn.is_outage()),
            UpstreamRelay::SendRequest(request) => self.enqueue(request).await,
        }
    }

    /// Spawns a request attempt.
    ///
    /// The request will run concurrently with other spawned requests and notify the action channel
    /// on completion.
    fn execute(&self, mut entry: Entry) {
        let client = self.client.clone();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let send_start = Instant::now();
            let result = client.send(entry.request.as_mut()).await;
            emit_response_metrics(send_start, &entry, &result);

            let status = match result {
                Err(ref err) if err.is_network_error() => RequestOutcome::Dropped,
                _ => RequestOutcome::Received,
            };

            match status {
                RequestOutcome::Dropped if entry.request.retry() => {
                    entry.retries += 1;
                    action_tx.send(Action::Retry(entry)).ok();
                }
                _ => {
                    entry.request.respond(result).await;
                    action_tx.send(Action::SuccessfulRequest).ok();
                }
            }

            // Send an action back to the action channel of the broker, which will invoke
            // `handle_action`. This is to let the broker know in a synchronized fashion that the
            // request has finished and may need to be retried (above).
            action_tx.send(Action::Complete(status)).ok();
        });
    }

    /// Marks completion of a running request and reclaims its slot.
    fn complete(&mut self, status: RequestOutcome) {
        self.permits += 1;

        match status {
            RequestOutcome::Dropped => self.conn.notify_error(&self.action_tx),
            RequestOutcome::Received => self.conn.reset_error(),
        }
    }

    /// Handler of the internal action channel.
    fn handle_action(&mut self, action: Action) {
        match action {
            Action::Retry(request) => self.queue.retry(request),
            Action::SuccessfulRequest => self.queue.retry_backoff_reset(),
            Action::Complete(status) => self.complete(status),
            Action::Connected => self.conn.reset_error(),
            Action::UpdateAuth(state) => self.auth_state = state,
        }
    }
}

/// Implementation of the [`UpstreamRelay`] interface.
#[derive(Debug)]
pub struct UpstreamRelayService {
    config: Arc<Config>,
}

impl UpstreamRelayService {
    /// Creates a new `UpstreamRelay` instance.
    pub fn new(config: Arc<Config>) -> Self {
        // Broker and other actual components are implemented in the Service's `spawn_handler`.
        Self { config }
    }
}

impl Service for UpstreamRelayService {
    type Interface = UpstreamRelay;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self { config } = self;

        let client = SharedClient::build(config.clone());

        // Channel for serialized communication from the auth monitor, connection monitor, and
        // concurrent requests back to the broker.
        let (action_tx, mut action_rx) = mpsc::unbounded_channel();

        // Spawn a recurring background check for authentication. It terminates automatically if
        // authentication is not required or rejected.
        let auth = AuthMonitor {
            config: config.clone(),
            client: client.clone(),
            state: AuthState::Unknown,
            tx: action_tx.clone(),
        };
        tokio::spawn(auth.run());

        // Main broker that serializes public and internal messages, as well as maintains connection
        // and authentication state.
        let mut broker = UpstreamBroker {
            client: client.clone(),
            queue: UpstreamQueue::new(),
            auth_state: AuthState::init(&config),
            conn: ConnectionMonitor::new(client),
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
