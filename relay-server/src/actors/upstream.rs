use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;

use itertools::Itertools;
use relay_common::RetryBackoff;
use reqwest::header;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{mpsc, watch, Notify, Semaphore};
use tokio::time::Instant;

use relay_config::Config;
use relay_quotas::{
    DataCategories, QuotaScope, RateLimit, RateLimitScope, RateLimits, RetryAfter, Scoping,
};
use relay_system::{Addr, AsyncResponse, FromMessage, Interface, NoResponse, Sender, Service};

use crate::http::{HttpError, Request, RequestBuilder, Response, StatusCode};
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

    // /// Called whenever the request will be send over HTTP (possible multiple times)
    fn build(&self, builder: RequestBuilder) -> Result<Request, HttpError>;

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

/// TODO(ja): Doc
#[derive(Debug)]
pub struct SendQuery<T: UpstreamQuery>(pub T);

/// TODO(ja): Doc
#[derive(Debug)]
pub enum UpstreamRelay {
    IsAuthenticated(IsAuthenticated, Sender<bool>),
    IsNetworkOutage(IsNetworkOutage, Sender<bool>),
    SendRequest(Box<dyn UpstreamRequest>),
    SendQuery(),
}

impl UpstreamRelay {
    pub fn from_registry() -> Addr<Self> {
        todo!()
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

impl<T: UpstreamQuery> FromMessage<SendQuery<T>> for UpstreamRelay {
    type Response = AsyncResponse<Result<T::Response, UpstreamRequestError>>;

    fn from_message(
        message: SendQuery<T>,
        _: Sender<Result<T::Response, UpstreamRequestError>>,
    ) -> Self {
        todo!()
    }
}

/// TODO(ja): Doc
/// TODO(ja): Name
#[derive(Debug)]
struct Dispatch {
    tx_high: mpsc::UnboundedSender<Box<dyn UpstreamRequest>>,
    tx_low: mpsc::UnboundedSender<Box<dyn UpstreamRequest>>,
    outage: OutageHandle,
}

impl Dispatch {
    fn enqueue_request(&mut self, request: Box<dyn UpstreamRequest>) {
        let priority = request.priority();

        match priority {
            RequestPriority::Immediate => todo!(),
            RequestPriority::High => self.tx_high.send(request).ok(),
            RequestPriority::Low => self.tx_low.send(request).ok(),
        };

        // TODO(ja): Make this a gauge
        // TODO(ja): Measure queue saturation
        relay_statsd::metric!(
            histogram(RelayHistograms::UpstreamMessageQueueSize) = 0u64,
            priority = priority.name(),
        );
    }

    fn handle_message(&mut self, message: UpstreamRelay) {
        match message {
            UpstreamRelay::IsAuthenticated(_, sender) => sender.send(todo!()),
            UpstreamRelay::IsNetworkOutage(_, sender) => sender.send(self.outage.is_active()),
            UpstreamRelay::SendRequest(request) => self.enqueue_request(request),
            UpstreamRelay::SendQuery() => todo!(),
        }
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
        | Err(UpstreamRequestError::Http(HttpError::Overflow)) => {
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
        RequestPriority::Immediate // TODO(ja): This is ugly since we never use this.
    }

    fn set_relay_id(&self) -> bool {
        true
    }

    fn intercept_status_errors(&self) -> bool {
        true
    }

    fn build(&self, builder: RequestBuilder) -> Result<Request, HttpError> {
        builder.finish()
    }

    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async {})
    }
}

/// TODO(ja): Doc
#[derive(Clone, Debug)]
struct OutageHandle {
    rx: watch::Receiver<bool>,
    notify: Arc<Notify>,
}

impl OutageHandle {
    /// Returns `true` if the upstream is in an outage situation.
    pub fn is_active(&self) -> bool {
        *self.rx.borrow()
    }

    /// Waits until the outage is resolved.
    ///
    /// If there is no outage, this resolves immediately. Otherwise, if an outage has been
    /// [notified](Self::notify), this will block until the outage monitor has reestablished
    /// connection.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn resolved(&mut self) {
        while self.is_active() {
            if self.rx.changed().await.is_err() {
                return; // Return if the channel has closed to prevent suspending indefinitely
                        // TODO(ja): Surface `Result` in public signature?
            }
        }
    }

    /// Notify the outage monitor of a network outage.
    ///
    /// This will set the upstream into outage state. Subsequent calls to `is_active` will return
    /// `true`, and the outage monitor will start to reestablish a connection with the upstream.
    /// Calls to `resolved` will now block until the connection is established.
    pub fn notify(&self) {
        // Do not use `notify_one` here, since that would store a permit for the next call to
        // `notify()` in the monitor. However, we need to ensure that the monitor only starts
        // watching after it is done with a healthcheck.
        self.notify.notify_waiters();
    }
}

/// TODO(ja): Doc
struct OutageMonitor {
    backoff: RetryBackoff,
    status: watch::Sender<bool>,
    notify: Arc<Notify>,
    client: Arc<SharedClient>,
}

impl OutageMonitor {
    async fn connect(&mut self) {
        self.backoff.reset();

        loop {
            let next_backoff = self.backoff.next_backoff();
            relay_log::warn!(
                "Network outage, scheduling another check in {:?}",
                next_backoff
            );

            tokio::time::sleep(next_backoff).await;
            match self.client.send(&GetHealthCheck).await {
                Ok(_) => return,
                Err(e) if !e.is_network_error() => return,
                Err(_) => continue,
            }
        }
    }

    pub async fn run(mut self) {
        loop {
            // Obtain the notify first to avoid a data race. As soon as this notify is created, it
            // will capture `notify_waiting`. After this, signal waiting receivers to continue with
            // new requests by setting the outage status to `false`.
            let notify = self.notify.notified();
            self.status.send(false).ok(); // TODO(ja): Bail on error, since nobody is listening anymore

            notify.await;
            self.status.send(true).ok();

            self.connect().await;
            relay_log::info!("Recovering from network outage.")
        }
    }
}

#[derive(Debug)]
struct SharedClient {
    config: Arc<Config>,
    reqwest: reqwest::Client,
    outage: OutageHandle,
    first_error: Option<Instant>,
}

impl SharedClient {
    fn build(config: Arc<Config>, outage: OutageHandle) -> Self {
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

        Self {
            config,
            reqwest,
            outage,
            first_error: None,
        }
    }

    /// TODO(ja): Doc
    fn build_request(
        &self,
        request: &dyn UpstreamRequest,
    ) -> Result<reqwest::Request, UpstreamRequestError> {
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
        // TODO(ja): All these headers can be prepared and cloned
        builder.header("Host", host_header.as_bytes());

        if request.set_relay_id() {
            if let Some(credentials) = self.config.credentials() {
                builder.header("X-Sentry-Relay-Id", credentials.id.to_string());
            }
        }

        match request.build(builder) {
            Ok(Request(client_request)) => Ok(client_request),
            Err(e) => Err(UpstreamRequestError::Http(e)),
        }
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
    /// TODO(ja): Name
    async fn send_once(
        &self,
        request: &dyn UpstreamRequest,
    ) -> Result<Response, UpstreamRequestError> {
        let client_request = self.build_request(request)?;
        let response = self.reqwest.execute(client_request).await?;
        self.transform_response(request, Response(response)).await
    }

    /// Records an occurrence of a network error.
    ///
    /// If the network errors persist throughout the http outage grace period, an outage is
    /// triggered, which results in halting all network requests and starting a reconnect loop.
    fn handle_network_error(&self) {
        // TODO(ja): Debounce this
        // let now = Instant::now();
        // let first_error = *self.first_error.get_or_insert(now);

        // // Only take action if we exceeded the grace period.
        // if now > first_error + self.config.http_outage_grace_period() {
        self.outage.notify();
        // }
    }

    /// Called when a message to the upstream goes through without a network error.
    fn reset_network_error(&self) {
        // self.first_error = None;
    }

    /// TODO(ja): Doc
    pub async fn send(
        &self,
        request: &dyn UpstreamRequest,
    ) -> Result<Response, UpstreamRequestError> {
        let mut attempt = 0;

        // TODO(ja): There is one regression: low-prio requests can stall high-prio reqs. Fine?
        loop {
            let send_start = Instant::now();
            let result = self.send_once(request).await;
            emit_response_metrics(send_start, request, &result, attempt);

            if matches!(result, Err(ref err) if err.is_network_error()) {
                self.handle_network_error();

                if request.retry() {
                    attempt += 1;
                    continue; // TODO(ja): This shouldn't loop if we're in outage state
                }
            } else {
                // we managed a request without a network error, reset the first time we got a
                // network error and resume sending events.
                self.reset_network_error();
            }

            return result;
        }
    }
}

/// TODO(ja): Doc
/// TODO(ja): Name
#[derive(Debug)]
struct Broker {
    rx_high: mpsc::UnboundedReceiver<Box<dyn UpstreamRequest>>,
    rx_low: mpsc::UnboundedReceiver<Box<dyn UpstreamRequest>>,
    semaphore: Arc<Semaphore>,
    client: Arc<SharedClient>,
    outage: OutageHandle,
}

impl Broker {
    async fn run(mut self) {
        loop {
            let permit = self.semaphore.clone().acquire_owned();

            // TODO(ja): Consider placement of this:
            //  - if we check it here, we might have a new outage by the time a new request comes
            //    in. That's unlikely though.
            //  - if we check it below, that means we spawn more tasks that will just be waiting
            //    instead of leaving requests in the queue.
            //  - the outage should be checked somewhere in the request loop too.
            self.outage.resolved().await;

            let request = tokio::select! {
                biased;

                Some(request) = self.rx_high.recv() => request,
                Some(request) = self.rx_low.recv() => request,
            };

            let client = self.client.clone();
            tokio::spawn(async move {
                let result = client.send(request.as_ref()).await;
                request.respond(result).await;
                drop(permit);
            });
        }
    }
}

/// TODO(ja): Doc
pub struct UpstreamRelayService {
    dispatch: Dispatch,
    broker: Broker,
    outage_monitor: OutageMonitor,
}

impl UpstreamRelayService {
    /// Creates a new `UpstreamRelay` instance.
    pub fn new(config: Arc<Config>) -> Self {
        let (outage_tx, outage_rx) = watch::channel(false);
        let outage_notify = Arc::new(Notify::new());
        let outage_handle = OutageHandle {
            rx: outage_rx,
            notify: outage_notify.clone(),
        };

        let client = Arc::new(SharedClient::build(config.clone(), outage_handle.clone()));

        let outage_monitor = OutageMonitor {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            status: outage_tx,
            notify: outage_notify,
            client: client.clone(),
        };

        let (tx_high, rx_high) = mpsc::unbounded_channel();
        let (tx_low, rx_low) = mpsc::unbounded_channel();

        let dispatch = Dispatch {
            tx_high,
            tx_low,
            outage: outage_handle.clone(),
        };

        let broker = Broker {
            rx_high,
            rx_low,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_requests())),
            client,
            outage: outage_handle,
        };

        Self {
            dispatch,
            broker,
            outage_monitor,
        }
    }
}

impl Service for UpstreamRelayService {
    type Interface = UpstreamRelay;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            mut dispatch,
            broker,
            outage_monitor,
        } = self;

        tokio::spawn(async move { outage_monitor.run().await });
        tokio::spawn(async move { broker.run().await });

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                dispatch.handle_message(message);
            }
        });
    }
}
