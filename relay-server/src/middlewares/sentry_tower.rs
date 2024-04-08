use std::convert::TryInto;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http::{header, uri, Request, Response, StatusCode};
use relay_log::sentry::{self, protocol, Hub, SentryFutureExt};
use tower::{Layer, Service};

/// Tower Layer that logs Http Request Headers.
///
/// The Service created by this Layer can also optionally start a new
/// performance monitoring transaction for each incoming request,
/// continuing the trace based on incoming distributed tracing headers.
///
/// The created transaction will automatically use the request URI as its name.
/// This is sometimes not desirable in case the request URI contains unique IDs
/// or similar. In this case, users should manually override the transaction name
/// in the request handler using the [`Scope::set_transaction`](sentry::Scope::set_transaction)
/// method.
#[derive(Clone, Default)]
pub struct SentryHttpLayer {
    start_transaction: bool,
}

impl SentryHttpLayer {
    /// Creates a new Layer which starts a new performance monitoring transaction
    /// for each incoming request.
    pub fn with_transaction() -> Self {
        Self {
            start_transaction: true,
        }
    }
}

/// Tower Service that logs Http Request Headers.
///
/// The Service can also optionally start a new performance monitoring transaction
/// for each incoming request, continuing the trace based on incoming
/// distributed tracing headers.
#[derive(Clone)]
pub struct SentryHttpService<S> {
    service: S,
    start_transaction: bool,
}

impl<S> Layer<S> for SentryHttpLayer {
    type Service = SentryHttpService<S>;

    fn layer(&self, service: S) -> Self::Service {
        Self::Service {
            service,
            start_transaction: self.start_transaction,
        }
    }
}

pin_project_lite::pin_project! {
    /// The Future returned from [`SentryHttpService`].
    pub struct SentryHttpFuture<F> {
        on_first_poll: Option<(protocol::Request, Option<sentry::TransactionContext>)>,
        transaction: Option<(
            sentry::TransactionOrSpan,
            Option<sentry::TransactionOrSpan>,
        )>,
        #[pin]
        future: F,
    }
}

impl<F, ResBody, Error> Future for SentryHttpFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, Error>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slf = self.project();
        if let Some((sentry_req, trx_ctx)) = slf.on_first_poll.take() {
            relay_log::configure_scope(|scope| {
                if let Some(trx_ctx) = trx_ctx {
                    let transaction: sentry::TransactionOrSpan =
                        sentry::start_transaction(trx_ctx).into();
                    transaction.set_request(sentry_req.clone());
                    let parent_span = scope.get_span();
                    scope.set_span(Some(transaction.clone()));
                    *slf.transaction = Some((transaction, parent_span));
                }

                scope.add_event_processor(move |mut event| {
                    if event.request.is_none() {
                        event.request = Some(sentry_req.clone());
                    }
                    Some(event)
                });
            });
        }
        match slf.future.poll(cx) {
            Poll::Ready(res) => {
                if let Some((transaction, parent_span)) = slf.transaction.take() {
                    if transaction.get_status().is_none() {
                        let status = match &res {
                            Ok(res) => map_status(res.status()),
                            Err(_) => protocol::SpanStatus::UnknownError,
                        };
                        transaction.set_status(status);
                    }
                    transaction.finish();
                    relay_log::configure_scope(|scope| scope.set_span(parent_span));
                }
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for SentryHttpService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = SentryHttpFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let sentry_req = protocol::Request {
            method: Some(request.method().to_string()),
            url: get_url_from_request(&request),
            headers: request
                .headers()
                .into_iter()
                .map(|(header, value)| {
                    (
                        header.to_string(),
                        value.to_str().unwrap_or_default().into(),
                    )
                })
                .collect(),
            ..Default::default()
        };
        let trx_ctx = if self.start_transaction {
            let headers = request.headers().into_iter().flat_map(|(header, value)| {
                value.to_str().ok().map(|value| (header.as_str(), value))
            });
            let tx_name = format!("{} {}", request.method(), path_from_request(&request));
            Some(sentry::TransactionContext::continue_from_headers(
                &tx_name,
                "http.server",
                headers,
            ))
        } else {
            None
        };

        SentryHttpFuture {
            on_first_poll: Some((sentry_req, trx_ctx)),
            transaction: None,
            future: self.service.call(request),
        }
    }
}

fn path_from_request<B>(request: &Request<B>) -> &str {
    if let Some(matched_path) = request.extensions().get::<axum::extract::MatchedPath>() {
        return matched_path.as_str();
    }

    request.uri().path()
}

fn map_status(status: StatusCode) -> protocol::SpanStatus {
    match status {
        StatusCode::UNAUTHORIZED => protocol::SpanStatus::Unauthenticated,
        StatusCode::FORBIDDEN => protocol::SpanStatus::PermissionDenied,
        StatusCode::NOT_FOUND => protocol::SpanStatus::NotFound,
        StatusCode::TOO_MANY_REQUESTS => protocol::SpanStatus::ResourceExhausted,
        status if status.is_client_error() => protocol::SpanStatus::InvalidArgument,
        StatusCode::NOT_IMPLEMENTED => protocol::SpanStatus::Unimplemented,
        StatusCode::SERVICE_UNAVAILABLE => protocol::SpanStatus::Unavailable,
        status if status.is_server_error() => protocol::SpanStatus::InternalError,
        StatusCode::CONFLICT => protocol::SpanStatus::AlreadyExists,
        status if status.is_success() => protocol::SpanStatus::Ok,
        _ => protocol::SpanStatus::UnknownError,
    }
}

fn get_url_from_request<B>(request: &Request<B>) -> Option<url::Url> {
    let uri = request.uri().clone();
    let mut uri_parts = uri.into_parts();
    uri_parts.scheme.get_or_insert(uri::Scheme::HTTP);
    if uri_parts.authority.is_none() {
        let host = request.headers().get(header::HOST)?.as_bytes();
        uri_parts.authority = Some(host.try_into().ok()?);
    }
    let uri = uri::Uri::from_parts(uri_parts).ok()?;
    uri.to_string().parse().ok()
}

/// Provides a hub for each request
pub trait HubProvider<H, Request>
where
    H: Into<Arc<Hub>>,
{
    /// Returns a hub to be bound to the request
    fn hub(&self, request: &Request) -> H;
}

impl<H, F, Request> HubProvider<H, Request> for F
where
    F: Fn(&Request) -> H,
    H: Into<Arc<Hub>>,
{
    fn hub(&self, request: &Request) -> H {
        (self)(request)
    }
}

impl<Request> HubProvider<Arc<Hub>, Request> for Arc<Hub> {
    fn hub(&self, _request: &Request) -> Arc<Hub> {
        self.clone()
    }
}

/// Provides a new hub made from the currently active hub for each request
#[derive(Clone, Copy)]
pub struct NewFromTopProvider;

impl<Request> HubProvider<Arc<Hub>, Request> for NewFromTopProvider {
    fn hub(&self, _request: &Request) -> Arc<Hub> {
        // The Clippy lint here is a false positive, the suggestion to write
        // `Hub::with(Hub::new_from_top)` does not compiles:
        //     143 |         Hub::with(Hub::new_from_top).into()
        //         |         ^^^^^^^^^ implementation of `std::ops::FnOnce` is not general enough
        #[allow(clippy::redundant_closure)]
        Hub::with(|hub| Hub::new_from_top(hub)).into()
    }
}

/// Tower layer that binds a specific Sentry hub for each request made.
pub struct SentryLayer<P, H, Request>
where
    P: HubProvider<H, Request>,
    H: Into<Arc<Hub>>,
{
    provider: P,
    _hub: PhantomData<(H, Request)>,
}

impl<S, P, H, Request> Layer<S> for SentryLayer<P, H, Request>
where
    P: HubProvider<H, Request> + Clone,
    H: Into<Arc<Hub>>,
{
    type Service = SentryService<S, P, H, Request>;

    fn layer(&self, service: S) -> Self::Service {
        SentryService {
            service,
            provider: self.provider.clone(),
            _hub: PhantomData,
        }
    }
}

impl<P, H, Request> Clone for SentryLayer<P, H, Request>
where
    P: HubProvider<H, Request> + Clone,
    H: Into<Arc<Hub>>,
{
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            _hub: PhantomData,
        }
    }
}

/// Tower service that binds a specific Sentry hub for each request made.
pub struct SentryService<S, P, H, Request>
where
    P: HubProvider<H, Request>,
    H: Into<Arc<Hub>>,
{
    service: S,
    provider: P,
    _hub: PhantomData<(H, Request)>,
}

impl<S, Request, P, H> Service<Request> for SentryService<S, P, H, Request>
where
    S: Service<Request>,
    P: HubProvider<H, Request>,
    H: Into<Arc<Hub>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = sentry::SentryFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let hub = self.provider.hub(&request).into();
        let fut = Hub::run(hub.clone(), || self.service.call(request));
        fut.bind_hub(hub)
    }
}

impl<S, P, H, Request> Clone for SentryService<S, P, H, Request>
where
    S: Clone,
    P: HubProvider<H, Request> + Clone,
    H: Into<Arc<Hub>>,
{
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            provider: self.provider.clone(),
            _hub: PhantomData,
        }
    }
}

/// Tower layer that binds a new Sentry hub for each request made
pub type NewSentryLayer<Request> = SentryLayer<NewFromTopProvider, Arc<Hub>, Request>;

impl<Request> NewSentryLayer<Request> {
    /// Create a new Sentry layer that binds a new Sentry hub for each request made
    pub fn new_from_top() -> Self {
        Self {
            provider: NewFromTopProvider,
            _hub: PhantomData,
        }
    }
}
