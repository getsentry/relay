//! Server endpoint that proxies any request to the upstream.
//!
//! This endpoint will issue a client request to the upstream and append relay's own headers
//! (`X-Forwarded-For` and `Sentry-Relay-Id`). The response is then streamed back to the origin.

use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use axum::extract::{DefaultBodyLimit, Request};
use axum::handler::Handler;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use once_cell::sync::Lazy;
use relay_common::glob2::GlobMatcher;
use relay_config::Config;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

use crate::actors::upstream::{Method, SendRequest, UpstreamRequest, UpstreamRequestError};
use crate::extractors::ForwardedFor;
use crate::http::{HttpError, RequestBuilder, Response as UpstreamResponse};
use crate::service::ServiceState;

/// Headers that this endpoint must handle and cannot forward.
static HOP_BY_HOP_HEADERS: &[HeaderName] = &[
    header::CONNECTION,
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

/// Headers ignored in addition to the headers defined in `HOP_BY_HOP_HEADERS`.
static IGNORED_REQUEST_HEADERS: &[HeaderName] = &[
    header::HOST,
    header::CONTENT_ENCODING,
    header::CONTENT_LENGTH,
];

/// Root path of all API endpoints.
const API_PATH: &str = "/api/";

fn status_to_1(status: reqwest::StatusCode) -> StatusCode {
    StatusCode::from_u16(status.as_u16()).unwrap()
}

fn status_to_1_opt(status: Option<reqwest::StatusCode>) -> Option<StatusCode> {
    Some(StatusCode::from_u16(status?.as_u16()).unwrap())
}

/// A wrapper struct that allows conversion of UpstreamRequestError into a `dyn ResponseError`. The
/// conversion logic is really only acceptable for blindly forwarded requests.
#[derive(Debug, thiserror::Error)]
#[error("error while forwarding request: {0}")]
struct ForwardError(#[from] UpstreamRequestError);

impl From<RecvError> for ForwardError {
    fn from(_: RecvError) -> Self {
        Self(UpstreamRequestError::ChannelClosed)
    }
}

impl IntoResponse for ForwardError {
    fn into_response(self) -> Response {
        match &self.0 {
            UpstreamRequestError::Http(e) => match e {
                HttpError::Overflow => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
                HttpError::Reqwest(error) => {
                    relay_log::error!(error = error as &dyn Error);
                    status_to_1_opt(error.status())
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
                        .into_response()
                }
                HttpError::Io(_) => StatusCode::BAD_GATEWAY.into_response(),
                HttpError::Json(_) => StatusCode::BAD_REQUEST.into_response(),
                HttpError::NoCredentials => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            },
            UpstreamRequestError::SendFailed(e) => {
                if e.is_timeout() {
                    StatusCode::GATEWAY_TIMEOUT.into_response()
                } else {
                    StatusCode::BAD_GATEWAY.into_response()
                }
            }
            error => {
                // should all be unreachable
                relay_log::error!(error = error as &dyn Error, "unreachable code");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

type ForwardResponse = (StatusCode, HeaderMap<HeaderValue>, Vec<u8>);

struct ForwardRequest {
    method: Method,
    path: String,
    headers: HeaderMap<HeaderValue>,
    forwarded_for: ForwardedFor,
    data: Bytes,
    max_response_size: usize,
    sender: oneshot::Sender<Result<ForwardResponse, UpstreamRequestError>>,
}

impl fmt::Debug for ForwardRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardRequest")
            .field("method", &self.method)
            .field("path", &self.path)
            .finish()
    }
}

impl UpstreamRequest for ForwardRequest {
    fn method(&self) -> Method {
        self.method.clone()
    }

    fn path(&self) -> Cow<'_, str> {
        self.path.as_str().into()
    }

    fn retry(&self) -> bool {
        false
    }

    fn intercept_status_errors(&self) -> bool {
        false
    }

    fn set_relay_id(&self) -> bool {
        false
    }

    fn route(&self) -> &'static str {
        "forward"
    }

    fn build(
        &mut self,
        _: &Config,
        mut builder: RequestBuilder,
    ) -> Result<crate::http::Request, HttpError> {
        for (key, value) in &self.headers {
            // Since the body is always decompressed by the server, we must not forward the
            // content-encoding header, as the upstream client will do its own content encoding.
            // Also, remove content-length because it's likely wrong.
            if !HOP_BY_HOP_HEADERS.contains(key) && !IGNORED_REQUEST_HEADERS.contains(key) {
                builder = builder.header(key, value);
            }
        }

        builder
            .header("X-Forwarded-For", self.forwarded_for.as_ref())
            .body(&self.data)
    }

    fn respond(
        self: Box<Self>,
        result: Result<UpstreamResponse, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let result = match result {
                Ok(response) => {
                    let status = status_to_1(response.status());
                    let headers = response
                        .headers()
                        .iter()
                        .filter(|(name, _)| !HOP_BY_HOP_HEADERS.contains(name))
                        .map(|(name, value)| {
                            (
                                // TODO: Undo this once reqwest is updated
                                name.as_str().parse().unwrap(),
                                value.as_str().parse().unwrap(),
                            )
                        })
                        .collect();

                    match response.bytes(self.max_response_size).await {
                        Ok(body) => Ok((status, headers, body)),
                        Err(error) => Err(UpstreamRequestError::Http(error)),
                    }
                }
                Err(error) => Err(error),
            };

            self.sender.send(result).ok();
        })
    }
}

/// Internal implementation of the forward endpoint.
async fn handle(
    state: ServiceState,
    forwarded_for: ForwardedFor,
    method: Method,
    uri: Uri,
    headers: HeaderMap<HeaderValue>,
    data: Bytes,
) -> Result<impl IntoResponse, ForwardError> {
    // The `/api/` path is special as it is actually a web UI endpoint. Therefore, reject requests
    // that either go to the API root or point outside the API.
    if uri.path() == API_PATH || !uri.path().starts_with(API_PATH) {
        return Ok(StatusCode::NOT_FOUND.into_response());
    }

    let (tx, rx) = oneshot::channel();

    let request = ForwardRequest {
        method,
        path: uri.to_string(),
        headers,
        forwarded_for,
        data,
        max_response_size: state.config().max_api_payload_size(),
        sender: tx,
    };

    state.upstream_relay().send(SendRequest(request));
    let (status, headers, body) = rx.await??;

    Ok(if headers.contains_key(header::CONTENT_TYPE) {
        (status, headers, body).into_response()
    } else {
        (status, headers).into_response()
    })
}

/// Route classes with request body limit overrides.
#[derive(Clone, Copy, Debug)]
enum SpecialRoute {
    FileUpload,
    ChunkUpload,
}

/// Glob matcher for special routes.
static SPECIAL_ROUTES: Lazy<GlobMatcher<SpecialRoute>> = Lazy::new(|| {
    let mut m = GlobMatcher::new();
    // file uploads / legacy dsym uploads
    m.add(
        "/api/0/projects/*/*/releases/*/files/",
        SpecialRoute::FileUpload,
    );
    m.add(
        "/api/0/projects/*/*/releases/*/dsyms/",
        SpecialRoute::FileUpload,
    );
    // new chunk uploads
    m.add(
        "/api/0/organizations/*/chunk-upload/",
        SpecialRoute::ChunkUpload,
    );
    m
});

/// Returns the maximum request body size for a route path.
fn get_limit_for_path(path: &str, config: &Config) -> usize {
    match SPECIAL_ROUTES.test(path) {
        Some(SpecialRoute::FileUpload) => config.max_api_file_upload_size(),
        Some(SpecialRoute::ChunkUpload) => config.max_api_chunk_upload_size(),
        None => config.max_api_payload_size(),
    }
}

/// Forward endpoint handler.
///
/// This endpoint will create a proxy request to the upstream for every incoming request and stream
/// the request body back to the origin. Regardless of the incoming connection, the connection to
/// the upstream uses its own HTTP version and transfer encoding.
///
/// # Usage
///
/// This endpoint is both a handler and a request function:
///
/// - Use it as [`Handler`] directly in router methods when registering this as a route.
/// - Call this manually from other request handlers to conditionally forward from other endpoints.
pub fn forward(state: ServiceState, req: Request) -> impl Future<Output = Response> {
    let limit = get_limit_for_path(req.uri().path(), state.config());
    handle.layer(DefaultBodyLimit::max(limit)).call(req, state)
}
