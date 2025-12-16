use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use relay_config::Config;
use relay_system::Addr;
use tokio::sync::oneshot;

use crate::extractors::ForwardedFor;
use crate::http::{HttpError, RequestBuilder, Response as UpstreamResponse};
use crate::services::upstream::{
    Method, SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};

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

/// Default timeout for the forward request.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Errors which may occur when forwarding a request.
///
/// This error implements an [`IntoResponse`] conversion which is only acceptable for blindly
/// forwarded requests.
#[derive(Debug, thiserror::Error)]
#[error("error while forwarding request: {0}")]
pub enum ForwardError {
    Upstream(#[from] UpstreamRequestError),
    SendError(#[from] oneshot::error::RecvError),
    Timeout(#[from] tokio::time::error::Elapsed),
}

impl IntoResponse for ForwardError {
    fn into_response(self) -> Response {
        match self {
            Self::Upstream(UpstreamRequestError::Http(e)) => match e {
                HttpError::Overflow => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
                HttpError::Reqwest(error) => error
                    .status()
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
                    .into_response(),
                HttpError::Io(_) => StatusCode::BAD_GATEWAY.into_response(),
                HttpError::Json(_) => StatusCode::BAD_REQUEST.into_response(),
            },
            Self::Upstream(UpstreamRequestError::SendFailed(e)) => {
                if e.is_timeout() {
                    StatusCode::GATEWAY_TIMEOUT.into_response()
                } else {
                    StatusCode::BAD_GATEWAY.into_response()
                }
            }
            Self::SendError(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            Self::Upstream(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT.into_response(),
        }
    }
}

/// A http response of a successfully forwarded request.
pub struct ForwardResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: axum::body::Body,
}

impl ForwardResponse {
    fn from_upstream(response: UpstreamResponse) -> Self {
        let response = axum::http::Response::from(response.0);

        let (parts, body) = response.into_parts();
        let body = axum::body::Body::new(body);

        let status = parts.status;
        let headers = {
            let mut headers = parts.headers;
            // Make sure we clean out any headers which should not be forwarded from the upstream
            // response.
            for header in HOP_BY_HOP_HEADERS {
                headers.remove(header);
            }
            headers
        };

        Self {
            status,
            headers,
            body,
        }
    }
}

impl IntoResponse for ForwardResponse {
    fn into_response(self) -> Response {
        (self.status, self.headers, self.body).into_response()
    }
}

/// A request which can be forwarded to the next upstream Relay.
pub struct ForwardRequest {
    name: &'static str,
    method: Method,
    path: Cow<'static, str>,
    headers: HeaderMap<HeaderValue>,
    forwarded_for: Option<ForwardedFor>,
    body: Bytes,
    sender: oneshot::Sender<Result<ForwardResponse, UpstreamRequestError>>,
}

impl ForwardRequest {
    /// Returns a builder for sending a new [`ForwardRequest`].
    pub fn builder(method: Method, path: impl Into<Cow<'static, str>>) -> ForwardRequestBuilder {
        let (sender, receiver) = oneshot::channel();

        let request = ForwardRequest {
            name: "forward",
            method,
            path: path.into(),
            headers: Default::default(),
            forwarded_for: None,
            body: Bytes::new(),
            sender,
        };

        ForwardRequestBuilder {
            request,
            receiver,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl fmt::Debug for ForwardRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardRequest")
            .field("name", &self.name)
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
        self.path.as_ref().into()
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
        self.name
    }

    fn build(&mut self, builder: &mut RequestBuilder) -> Result<(), HttpError> {
        for (key, value) in &self.headers {
            // Since the body is always decompressed by the server, we must not forward the
            // content-encoding header, as the upstream client will do its own content encoding.
            // Also, remove content-length because it's likely wrong.
            if !HOP_BY_HOP_HEADERS.contains(key) && !IGNORED_REQUEST_HEADERS.contains(key) {
                builder.header(key, value);
            }
        }

        if let Some(forwarded_for) = &self.forwarded_for {
            builder.header("X-Forwarded-For", forwarded_for.as_ref());
        }

        builder.body(self.body.clone());

        Ok(())
    }

    fn respond(
        self: Box<Self>,
        result: Result<UpstreamResponse, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let result = result.map(ForwardResponse::from_upstream);
            let _ = self.sender.send(result);
        })
    }
}

/// A builder for creating and sending a [`ForwardRequest`].
pub struct ForwardRequestBuilder {
    request: ForwardRequest,
    timeout: Duration,
    receiver: oneshot::Receiver<Result<ForwardResponse, UpstreamRequestError>>,
}

impl ForwardRequestBuilder {
    /// Changes the request name used for internal reporting of metrics.
    ///
    /// If not set the name defaults to `forward`.
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.request.name = name;
        self
    }

    /// Adds additional headers to the request.
    ///
    /// The builder takes care of removing headers which can or should not be forwarded to a
    /// different http server.
    pub fn with_headers(mut self, mut headers: HeaderMap) -> Self {
        let headers_to_remove = HOP_BY_HOP_HEADERS.iter().chain(IGNORED_REQUEST_HEADERS);
        for header in headers_to_remove {
            headers.remove(header);
        }

        self.request.headers = headers;
        self
    }

    /// Adds an optional forwarded for header.
    pub fn with_forwarded_for(mut self, ff: impl Into<Option<ForwardedFor>>) -> Self {
        self.request.forwarded_for = ff.into();
        self
    }

    /// Adds the request body.
    ///
    /// The body may be empty for `GET` requests.
    pub fn with_body(mut self, body: Bytes) -> Self {
        self.request.body = body;
        self
    }

    /// Applies the specified Relay [`Config`] to the forwarded request.
    pub fn with_config(mut self, config: &Config) -> Self {
        self.timeout = config.http_timeout();
        self
    }

    /// Sends the final request to the next upstream Relay and awaits its response.
    ///
    /// The return result can be turned into a [`axum::response::Response`].
    pub async fn send_to(
        self,
        upstream: &Addr<UpstreamRelay>,
    ) -> Result<ForwardResponse, ForwardError> {
        upstream.send(SendRequest(self.request));

        Ok(tokio::time::timeout(self.timeout, self.receiver).await???)
    }
}
