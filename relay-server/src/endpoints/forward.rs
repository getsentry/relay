//! Server endpoint that proxies any request to the upstream.
//!
//! This endpoint will issue a client request to the upstream and append relay's own headers
//! (`X-Forwarded-For` and `Sentry-Relay-Id`). The response is then streamed back to the origin.

use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use actix::ResponseFuture;
use actix_web::error::{ParseError, ResponseError};
use actix_web::http::header::{self, HeaderName, HeaderValue};
use actix_web::http::uri::PathAndQuery;
use actix_web::http::{HeaderMap, StatusCode};
use actix_web::{App, Error, HttpMessage, HttpRequest, HttpResponse};
use axum::body::Bytes;
use futures::TryFutureExt;
use once_cell::sync::Lazy;
use relay_config::Config;
use relay_general::utils::GlobMatcher;
use relay_log::LogError;
use tokio::sync::oneshot;

use crate::actors::upstream::{
    Method, SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::body;
use crate::endpoints::statics;
use crate::extractors::ForwardedFor;
use crate::http::{HttpError, RequestBuilder, Response};
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

/// Route classes with request body limit overrides.
#[derive(Clone, Copy, Debug)]
enum SpecialRoute {
    FileUpload,
    ChunkUpload,
}

/// A wrapper struct that allows conversion of UpstreamRequestError into a `dyn ResponseError`. The
/// conversion logic is really only acceptable for blindly forwarded requests.
#[derive(Debug, thiserror::Error)]
#[error("error while forwarding request: {0}")]
struct ForwardedUpstreamRequestError(#[from] UpstreamRequestError);

impl ResponseError for ForwardedUpstreamRequestError {
    fn error_response(&self) -> HttpResponse {
        match &self.0 {
            UpstreamRequestError::Http(e) => match e {
                HttpError::Overflow => HttpResponse::PayloadTooLarge().finish(),
                HttpError::Reqwest(error) => {
                    relay_log::error!("{}", LogError(error));
                    HttpResponse::new(
                        StatusCode::from_u16(error.status().map(|x| x.as_u16()).unwrap_or(500))
                            .unwrap(),
                    )
                }
                HttpError::Io(_) => HttpResponse::BadGateway().finish(),
                HttpError::Json(e) => e.error_response(),
                HttpError::NoCredentials => HttpResponse::InternalServerError().finish(),
            },
            UpstreamRequestError::SendFailed(e) => {
                if e.is_timeout() {
                    HttpResponse::GatewayTimeout().finish()
                } else {
                    HttpResponse::BadGateway().finish()
                }
            }
            e => {
                // should all be unreachable
                relay_log::error!(
                    "supposedly unreachable codepath for forward endpoint: {}",
                    LogError(e)
                );
                HttpResponse::InternalServerError().finish()
            }
        }
    }
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

type Headers = Vec<(String, Vec<u8>)>;
type ForwardResponse = (StatusCode, Headers, Vec<u8>);

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
            // Since there is no API in actix-web to access the raw, not-yet-decompressed stream, we
            // must not forward the content-encoding header, as the actix http client will do its own
            // content encoding. Also remove content-length because it's likely wrong.
            if HOP_BY_HOP_HEADERS.iter().any(|x| x == key)
                || IGNORED_REQUEST_HEADERS.iter().any(|x| x == key)
            {
                continue;
            }

            builder.header(key, value);
        }

        builder.header("X-Forwarded-For", self.forwarded_for.as_ref());
        builder.body(&self.data)
    }

    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let result = match result {
                Ok(response) => {
                    let status = StatusCode::from_u16(response.status().as_u16()).unwrap();
                    let headers = response.clone_headers();

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

/// Implementation of the forward endpoint.
///
/// This endpoint will create a proxy request to the upstream for every incoming request and stream
/// the request body back to the origin. Regardless of the incoming connection, the connection to
/// the upstream uses its own HTTP version and transfer encoding.
async fn forward_upstream(request: HttpRequest<ServiceState>) -> Result<HttpResponse, Error> {
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(PathAndQuery::as_str)
        .unwrap_or("")
        .to_owned();

    let Ok(method) = Method::from_bytes(request.method().as_ref().as_bytes()) else {
        return Err(ParseError::Method.into());
    };

    let config = request.state().config();
    let limit = get_limit_for_path(request.path(), config);
    let data = body::request_body(&request, limit).await?;

    let (tx, rx) = oneshot::channel();
    UpstreamRelay::from_registry().send(SendRequest(ForwardRequest {
        method,
        path: path_and_query,
        headers: request.headers().clone(),
        forwarded_for: ForwardedFor::from(&request),
        data,
        max_response_size: config.max_api_payload_size(),
        sender: tx,
    }));

    let (status, headers, body) = rx
        .await
        .map_err(|_| ForwardedUpstreamRequestError(UpstreamRequestError::ChannelClosed))?
        .map_err(ForwardedUpstreamRequestError)?;

    let mut forwarded_response = HttpResponse::build(status);
    let mut has_content_type = false;

    for (key, value) in headers {
        if key == "content-type" {
            has_content_type = true;
        }

        // 2. Just pass content-length, content-encoding etc through
        if HOP_BY_HOP_HEADERS.iter().any(|x| key.as_str() == x) {
            continue;
        }

        forwarded_response.header(&key, &*value);
    }

    // For reqwest the option to disable automatic response decompression can only be
    // set per-client. For non-forwarded upstream requests that is desirable, so we
    // keep it enabled.
    //
    // Essentially this means that content negotiation is done twice, and the response
    // body is first decompressed by reqwest, then re-compressed by actix-web.

    Ok(if has_content_type {
        forwarded_response.body(body)
    } else {
        forwarded_response.finish()
    })
}

pub fn forward_compat(request: &HttpRequest<ServiceState>) -> ResponseFuture<HttpResponse, Error> {
    Box::new(Box::pin(forward_upstream(request.clone())).compat())
}

/// Registers this endpoint in the actix-web app.
///
/// NOTE: This endpoint registers a catch-all handler on `/api`. Register this endpoint last, since
/// no routes can be registered afterwards!
pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
    // We only forward API requests so that relays cannot be used to surf sentry's frontend. The
    // "/api/" path is special as it is actually a web UI endpoint.
    app.resource("/api/", |r| {
        r.name("api-root");
        r.f(statics::not_found)
    })
    .handler("/api", forward_compat)
}
