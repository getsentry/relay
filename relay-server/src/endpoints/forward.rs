//! Server endpoint that proxies any request to the upstream.
//!
//! This endpoint will issue a client request to the upstream and append relay's own headers
//! (`X-Forwarded-For` and `Sentry-Relay-Id`). The response is then streamed back to the origin.

use std::future::Future;
use std::sync::LazyLock;

use axum::extract::{DefaultBodyLimit, Request};
use axum::handler::Handler;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use relay_common::glob2::GlobMatcher;
use relay_config::Config;

use crate::extractors::ForwardedFor;
use crate::service::ServiceState;
use crate::services::upstream::Method;
use crate::utils::ForwardRequest;

/// Root path of all API endpoints.
const API_PATH: &str = "/api/";

/// Internal implementation of the forward endpoint.
async fn handle(
    state: ServiceState,
    forwarded_for: ForwardedFor,
    method: Method,
    uri: Uri,
    headers: HeaderMap<HeaderValue>,
    data: Bytes,
) -> impl IntoResponse {
    if !state.config().http_forward() {
        return StatusCode::NOT_FOUND.into_response();
    }

    // The `/api/` path is special as it is actually a web UI endpoint. Therefore, reject requests
    // that either go to the API root or point outside the API.
    if uri.path() == API_PATH || !uri.path().starts_with(API_PATH) {
        return StatusCode::NOT_FOUND.into_response();
    }

    ForwardRequest::builder(method, uri.to_string())
        .with_name("forward")
        .with_headers(headers)
        .with_forwarded_for(forwarded_for)
        .with_body(data)
        .with_config(state.config())
        .send_to(state.upstream_relay())
        .await
        .into_response()
}

/// Route classes with request body limit overrides.
#[derive(Clone, Copy, Debug)]
enum SpecialRoute {
    FileUpload,
    ChunkUpload,
}

/// Glob matcher for special routes.
static SPECIAL_ROUTES: LazyLock<GlobMatcher<SpecialRoute>> = LazyLock::new(|| {
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
