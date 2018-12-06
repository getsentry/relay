//! Server endpoint that proxies any request to the upstream.
//!
//! This endpoint will issue a client request to the upstream and append relay's own headers
//! (`X-Forwarded-For` and `Sentry-Relay-Id`). The response is then streamed back to the origin.

use ::actix::prelude::*;
use actix_web::client::ClientRequest;
use actix_web::http::{header, header::HeaderName, ContentEncoding};
use actix_web::{AsyncResponder, Error, HttpMessage, HttpRequest, HttpResponse};
use futures::prelude::*;
use lazy_static::lazy_static;

use semaphore_common::{Config, GlobMatcher};

use crate::body::ForwardBody;
use crate::extractors::ForwardedFor;
use crate::service::{ServiceApp, ServiceState};

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
static IGNORED_REQUEST_HEADERS: &[HeaderName] = &[header::CONTENT_ENCODING, header::CONTENT_LENGTH];

/// Route classes with request body limit overrides.
#[derive(Clone, Copy, Debug)]
enum SpecialRoute {
    FileUpload,
    ChunkUpload,
}

lazy_static! {
    /// Glob matcher for special routes.
    static ref SPECIAL_ROUTES: GlobMatcher<SpecialRoute> = {
        let mut m = GlobMatcher::new();
        // file uploads / legacy dsym uploads
        m.add("/api/0/projects/*/*/releases/*/files/", SpecialRoute::FileUpload);
        m.add("/api/0/projects/*/*/releases/*/dsyms/", SpecialRoute::FileUpload);
        // new chunk uploads
        m.add("/api/0/organizations/*/chunk-upload/", SpecialRoute::ChunkUpload);
        m
    };
}

/// Returns the maximum request body size for a route path.
fn get_limit_for_path(path: &str, config: &Config) -> usize {
    match SPECIAL_ROUTES.test(path) {
        Some(SpecialRoute::FileUpload) => config.max_api_file_upload_size(),
        Some(SpecialRoute::ChunkUpload) => config.max_api_chunk_upload_size(),
        None => config.max_api_payload_size(),
    }
}

/// Implementation of the forward endpoint.
///
/// This endpoint will create a proxy request to the upstream for every incoming request and stream
/// the request body back to the origin. Regardless of the incoming connection, the connection to
/// the upstream uses its own HTTP version and transfer encoding.
fn forward_upstream(request: &HttpRequest<ServiceState>) -> ResponseFuture<HttpResponse, Error> {
    let config = request.state().config();
    let upstream = config.upstream_descriptor();
    let limit = get_limit_for_path(request.path(), &config);

    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("");

    let mut forwarded_request_builder = ClientRequest::build();
    for (key, value) in request.headers() {
        // Since there is no API in actix-web to access the raw, not-yet-decompressed stream, we
        // must not forward the content-encoding header, as the actix http client will do its own
        // content encoding. Also remove content-length because it's likely wrong.
        if HOP_BY_HOP_HEADERS.contains(key) || IGNORED_REQUEST_HEADERS.contains(key) {
            continue;
        }
        forwarded_request_builder.header(key.clone(), value.clone());
    }

    let host_header = config.http_host_header().unwrap_or_else(|| upstream.host());
    forwarded_request_builder
        .no_default_headers()
        .disable_decompress()
        .method(request.method().clone())
        .uri(upstream.get_url(path_and_query))
        .set_header("Host", host_header)
        .set_header("X-Forwarded-For", ForwardedFor::from(request))
        .set_header("Connection", "close")
        .timeout(config.http_timeout());

    ForwardBody::new(request)
        .limit(limit)
        .map_err(Error::from)
        .and_then(move |data| forwarded_request_builder.body(data).map_err(Error::from))
        .and_then(move |request| request.send().map_err(Error::from))
        .and_then(move |response| {
            let mut forwarded_response = HttpResponse::build(response.status());

            // For the response body we're able to disable all automatic decompression and
            // compression done by actix-web or actix' http client.
            //
            // 0. Use ClientRequestBuilder::disable_decompress() (see above)
            // 1. Set content-encoding to identity such that actix-web will not to compress again
            forwarded_response.content_encoding(ContentEncoding::Identity);

            for (key, value) in response.headers() {
                // 2. Just pass content-length, content-encoding etc through
                if HOP_BY_HOP_HEADERS.contains(key) {
                    continue;
                }
                forwarded_response.header(key.clone(), value.clone());
            }

            Ok(if response.headers().get(header::CONTENT_TYPE).is_some() {
                forwarded_response.streaming(response.payload())
            } else {
                forwarded_response.finish()
            })
        })
        .responder()
}

/// Registers this endpoint in the actix-web app.
pub fn configure_app(app: ServiceApp) -> ServiceApp {
    // We only forward API requests so that relays cannot be used to surf sentry's frontend. The
    // "/api/" path is special as it is actually a web UI endpoint.
    app.resource("/api/", |r| r.f(|_| HttpResponse::NotFound()))
        .handler("/api", forward_upstream)
}
