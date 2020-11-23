//! Server endpoint that proxies any request to the upstream.
//!
//! This endpoint will issue a client request to the upstream and append relay's own headers
//! (`X-Forwarded-For` and `Sentry-Relay-Id`). The response is then streamed back to the origin.

use ::actix::prelude::*;
use actix_web::error::ResponseError;
use actix_web::http::{header, header::HeaderName, uri::PathAndQuery, ContentEncoding, StatusCode};
use actix_web::{AsyncResponder, Error, HttpMessage, HttpRequest, HttpResponse};
use failure::Fail;
use futures::prelude::*;
use lazy_static::lazy_static;

use relay_common::{GlobMatcher, LogError};
use relay_config::Config;

use crate::actors::upstream::{SendRequest, UpstreamRequestError};
use crate::body::ForwardBody;
use crate::endpoints::statics;
use crate::extractors::ForwardedFor;
use crate::http::{RequestBuilder, Response};
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
#[derive(Fail, Debug)]
#[fail(display = "error while forwarding request: {}", _0)]
struct ForwardedUpstreamRequestError(#[cause] UpstreamRequestError);

impl From<UpstreamRequestError> for ForwardedUpstreamRequestError {
    fn from(e: UpstreamRequestError) -> Self {
        ForwardedUpstreamRequestError(e)
    }
}

impl ResponseError for ForwardedUpstreamRequestError {
    fn error_response(&self) -> HttpResponse {
        match &self.0 {
            // should be unreachable
            UpstreamRequestError::NoCredentials | UpstreamRequestError::InvalidJson(_) => {
                log::error!("unreachable codepath: {}", LogError(self));
                HttpResponse::InternalServerError().finish()
            }
            UpstreamRequestError::SendFailed(e) => e.error_response(),
            UpstreamRequestError::BuildFailed(e) => e.as_response_error().error_response(),
            UpstreamRequestError::PayloadFailed(e) => e.error_response(),
            UpstreamRequestError::RateLimited(_) => {
                HttpResponse::new(StatusCode::TOO_MANY_REQUESTS)
            }
            UpstreamRequestError::ResponseError(code, _) => HttpResponse::new(*code),
            UpstreamRequestError::ChannelClosed => {
                log::error!("{}", LogError(self));
                HttpResponse::InternalServerError().finish()
            }
            UpstreamRequestError::Reqwest(error) => {
                log::error!("{}", LogError(error));
                HttpResponse::new(
                    StatusCode::from_u16(error.status().map(|x| x.as_u16()).unwrap_or(500))
                        .unwrap(),
                )
            }
        }
    }
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
pub fn forward_upstream(
    request: &HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, Error> {
    let config = request.state().config();
    let max_response_size = config.max_api_payload_size();
    let upstream_relay = request.state().upstream_relay();
    let limit = get_limit_for_path(request.path(), &config);

    let path_and_query = request
        .uri()
        .path_and_query()
        .map(PathAndQuery::as_str)
        .unwrap_or("")
        .to_owned();

    let method = request.method().clone();
    let headers = request.headers().clone();
    let forwarded_for = ForwardedFor::from(request);

    ForwardBody::new(request, limit)
        .map_err(Error::from)
        .and_then(move |data| {
            let forward_request = SendRequest::new(method, path_and_query)
                .retry(false)
                .update_rate_limits(false)
                .set_relay_id(false)
                .build(move |mut builder: RequestBuilder| {
                    for (key, value) in &headers {
                        // Since there is no API in actix-web to access the raw, not-yet-decompressed stream, we
                        // must not forward the content-encoding header, as the actix http client will do its own
                        // content encoding. Also remove content-length because it's likely wrong.
                        if HOP_BY_HOP_HEADERS.iter().any(|x| x == key)
                            || IGNORED_REQUEST_HEADERS.iter().any(|x| x == key)
                        {
                            continue;
                        }

                        builder.header(key.as_str(), value.as_bytes());
                    }

                    // actix-web specific workarounds. We don't remember why no_default_headers was
                    // necessary, but we suspect that actix sends out headers twice instead of
                    // having them be overridden (user-agent?)
                    //
                    // Need for disabling decompression is demonstrated by the
                    // `test_forwarding_content_encoding` integration test.
                    if let RequestBuilder::Actix(ref mut builder) = builder {
                        builder.no_default_headers();
                        builder.disable_decompress();
                    }

                    builder.header("X-Forwarded-For", forwarded_for.as_ref().as_bytes());

                    builder.body(data.clone().into())
                })
                .transform(move |response: Response| {
                    let status = response.status();
                    let headers = response.clone_headers();
                    response
                        .bytes(max_response_size)
                        .and_then(move |body| Ok((status, headers, body)))
                        .map_err(ForwardedUpstreamRequestError)
                });

            upstream_relay.send(forward_request).map_err(|_| {
                Error::from(ForwardedUpstreamRequestError(
                    UpstreamRequestError::ChannelClosed,
                ))
            })
        })
        .and_then(move |result: Result<_, ForwardedUpstreamRequestError>| {
            let (status, headers, body) = result?;
            let mut forwarded_response = HttpResponse::build(status);

            // For the response body we're able to disable all automatic decompression and
            // compression done by actix-web or actix' http client.
            //
            // 0. Use ClientRequestBuilder::disable_decompress() (see above)
            // 1. Set content-encoding to identity such that actix-web will not to compress again
            forwarded_response.content_encoding(ContentEncoding::Identity);

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

            Ok(if has_content_type {
                forwarded_response.body(body)
            } else {
                forwarded_response.finish()
            })
        })
        .responder()
}

/// Registers this endpoint in the actix-web app.
///
/// NOTE: This endpoint registers a catch-all handler on `/api`. Register this endpoint last, since
/// no routes can be registered afterwards!
pub fn configure_app(app: ServiceApp) -> ServiceApp {
    // We only forward API requests so that relays cannot be used to surf sentry's frontend. The
    // "/api/" path is special as it is actually a web UI endpoint.
    app.resource("/api/", |r| {
        r.name("api-root");
        r.f(statics::not_found)
    })
    .handler("/api", forward_upstream)
}
