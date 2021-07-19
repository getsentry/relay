//! Server endpoint that proxies any request to the upstream.
//!
//! This endpoint will issue a client request to the upstream and append relay's own headers
//! (`X-Forwarded-For` and `Sentry-Relay-Id`). The response is then streamed back to the origin.

use std::borrow::Cow;
use std::fmt;

use ::actix::prelude::*;
use actix_web::error::ResponseError;
use actix_web::http::header::HeaderValue;
use actix_web::http::{header, header::HeaderName, uri::PathAndQuery, StatusCode};
use actix_web::http::{HeaderMap, Method};
use actix_web::{AsyncResponder, Error, HttpMessage, HttpRequest, HttpResponse};
use bytes::Bytes;
use failure::Fail;
use futures::{prelude::*, sync::oneshot};

use lazy_static::lazy_static;

use relay_common::GlobMatcher;
use relay_config::Config;
use relay_log::LogError;

use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError};
use crate::body::ForwardBody;
use crate::endpoints::statics;
use crate::extractors::ForwardedFor;
use crate::http::{HttpError, RequestBuilder, Response};
use crate::service::{ServiceApp, ServiceState};
use crate::utils::ApiErrorResponse;

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
                HttpError::Custom(e) => {
                    HttpResponse::InternalServerError().json(&ApiErrorResponse::from_fail(e))
                }
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

//#[derive(Debug)]
struct ForwardRequest {
    method: Method,
    path: String,
    headers: HeaderMap<HeaderValue>,
    forwarded_for: ForwardedFor,
    data: Bytes,
    response_channel: Option<oneshot::Sender<Result<Response, UpstreamRequestError>>>,
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

    fn build(&mut self, mut builder: RequestBuilder) -> Result<crate::http::Request, HttpError> {
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
        &mut self,
        response: Result<Response, UpstreamRequestError>,
    ) -> ResponseFuture<(), ()> {
        let result = if response.is_ok() {
            futures::future::ok(())
        } else {
            futures::future::err(())
        };
        self.response_channel.take().unwrap().send(response).ok();
        Box::new(result)
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
        .and_then(|data| {
            let (tx, rx) = oneshot::channel();

            let forward_request = ForwardRequest {
                method,
                path: path_and_query,
                headers,
                forwarded_for,
                data,
                response_channel: Some(tx),
            };

            UpstreamRelay::from_registry().do_send(SendRequest(forward_request));
            rx.map_err(|_| {
                Error::from(ForwardedUpstreamRequestError(
                    UpstreamRequestError::ChannelClosed,
                ))
            })
        })
        .and_then(|response| {
            response.map_err(|e| Error::from(ForwardedUpstreamRequestError::from(e)))
        })
        .and_then(move |response| {
            let status = response.status();
            let headers = response.clone_headers();
            response
                .bytes(max_response_size)
                .and_then(move |body| Ok((status, headers, body)))
                .map_err(|e| {
                    Error::from(ForwardedUpstreamRequestError(UpstreamRequestError::Http(e)))
                })
        })
        .and_then(move |(status, headers, body)| {
            let actix_code = StatusCode::from_u16(status.as_u16()).unwrap();
            let mut forwarded_response = HttpResponse::build(actix_code);

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
