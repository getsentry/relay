use actix::prelude::*;
use actix_web::client::ClientRequest;
use actix_web::http::{header, header::HeaderName, ContentEncoding};
use actix_web::{AsyncResponder, Body, Error, HttpMessage, HttpRequest, HttpResponse};
use futures::prelude::*;

use service::{ServiceApp, ServiceState};

static HOP_BY_HOP_HEADERS: &[HeaderName] = &[
    header::CONNECTION,
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

fn get_forwarded_for<S>(request: &HttpRequest<S>) -> String {
    let peer_addr = request
        .peer_addr()
        .map(|v| v.ip().to_string())
        .unwrap_or_else(String::new);

    let forwarded = request
        .headers()
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if forwarded.is_empty() {
        peer_addr
    } else if peer_addr.is_empty() {
        forwarded.to_string()
    } else {
        format!("{}, {}", peer_addr, forwarded)
    }
}

fn forward_upstream(request: &HttpRequest<ServiceState>) -> ResponseFuture<HttpResponse, Error> {
    let config = request.state().config();
    let upstream = config.upstream_descriptor();

    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("");

    let mut forwarded_request = ClientRequest::build();
    for (key, value) in request.headers() {
        // Since there is no API in actix-web to access the raw, not-yet-decompressed stream, we
        // must not forward the content-encoding header, as the actix http client will do its own
        // content encoding. Also remove content-length because it's likely wrong.
        if HOP_BY_HOP_HEADERS.contains(key)
            || key == header::CONTENT_ENCODING
            || key == header::CONTENT_LENGTH
        {
            continue;
        }
        forwarded_request.header(key.clone(), value.clone());
    }

    let forwarded_request = tryf!(
        forwarded_request
            .no_default_headers()
            .disable_decompress()
            .method(request.method().clone())
            .uri(upstream.get_url(path_and_query))
            .set_header("Host", upstream.host())
            .set_header("X-Forwarded-For", get_forwarded_for(request))
            .set_header("Connection", "close")
            .body(if request.headers().get(header::CONTENT_TYPE).is_some() {
                Body::Streaming(Box::new(request.payload().from_err()))
            } else {
                Body::Empty
            })
    );

    forwarded_request
        .send()
        .map_err(Error::from)
        .and_then(move |response| {
            let mut forwarded_response = HttpResponse::build(response.status());
            // For the response body we're able to disable all automatic decompression and
            // compression done by actix-web or actix' http client.
            //
            // 0. use ClientRequestBuilder::disable_decompress() (see above)
            //
            // 1. Set content-encoding to identity such that actix-web will not attempt to compress
            //    again
            forwarded_response.content_encoding(ContentEncoding::Identity);

            for (key, value) in response.headers() {
                // 2. Just pass content-length, content-encoding etc through
                if HOP_BY_HOP_HEADERS.contains(key) {
                    continue;
                }
                forwarded_response.header(key.clone(), value.clone());
            }

            Ok(
                forwarded_response.body(
                    if response.headers().get(header::CONTENT_TYPE).is_some() {
                        Body::Streaming(Box::new(response.payload().from_err()))
                    } else {
                        Body::Empty
                    },
                ),
            )
        })
        .responder()
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.default_resource(|r| r.f(forward_upstream))
}
