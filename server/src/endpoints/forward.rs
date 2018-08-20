use actix_web::client::ClientRequest;
use actix_web::{AsyncResponder, Body, Error, HttpMessage, HttpRequest, HttpResponse};

use futures::{future, Future, Stream};

use service::{ServiceApp, ServiceState};


fn get_forwarded_for<S>(request: &HttpRequest<S>) -> String {
    let peer_addr = request
        .peer_addr()
        .map(|v| v.ip().to_string())
        .unwrap_or_else(|| String::new());

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

fn forward_upstream(
    request: &HttpRequest<ServiceState>,
) -> Box<Future<Item = HttpResponse, Error = Error>> {
    let config = request.state().config();
    let upstream = config.upstream_descriptor();

    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("");

    let client_request = ClientRequest::build_from(request)
        .uri(upstream.get_url(path_and_query))
        .set_header("Host", upstream.host())
        .set_header("X-Forwarded-For", get_forwarded_for(request))
        .body(Body::Streaming(Box::new(request.payload().from_err())));

    tryf!(client_request)
        .send()
        .map_err(Error::from)
        .and_then(|resp| {
            let mut response_builder = HttpResponse::build(resp.status());
            for (key, value) in resp.headers() {
                response_builder.header(key.clone(), value.clone());
            }
            Ok(response_builder.body(Body::Streaming(Box::new(resp.payload().from_err()))))
        })
        .responder()
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.default_resource(|r| r.f(forward_upstream))
}
