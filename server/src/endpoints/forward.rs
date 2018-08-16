use actix_web::client::ClientRequest;
use actix_web::{AsyncResponder, Body, Error, HttpMessage, HttpRequest, HttpResponse};

use futures::{future, Future, Stream};
use http::header::{self, HeaderValue};

use service::{ServiceApp, ServiceState};

macro_rules! tryf {
    ($e:expr) => {
        match $e {
            Ok(value) => value,
            Err(e) => return Box::new(future::err(Error::from(e))),
        }
    };
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

    let mut request_builder = ClientRequest::build_from(request);
    request_builder.uri(upstream.get_url(path_and_query));

    if let Some(peer_addr) = request.peer_addr() {
        request_builder.header(
            header::FORWARDED,
            tryf!(HeaderValue::from_str(&peer_addr.to_string())),
        );
    }

    let client_request = tryf!(request_builder.finish());
    client_request
        .send()
        .map_err(Error::from)
        .and_then(|resp| {
            Ok(HttpResponse::Ok().body(Body::Streaming(Box::new(resp.payload().from_err()))))
        })
        .responder()
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.default_resource(|r| r.f(forward_upstream))
}
