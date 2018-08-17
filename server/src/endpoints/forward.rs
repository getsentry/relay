use actix_web::client::ClientRequest;
use actix_web::{AsyncResponder, Body, Error, HttpMessage, HttpRequest, HttpResponse};
use itertools::Itertools;

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

    let addr = request.peer_addr().map(|x| x.ip().to_string());

    let forwarded_ips = addr
        .as_ref()
        .map(|x| x.as_str())
        .into_iter()
        .chain(
            request
                .headers()
                .get("X-Forwarded-For")
                .and_then(|x| x.to_str().ok())
                .unwrap_or("")
                .split(",")
                .map(|x| x.trim()),
        )
        .join(",");

    let mut request_builder = ClientRequest::build_from(request);
    request_builder.uri(upstream.get_url(path_and_query));
    request_builder.set_header("X-Forwarded-For", forwarded_ips);

    let client_request =
        tryf!(request_builder.body(Body::Streaming(Box::new(request.payload().from_err()))));
    client_request
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
