use actix::prelude::*;
use actix_web::client::ClientRequest;
use actix_web::http::{header, header::HeaderName};
use actix_web::{AsyncResponder, Error, HttpRequest, HttpResponse};
use futures::prelude::*;

use service::{ServiceApp, ServiceState};

fn forward_upstream(request: &HttpRequest<ServiceState>) -> ResponseFuture<HttpResponse, Error> {
    let config = request.state().config();
    let upstream = config.upstream_descriptor();

    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("");

    let mut forwarded_request_builder = ClientRequest::build();
    forwarded_request_builder
        .method(request.method().clone())
        .uri(upstream.get_url(path_and_query))
        .header("Connection", "close")
        .timeout(config.http_timeout());

    forwarded_request_builder.finish().unwrap()
        .send().map_err(Error::from)
        .and_then(move |response| {
            let mut forwarded_response = HttpResponse::build(response.status());
            Ok(forwarded_response.finish())
        })
        .responder()
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/", |r| r.f(|_| HttpResponse::NotFound()))
        .handler("/api", forward_upstream)
}
