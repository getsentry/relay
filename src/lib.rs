extern crate actix;
extern crate futures;
extern crate actix_web;

use actix::prelude::*;
use actix_web::{server, App};

use actix_web::client::ClientRequest;
use actix_web::{AsyncResponder, Error, HttpRequest, HttpResponse};

use futures::prelude::*;

fn forward_upstream(request: &HttpRequest<()>) -> ResponseFuture<HttpResponse, Error> {
    let mut forwarded_request_builder = ClientRequest::build();
    forwarded_request_builder
        .method(request.method().clone())
        .uri("http://localhost:9000/api/foo")
        .header("Connection", "close");

    forwarded_request_builder.finish().unwrap()
        .send().map_err(Error::from)
        .and_then(move |response| {
            let mut forwarded_response = HttpResponse::build(response.status());
            Ok(forwarded_response.finish())
        })
        .responder()
}

pub fn run() {
    let sys = System::new("relay");

    let mut server = server::new(|| {
        App::with_state(())
            .handler("/api", forward_upstream)
    });

    server = server.bind("localhost:3000").unwrap();
    server.system_exit().start();
    let _ = sys.run();
}
