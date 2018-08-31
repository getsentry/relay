//! Implements basics for the protocol.
#![warn(missing_docs)]

extern crate actix;
extern crate actix_web;
extern crate futures;
extern crate parking_lot;
extern crate url;

use actix::prelude::*;
use actix_web::{server, App};

/// The actix app type for the relay web service.
type ServiceApp = App<()>;

fn make_app() -> ServiceApp {
    let app = App::with_state(());

    app.resource("/api/", |r| r.f(|_| HttpResponse::NotFound()))
        .handler("/api", forward_upstream)
}

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

/// Given a relay config spawns the server together with all actors and lets them run forever.
///
/// Effectively this boots the server.
pub fn run() {
    let sys = System::new("relay");

    let mut server = server::new(move || make_app());

    server = server.bind("localhost:3000").unwrap();

    server.system_exit().start();
    let _ = sys.run();
}
