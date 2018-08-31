extern crate actix;
extern crate futures;
extern crate actix_web;

use actix::prelude::*;
use actix_web::{server, App};

use actix_web::client::ClientRequest;
use actix_web::{AsyncResponder, Error, HttpRequest, HttpResponse};

use futures::prelude::*;

fn forward_upstream(request: &HttpRequest<()>) -> ResponseFuture<HttpResponse, Error> {
    ClientRequest::build()
        .method(request.method().clone())
        .uri("http://localhost:3000/level2")
        .header("Connection", "close")
        .finish().unwrap()
        .send().map_err(|e| {
            println!("{:?}", e);
            Error::from(e)
        })
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
            .handler("/", forward_upstream)
    });

    server = server.bind("localhost:3000").unwrap();
    server.system_exit().start();
    let _ = sys.run();
}
