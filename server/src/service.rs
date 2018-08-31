use std::fmt;
use std::sync::Arc;

use actix::prelude::*;
use actix_web::{server, App};
use failure::ResultExt;
use failure::{Backtrace, Context, Fail};
use listenfd::ListenFd;

use semaphore_common::Config;

/// Server state.
#[derive(Clone)]
pub struct ServiceState {
    config: Arc<Config>,
}

impl ServiceState {
    /// Returns an atomically counted reference to the config.
    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
    }
}

/// The actix app type for the relay web service.
pub type ServiceApp = App<ServiceState>;

fn make_app(state: ServiceState) -> ServiceApp {
    let app = App::with_state(state);

    app.resource("/api/", |r| r.f(|_| HttpResponse::NotFound()))
        .handler("/api", forward_upstream)
}

use actix::prelude::*;
use actix_web::client::ClientRequest;
use actix_web::http::{header, header::HeaderName};
use actix_web::{AsyncResponder, Error, HttpRequest, HttpResponse};
use futures::prelude::*;


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

/// Given a relay config spawns the server together with all actors and lets them run forever.
///
/// Effectively this boots the server.
pub fn run(config: Config) {
    let config = Arc::new(config);
    let sys = System::new("relay");

    let service_state = ServiceState {
        config: config.clone(),
    };

    let mut server = server::new(move || make_app(service_state.clone()));

    server = server
            .bind(config.listen_addr()).unwrap();

    server.system_exit().start();
    let _ = sys.run();
}
