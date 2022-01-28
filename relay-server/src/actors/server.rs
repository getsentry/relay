use ::actix::prelude::*;
use actix_web::server::StopServer;
use futures::prelude::*;

use relay_config::Config;
use relay_statsd::metric;

use crate::service;
use crate::statsd::RelayCounters;
use relay_common_actors::controller::{Controller, Shutdown};

pub use crate::service::ServerError;

pub struct Server {
    http_server: Recipient<StopServer>,
}

impl Server {
    pub fn start(config: Config) -> Result<Addr<Self>, ServerError> {
        metric!(counter(RelayCounters::ServerStarting) += 1);
        let http_server = service::start(config)?;
        Ok(Server { http_server }.start())
    }
}

impl Actor for Server {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        Controller::subscribe(context.address());
    }
}

impl Handler<Shutdown> for Server {
    type Result = ResponseFuture<(), ()>;

    fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
        let graceful = message.timeout.is_some();

        // We assume graceful shutdown if we're given a timeout. The actix-web http server is
        // configured with the same timeout, so it will match. Unfortunately, we have to drop any
        // errors  and replace them with the generic `TimeoutError`.
        let future = self
            .http_server
            .send(StopServer { graceful })
            .map_err(|_| ())
            .and_then(|result| result.map_err(|_| ()));

        Box::new(future)
    }
}
