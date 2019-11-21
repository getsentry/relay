use actix::prelude::*;

use futures::future;
use futures::prelude::*;

use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use crate::actors::upstream::{IsAuthenticated, UpstreamRelay};

pub struct Healthcheck {
    is_shutting_down: bool,
    upstream: Addr<UpstreamRelay>,
}

impl Healthcheck {
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        Healthcheck {
            is_shutting_down: false,
            upstream,
        }
    }
}

impl Actor for Healthcheck {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        Controller::from_registry().do_send(Subscribe(context.address().recipient()));
    }
}

impl Handler<Shutdown> for Healthcheck {
    type Result = Result<(), TimeoutError>;

    fn handle(&mut self, _message: Shutdown, _context: &mut Self::Context) -> Self::Result {
        self.is_shutting_down = true;
        Ok(())
    }
}

pub enum IsHealthy {
    /// Check if the Relay is alive at all.
    Liveness,
    /// Check if the Relay is in a state where the load balancer should route traffic to it (i.e.
    /// it's both live/alive and not too busy).
    Readiness,
}

impl Message for IsHealthy {
    type Result = Result<bool, ()>;
}

impl Handler<IsHealthy> for Healthcheck {
    type Result = ResponseFuture<bool, ()>;

    fn handle(&mut self, message: IsHealthy, _context: &mut Self::Context) -> Self::Result {
        match message {
            IsHealthy::Liveness => Box::new(future::ok(true)),
            IsHealthy::Readiness => {
                if self.is_shutting_down {
                    Box::new(future::ok(false))
                } else {
                    Box::new(self.upstream.send(IsAuthenticated).map_err(|_| ()))
                }
            }
        }
    }
}
