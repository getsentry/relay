use std::sync::Arc;

use actix::prelude::*;

use futures::future;
use futures::prelude::*;

use relay_config::{Config, RelayMode};
use relay_statsd::metric;

use crate::actors::controller::{Controller, Shutdown};
use crate::actors::upstream::{IsAuthenticated, IsNetworkOutage, UpstreamRelay};
use crate::metrics::RelayGauges;

pub struct Healthcheck {
    is_shutting_down: bool,
    config: Arc<Config>,
}

impl Healthcheck {
    pub fn new(config: Arc<Config>) -> Self {
        Healthcheck {
            is_shutting_down: false,
            config,
        }
    }
}

impl Actor for Healthcheck {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        Controller::subscribe(context.address());
    }
}

impl Supervised for Healthcheck {}

impl SystemService for Healthcheck {}

impl Default for Healthcheck {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}

impl Handler<Shutdown> for Healthcheck {
    type Result = Result<(), ()>;

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

    fn handle(&mut self, message: IsHealthy, context: &mut Self::Context) -> Self::Result {
        let upstream = UpstreamRelay::from_registry();

        if self.config.relay_mode() == RelayMode::Managed {
            upstream
                .send(IsNetworkOutage)
                .map_err(|_| ())
                .map(|is_network_outage| {
                    metric!(
                        gauge(RelayGauges::NetworkOutage) = if is_network_outage { 1 } else { 0 }
                    );
                })
                .into_actor(self)
                .spawn(context);
        }

        match message {
            IsHealthy::Liveness => Box::new(future::ok(true)),
            IsHealthy::Readiness => {
                if self.is_shutting_down {
                    Box::new(future::ok(false))
                } else if self.config.requires_auth() {
                    Box::new(upstream.send(IsAuthenticated).map_err(|_| ()))
                } else {
                    Box::new(future::ok(true))
                }
            }
        }
    }
}
