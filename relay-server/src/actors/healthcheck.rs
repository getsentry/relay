use std::sync::Arc;

use actix::SystemService;
use futures03::compat::Future01CompatExt;
use once_cell::sync::Lazy;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{mpsc, oneshot};

use relay_config::{Config, RelayMode};
use relay_metrics::{AcceptsMetrics, Aggregator};
use relay_statsd::metric;
use relay_system::{Controller, Shutdown};

use crate::actors::upstream::{IsAuthenticated, IsNetworkOutage, UpstreamRelay};
use crate::statsd::RelayGauges;

static ADDRESS: Lazy<std::sync::Mutex<Option<Addr<HealthCheckMessage>>>> =
    Lazy::new(|| std::sync::Mutex::new(None));

// Wrapper for our data and oneshot chanel(needed to retransmit back the reply)
#[derive(Debug)]
struct Message<T> {
    data: T,
    responder: oneshot::Sender<bool>,
}

// Custom error that acts as an abstraction
pub struct SendError;

// tx = transmitter, mpcs = multi-producer, single consumer
#[derive(Clone, Debug)]
pub struct Addr<T> {
    tx: mpsc::UnboundedSender<Message<T>>,
}

// Impl the send function which can then be used to send a shut down message
/*
impl<T> ShutdownReceiver for Addr<T>
where
    T: From<Shutdown> + std::marker::Send, // FIXME: Check if that is an actual solution
{
    fn send<'a>(
        &'a self,
        message: Shutdown,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.send(message.into()).map(|_| ()))
    }

    fn foo(&self) -> Box<Self> {
        Bow::new(self.clone())
    }
}
*/

// Name send chosen to keep it compatible with the existing code that is invoking it
impl<T> Addr<T> {
    pub async fn send(&self, data: T) -> Result<bool, SendError> {
        let (responder, rx) = oneshot::channel(); // Generate the oneshot channel
        let message = Message { data, responder }; // Construct the message
        self.tx.send(message).map_err(|_| SendError)?; // Send the message and return an error if it fails
        rx.await.map_err(|_| SendError) // await the result and return it, if anything goes wrong, throw a SendError
    }
}

// Unchanged

pub struct Healthcheck {
    is_shutting_down: bool,
    config: Arc<Config>,
}

impl Healthcheck {
    // Unchanged
    pub fn new(config: Arc<Config>) -> Self {
        Healthcheck {
            is_shutting_down: false,
            config,
        }
    }

    async fn handle_is_healthy(&mut self, message: IsHealthy) -> bool {
        let upstream = UpstreamRelay::from_registry(); // Legacy, still needed for now

        if self.config.relay_mode() == RelayMode::Managed {
            let fut = upstream.send(IsNetworkOutage).compat();
            tokio::spawn(async move {
                if let Ok(is_outage) = fut.await {
                    metric!(gauge(RelayGauges::NetworkOutage) = if is_outage { 1 } else { 0 });
                }
            });
        }

        match message {
            IsHealthy::Liveness => true, // Liveness always returns true
            IsHealthy::Readiness => {
                // If we are shutting down we are no longer Ready
                if self.is_shutting_down {
                    return false;
                }

                // If we need authentication check that we have authentication
                if self.config.requires_auth()
                    && !upstream
                        .send(IsAuthenticated)
                        .compat() // Convert from the old futures to the new futures
                        .await
                        .unwrap_or(false)
                {
                    return false;
                }

                Aggregator::from_registry() // Still legacy?
                    .send(AcceptsMetrics)
                    .compat() // Convert from the old futures to the new futures
                    .await
                    .unwrap_or(false)
            }
        }
    }

    fn handle_shutdown(&mut self) -> bool {
        self.is_shutting_down = true;
        true
    }

    async fn handle(&mut self, message: HealthCheckMessage) -> bool {
        match message {
            HealthCheckMessage::Health(message) => self.handle_is_healthy(message).await,
            HealthCheckMessage::Shutdown => self.handle_shutdown(),
        }
    }

    // Start the healthCheck actor, returns an Addr so that it can be reached
    pub fn start(mut self) -> Addr<HealthCheckMessage> {
        // Make the channel to use for communication
        let (tx, mut rx) = mpsc::unbounded_channel::<Message<_>>();
        let addr = Addr { tx: tx.clone() };
        *ADDRESS.lock().unwrap() = Some(addr.clone());

        tokio::spawn(async move {
            // While incoming messages are still being received
            while let Some(message) = rx.recv().await {
                let response = self.handle(message.data).await;
                message.responder.send(response).ok();
            }
        });

        // let mut wrx = Controller::subscribe_v2(); <- this works but could it be because we don't await it?

        // When receiving a shutdown message forward it to our mpsc channel
        tokio::spawn(async move {
            // Get the receiving end of the watch channel from the Controller
            // let _guard = handle.enter();
            let mut wrx = Controller::subscribe_v2().await; // <-  Is this were the error comes from

            loop {
                if wrx.changed().await.is_ok() && wrx.borrow().is_none() {
                    continue;
                }
                let (responder, _) = oneshot::channel();
                let _ = tx.send(Message {
                    data: HealthCheckMessage::Shutdown,
                    responder,
                });
                break;
            }
        });
        addr
    }

    // Maybe give back our own Addr and say that it can be converted into the actix Addr?
    // Because Healthcheck is not an actor so doesn't work?
    // So escentially need to make a converter? or leave it an Actor
    pub fn from_registry() -> Addr<HealthCheckMessage> {
        // TODO: Here need to return the address from the HealthCheck
        // HTF do we do this, surely we now need to make a registry
        let guard = ADDRESS.lock().unwrap();
        match *guard {
            Some(ref addr) => addr.clone(),
            None => panic!(),
        }
    }
}

/*
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
*/

// - More sophisticated solution -
/*
trait Handler<T> {
    type Result;

    fn handle<'a>(
        &'a mut self,
        message: T,
    ) -> Pin<Box<dyn std::future::Future<Output = Self::Result> + Send + 'a>>;
}

impl Handler<Shutdown> for Healthcheck {
    type Result = Result<(), ()>;

    fn handle<'a>(
        &'a mut self,
        _message: Shutdown,
    ) -> Pin<Box<dyn std::future::Future<Output = Self::Result> + Send + 'a>> {
        let result_future = async move {
            self.is_shutting_down = true;
            Ok(())
        };
        Box::pin(result_future)
    }
}
*/

#[derive(Clone, Debug)]
pub enum IsHealthy {
    /// Check if the Relay is alive at all.
    Liveness,
    /// Check if the Relay is in a state where the load balancer should route traffic to it (i.e.
    /// it's both live/alive and not too busy).
    Readiness,
}

#[derive(Clone, Debug)]
pub enum HealthCheckMessage {
    Health(IsHealthy),
    Shutdown,
}

// Allows for shutdown to be converted to HealthCheckMessage
// Needed so that that the shutdown message can be send to multiple actors in the same way
impl From<Shutdown> for HealthCheckMessage {
    fn from(_: Shutdown) -> Self {
        HealthCheckMessage::Shutdown
    }
}

impl From<IsHealthy> for HealthCheckMessage {
    fn from(is_healthy: IsHealthy) -> Self {
        Self::Health(is_healthy)
    }
}

/*
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
                    return Box::new(future::ok(false));
                }

                let is_aggregator_full = Aggregator::from_registry()
                    .send(AcceptsMetrics)
                    .map_err(|_| ());
                let is_authenticated: Self::Result = if self.config.requires_auth() {
                    Box::new(upstream.send(IsAuthenticated).map_err(|_| ()))
                } else {
                    Box::new(future::ok(true))
                };

                Box::new(
                    is_aggregator_full
                        .join(is_authenticated)
                        .map(|(a, b)| a && b),
                )
            }
        }
    }
}
*/
