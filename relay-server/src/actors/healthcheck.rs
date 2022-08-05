use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::SystemService;
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};

use relay_config::{Config, RelayMode};
use relay_metrics::{AcceptsMetrics, Aggregator};
use relay_statsd::metric;
use relay_system::{compat, Controller};

use crate::actors::upstream::{IsAuthenticated, IsNetworkOutage, UpstreamRelay};
use crate::statsd::RelayGauges;

lazy_static::lazy_static! {
    /// Singleton of the `Healthcheck` service.
    static ref ADDRESS: RwLock<Option<Addr<HealthcheckMessage>>> = RwLock::new(None);
}

/// Internal wrapper of a message sent through an `Addr` with return channel.
#[derive(Debug)]
struct Message<T> {
    data: T,
    // TODO(tobias): This is hard-coded to return `bool`.
    // Might need some associated types to make this work
    responder: oneshot::Sender<bool>,
}

/// An error when [sending](Addr::send) a message to a service fails.
#[derive(Clone, Copy, Debug)]
pub struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to send message to service")
    }
}

impl Error for SendError {}

/// Channel for sending public messages into a service.
///
/// To send a message, use [`Addr::send`].
#[derive(Clone, Debug)]
pub struct Addr<T> {
    tx: mpsc::UnboundedSender<Message<T>>,
}

impl<T> Addr<T> {
    /// Sends an asynchronous message to the service and waits for the response.
    ///
    /// The result of the message does not have to be awaited. The message will be delivered and
    /// handled regardless. The communication channel with the service is unbounded, so backlogs
    /// could occur when sending too many messages.
    ///
    /// Sending the message can fail with `Err(SendError)` if the service has shut down.
    pub async fn send(&self, data: T) -> Result<bool, SendError> {
        let (responder, rx) = oneshot::channel();
        let message = Message { data, responder };
        self.tx.send(message).map_err(|_| SendError)?;
        rx.await.map_err(|_| SendError)
    }
}

#[derive(Debug)]
pub struct Healthcheck {
    is_shutting_down: AtomicBool,
    config: Arc<Config>,
}

impl Healthcheck {
    /// Returns the [`Addr`] of the [`Healthcheck`] actor.
    ///
    /// Prior to using this, the service must be started using [`Healthcheck::start`].
    ///
    /// # Panics
    ///
    /// Panics if the service was not started using [`Healthcheck::start`] prior to this being used.
    pub fn from_registry() -> Addr<HealthcheckMessage> {
        ADDRESS.read().as_ref().unwrap().clone()
    }

    /// Creates a new instance of the Healthcheck service.
    ///
    /// The service does not run. To run the service, use [`start`](Self::start).
    pub fn new(config: Arc<Config>) -> Self {
        Healthcheck {
            is_shutting_down: AtomicBool::new(false),
            config,
        }
    }

    async fn handle_is_healthy(&self, message: IsHealthy) -> bool {
        let upstream = UpstreamRelay::from_registry();

        if self.config.relay_mode() == RelayMode::Managed {
            let fut = compat::send(upstream.clone(), IsNetworkOutage);
            tokio::spawn(async move {
                if let Ok(is_outage) = fut.await {
                    metric!(gauge(RelayGauges::NetworkOutage) = if is_outage { 1 } else { 0 });
                }
            });
        }

        match message {
            IsHealthy::Liveness => true,
            IsHealthy::Readiness => {
                if self.is_shutting_down.load(Ordering::Relaxed) {
                    return false;
                }

                if self.config.requires_auth()
                    && !compat::send(upstream, IsAuthenticated)
                        .await
                        .unwrap_or(false)
                {
                    return false;
                }

                compat::send(Aggregator::from_registry(), AcceptsMetrics)
                    .await
                    .unwrap_or(false)
            }
        }
    }

    /// Start this service, returning an [`Addr`] for communication.
    pub fn start(self) -> Addr<HealthcheckMessage> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Message<HealthcheckMessage>>();

        let addr = Addr { tx };
        *ADDRESS.write() = Some(addr.clone());

        let service = Arc::new(self);
        let main_service = service.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let service = main_service.clone();

                tokio::spawn(async move {
                    let response = match message.data {
                        HealthcheckMessage::Health(data) => service.handle_is_healthy(data).await,
                    };
                    message.responder.send(response).ok();
                });
            }
        });

        // Handle the shutdown signals
        tokio::spawn(async move {
            let mut shutdown_rx = Controller::subscribe_v2().await;

            while shutdown_rx.changed().await.is_ok() {
                if shutdown_rx.borrow_and_update().is_some() {
                    service.is_shutting_down.store(true, Ordering::Relaxed);
                }
            }
        });

        addr
    }
}

#[derive(Clone, Copy, Debug)]
pub enum IsHealthy {
    /// Check if the Relay is alive at all.
    Liveness,
    /// Check if the Relay is in a state where the load balancer should route traffic to it (i.e.
    /// it's both live/alive and not too busy).
    Readiness,
}

/// All the message types which can be sent to the [`Healthcheck`] actor.
#[derive(Clone, Debug)]
pub enum HealthcheckMessage {
    Health(IsHealthy),
}

impl From<IsHealthy> for HealthcheckMessage {
    fn from(is_healthy: IsHealthy) -> Self {
        Self::Health(is_healthy)
    }
}
