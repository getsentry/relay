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
    static ref ADDRESS: RwLock<Option<Addr<Healthcheck>>> = RwLock::new(None);
}

/// Our definition of a service.
///
/// Services are much like actors: they receive messages from an inbox and handles them one
/// by one.  Services are free to concurrently process these messages or not, most probably
/// should.
///
/// Messages always have a response which will be sent once the message is handled by the
/// service.
pub trait Service {
    /// The envelope is what is sent to the inbox of this service.
    ///
    /// It is an enum of all the message types that can be handled by this service together
    /// with the response [sender](oneshot::Sender) for each message.
    type Envelope: Send + 'static;
}

/// A message which can be sent to a service.
///
/// Messages have an associated `Response` type and can be unconditionally converted into
/// the envelope type of their [`Service`].
pub trait ServiceMessage<S: Service> {
    type Response: Send + 'static;

    fn into_envelope(self) -> (S::Envelope, oneshot::Receiver<Self::Response>);
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

/// The address for a [`Service`].
///
/// The address of a [`Service`] allows you to [send](Addr::send) messages to the service as
/// long as the service is running.  It can be freely cloned.
#[derive(Debug)]
pub struct Addr<S: Service> {
    tx: mpsc::UnboundedSender<S::Envelope>,
}

// Manually derive clone since we do not require `S: Clone` and the Clone derive adds this
// constraint.
impl<S: Service> Clone for Addr<S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<S: Service> Addr<S> {
    /// Sends an asynchronous message to the service and waits for the response.
    ///
    /// The result of the message does not have to be awaited. The message will be delivered and
    /// handled regardless. The communication channel with the service is unbounded, so backlogs
    /// could occur when sending too many messages.
    ///
    /// Sending the message can fail with `Err(SendError)` if the service has shut down.
    pub async fn send<M>(&self, message: M) -> Result<M::Response, SendError>
    where
        M: ServiceMessage<S>,
    {
        let (envelope, response_rx) = message.into_envelope();
        self.tx.send(envelope).map_err(|_| SendError)?;
        response_rx.await.map_err(|_| SendError)
    }
}

#[derive(Debug)]
pub struct Healthcheck {
    is_shutting_down: AtomicBool,
    config: Arc<Config>,
}

impl Service for Healthcheck {
    type Envelope = HealthcheckEnvelope;
}

impl Healthcheck {
    /// Returns the [`Addr`] of the [`Healthcheck`] actor.
    ///
    /// Prior to using this, the service must be started using [`Healthcheck::start`].
    ///
    /// # Panics
    ///
    /// Panics if the service was not started using [`Healthcheck::start`] prior to this being used.
    pub fn from_registry() -> Addr<Self> {
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
    pub fn start(self) -> Addr<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<HealthcheckEnvelope>();

        let addr = Addr { tx };
        *ADDRESS.write() = Some(addr.clone());

        let service = Arc::new(self);
        let main_service = service.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let service = main_service.clone();

                tokio::spawn(async move {
                    match message {
                        HealthcheckEnvelope::IsHealthy(msg, response_tx) => {
                            let response = service.handle_is_healthy(msg).await;
                            response_tx.send(response).ok()
                        }
                    };
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

impl ServiceMessage<Healthcheck> for IsHealthy {
    type Response = bool;

    fn into_envelope(self) -> (HealthcheckEnvelope, oneshot::Receiver<Self::Response>) {
        let (tx, rx) = oneshot::channel();
        (HealthcheckEnvelope::IsHealthy(self, tx), rx)
    }
}

/// All the message types which can be sent to the [`Healthcheck`] actor.
#[derive(Debug)]
pub enum HealthcheckEnvelope {
    IsHealthy(IsHealthy, oneshot::Sender<bool>),
}
