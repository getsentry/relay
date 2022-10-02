use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::SystemService;

use relay_config::{Config, RelayMode};
use relay_metrics::AcceptsMetrics;
use relay_statsd::metric;
use relay_system::{
    compat, Addr, AsyncResponse, Controller, FromMessage, Interface, Sender, Service,
};

use crate::actors::upstream::{IsAuthenticated, IsNetworkOutage, UpstreamRelay};
use crate::service::REGISTRY;
use crate::statsd::RelayGauges;

/// Checks whether Relay is alive and healthy based on its variant.
#[derive(Clone, Copy, Debug)]
pub enum IsHealthy {
    /// Check if the Relay is alive at all.
    Liveness,
    /// Check if the Relay is in a state where the load balancer should route traffic to it (i.e.
    /// it's both live/alive and not too busy).
    Readiness,
}

/// Service interface for the [`IsHealthy`] message.
pub struct HealthCheck(IsHealthy, Sender<bool>);

impl HealthCheck {
    /// Returns the [`Addr`] of the [`HealthCheck`] service.
    ///
    /// Prior to using this, the service must be started using [`HealthCheckService::start`].
    ///
    /// # Panics
    ///
    /// Panics if the service was not started using [`HealthCheckService::start`] prior to this
    /// being used.
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().health_check.clone()
    }
}

impl Interface for HealthCheck {}

impl FromMessage<IsHealthy> for HealthCheck {
    type Response = AsyncResponse<bool>;

    fn from_message(message: IsHealthy, sender: Sender<bool>) -> Self {
        Self(message, sender)
    }
}

/// Service implementing the [`HealthCheck`] interface.
#[derive(Debug)]
pub struct HealthCheckService {
    is_shutting_down: AtomicBool,
    config: Arc<Config>,
}

impl HealthCheckService {
    /// Creates a new instance of the HealthCheck service.
    ///
    /// The service does not run. To run the service, use [`start`](Self::start).
    pub fn new(config: Arc<Config>) -> Self {
        HealthCheckService {
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

                // NOTE: usually this unwrap is hidden in the `from_registry` function on the
                // specific service or actor. But since the `AggregatorService` is in the different
                // crate we cannot easily implement that pattarn and for now we have to use
                // REGISTRY dirrectly and get the aggregator service out of it.
                let aggregator = REGISTRY
                    .get()
                    .map(|registry| registry.aggregator.clone())
                    .unwrap();
                aggregator.send(AcceptsMetrics).await.unwrap_or_default()
            }
        }
    }

    async fn handle_message(&self, message: HealthCheck) {
        let HealthCheck(message, sender) = message;
        let response = self.handle_is_healthy(message).await;
        sender.send(response);
    }
}

impl Service for HealthCheckService {
    type Interface = HealthCheck;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let service = Arc::new(self);

        let main_service = service.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let service = main_service.clone();
                tokio::spawn(async move { service.handle_message(message).await });
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
    }
}
