use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use relay_config::{Config, RelayMode};
use relay_metrics::{AcceptsMetrics, Aggregator};
use relay_statsd::metric;
use relay_system::{Addr, AsyncResponse, Controller, FromMessage, Interface, Sender, Service};
use tokio::time::Instant;

use crate::services::project_cache::{ProjectCache, SpoolHealth, SpoolHealthWrapper};
use crate::services::upstream::{IsAuthenticated, IsNetworkOutage, UpstreamRelay};
use crate::statsd::{RelayGauges, RelayTimers};

/// Checks whether Relay is alive and healthy based on its variant.
#[derive(Clone, Copy, Debug, serde::Deserialize)]
pub enum IsHealthy {
    /// Check if the Relay is alive at all.
    #[serde(rename = "live")]
    Liveness,
    /// Check if the Relay is in a state where the load balancer should route traffic to it (i.e.
    /// it's both live/alive and not too busy).
    #[serde(rename = "ready")]
    Readiness,
}

impl IsHealthy {
    pub fn variant(&self) -> &str {
        match self {
            IsHealthy::Liveness => "liveness",
            IsHealthy::Readiness => "readiness",
        }
    }
}

pub struct IsHealthyWrapper {
    pub kind: IsHealthy,
    pub received: Instant,
}

/// Service interface for the [`IsHealthy`] message.
pub struct HealthCheck(IsHealthyWrapper, Sender<bool>);

impl Interface for HealthCheck {}

impl FromMessage<IsHealthyWrapper> for HealthCheck {
    type Response = AsyncResponse<bool>;

    fn from_message(message: IsHealthyWrapper, sender: Sender<bool>) -> Self {
        Self(message, sender)
    }
}

/// Service implementing the [`HealthCheck`] interface.
#[derive(Debug)]
pub struct HealthCheckService {
    is_shutting_down: AtomicBool,
    config: Arc<Config>,
    aggregator: Addr<Aggregator>,
    upstream_relay: Addr<UpstreamRelay>,
    project_cache: Addr<ProjectCache>,
}

impl HealthCheckService {
    /// Creates a new instance of the HealthCheck service.
    ///
    /// The service does not run. To run the service, use [`start`](Self::start).
    pub fn new(
        config: Arc<Config>,
        aggregator: Addr<Aggregator>,
        upstream_relay: Addr<UpstreamRelay>,
        project_cache: Addr<ProjectCache>,
    ) -> Self {
        HealthCheckService {
            is_shutting_down: AtomicBool::new(false),
            config,
            aggregator,
            upstream_relay,
            project_cache,
        }
    }

    async fn handle_is_healthy(&self, message: IsHealthy) -> bool {
        let upstream = self.upstream_relay.clone();

        if self.config.relay_mode() == RelayMode::Managed {
            let fut = upstream.send(IsNetworkOutage);
            tokio::spawn(async move {
                if let Ok(is_outage) = fut.await {
                    metric!(gauge(RelayGauges::NetworkOutage) = u64::from(is_outage));
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
                    && !upstream.send(IsAuthenticated).await.unwrap_or(false)
                {
                    return false;
                }

                if !self
                    .aggregator
                    .send(AcceptsMetrics)
                    .await
                    .unwrap_or_default()
                {
                    return false;
                }

                let project_cache_sent = Instant::now();
                let result = self
                    .project_cache
                    .send(SpoolHealthWrapper {
                        message: SpoolHealth,
                        sent_instant: project_cache_sent,
                    })
                    .await
                    .unwrap_or_default();
                relay_statsd::metric!(
                    timer(RelayTimers::HealthcheckServiceProjectcacheResponseDuration) =
                        project_cache_sent.elapsed(),
                );

                result
            }
        }
    }

    async fn handle_message(&self, message: HealthCheck) {
        let handle_start = Instant::now();
        let HealthCheck(message, sender) = message;
        relay_statsd::metric!(
            timer(RelayTimers::HealthcheckServiceReceivedDelay) = message.received.elapsed(),
        );

        let response = self.handle_is_healthy(message.kind).await;
        relay_statsd::metric!(
            timer(RelayTimers::HealthcheckServiceHandleDuration) = handle_start.elapsed(),
            kind = message.kind.variant()
        );
        sender.send(response);
    }
}

impl Service for HealthCheckService {
    type Interface = HealthCheck;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let service = Arc::new(self);

        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();

            loop {
                tokio::select! {
                    biased;

                    Some(message) = rx.recv() => {
                        let service = service.clone();
                        tokio::spawn(async move { service.handle_message(message).await });
                    }
                    _ = shutdown.notified() => {
                        service.is_shutting_down.store(true, Ordering::Relaxed);
                    }
                }
            }
        });
    }
}
