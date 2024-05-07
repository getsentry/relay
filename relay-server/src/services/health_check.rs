use std::sync::Arc;

use relay_config::{Config, RelayMode};
use relay_metrics::{AcceptsMetrics, Aggregator};
use relay_statsd::metric;
use relay_system::{
    Addr, AsyncResponse, Controller, FromMessage, Interface, Sender, Service, ShutdownHandle,
};
use std::future::Future;
use sysinfo::{MemoryRefreshKind, System};
use tokio::sync::watch;
use tokio::time::{timeout, Instant};

use crate::services::project_cache::{ProjectCache, SpoolHealth};
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

/// Health check status.
#[derive(Debug, Copy, Clone)]
pub enum Status {
    /// Relay is healthy.
    Healthy,
    /// Relay is unhealthy.
    Unhealthy,
}

impl From<bool> for Status {
    fn from(value: bool) -> Self {
        match value {
            true => Self::Healthy,
            false => Self::Unhealthy,
        }
    }
}

impl FromIterator<Status> for Status {
    fn from_iter<T: IntoIterator<Item = Status>>(iter: T) -> Self {
        let healthy = iter
            .into_iter()
            .all(|status| matches!(status, Self::Healthy));
        Self::from(healthy)
    }
}

/// Service interface for the [`IsHealthy`] message.
pub struct HealthCheck(IsHealthy, Sender<Status>);

impl Interface for HealthCheck {}

impl FromMessage<IsHealthy> for HealthCheck {
    type Response = AsyncResponse<Status>;

    fn from_message(message: IsHealthy, sender: Sender<Status>) -> Self {
        Self(message, sender)
    }
}

#[derive(Debug)]
struct Statuses {
    live: Status,
    ready: Status,
    instant: Instant,
}

impl Statuses {
    pub fn healthy() -> Self {
        Self {
            live: Status::Healthy,
            ready: Status::Healthy,
            instant: Instant::now(),
        }
    }
}

/// Service implementing the [`HealthCheck`] interface.
#[derive(Debug)]
pub struct HealthCheckService {
    config: Arc<Config>,
    aggregator: Addr<Aggregator>,
    upstream_relay: Addr<UpstreamRelay>,
    project_cache: Addr<ProjectCache>,
    system: System,
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
        Self {
            system: System::new(),
            aggregator,
            upstream_relay,
            project_cache,
            config,
        }
    }

    fn system_memory_probe(&mut self) -> Status {
        self.system
            .refresh_memory_specifics(MemoryRefreshKind::new().with_ram());

        // Use the cgroup if available in case Relay is running in a container.
        let memory = match self.system.cgroup_limits() {
            Some(cgroup) => Memory {
                used: cgroup.total_memory.saturating_sub(cgroup.free_memory),
                total: cgroup.total_memory,
            },
            None => Memory {
                used: self.system.used_memory(),
                total: self.system.total_memory(),
            },
        };

        metric!(gauge(RelayGauges::SystemMemoryUsed) = memory.used);
        metric!(gauge(RelayGauges::SystemMemoryTotal) = memory.total);

        if memory.used_percent() >= self.config.health_max_memory_watermark_percent() {
            relay_log::error!(
                "Not enough memory, {} / {} ({:.2}% >= {:.2}%)",
                memory.used,
                memory.total,
                memory.used_percent() * 100.0,
                self.config.health_max_memory_watermark_percent() * 100.0,
            );
            return Status::Unhealthy;
        }

        if memory.used > self.config.health_max_memory_watermark_bytes() {
            relay_log::error!(
                "Not enough memory, {} / {} ({} >= {})",
                memory.used,
                memory.total,
                memory.used,
                self.config.health_max_memory_watermark_bytes(),
            );
            return Status::Unhealthy;
        }

        Status::Healthy
    }

    async fn auth_probe(&self) -> Status {
        if !self.config.requires_auth() {
            return Status::Healthy;
        }

        self.upstream_relay
            .send(IsAuthenticated)
            .await
            .map_or(Status::Unhealthy, Status::from)
    }

    async fn aggregator_probe(&self) -> Status {
        self.aggregator
            .send(AcceptsMetrics)
            .await
            .map_or(Status::Unhealthy, Status::from)
    }

    async fn spool_health_probe(&self) -> Status {
        self.project_cache
            .send(SpoolHealth)
            .await
            .map_or(Status::Unhealthy, Status::from)
    }

    async fn probe(&self, name: &'static str, fut: impl Future<Output = Status>) -> Status {
        match timeout(self.config.health_probe_timeout(), fut).await {
            Err(_) => {
                relay_log::error!("Health check probe '{name}' timed out");
                Status::Unhealthy
            }
            Ok(Status::Unhealthy) => {
                relay_log::error!("Health check probe '{name}' failed");
                Status::Unhealthy
            }
            Ok(Status::Healthy) => Status::Healthy,
        }
    }

    async fn check_readiness(&mut self, shutdown: &ShutdownHandle) -> Status {
        // Internal metric that we need to be logged in recurring intervals. This is a form of
        // health check, but it does not contribute to the status of this service.
        if self.config.relay_mode() == RelayMode::Managed {
            if let Ok(is_outage) = self.upstream_relay.send(IsNetworkOutage).await {
                metric!(gauge(RelayGauges::NetworkOutage) = u64::from(is_outage));
            }
        }

        if shutdown.get().is_some() {
            return Status::Unhealthy;
        }

        // System memory is sync and requires mutable access, but we still want to log errors.
        let sys_mem = self.system_memory_probe();

        let (sys_mem, auth, agg, proj) = tokio::join!(
            self.probe("system memory", async { sys_mem }),
            self.probe("auth", self.auth_probe()),
            self.probe("aggregator", self.aggregator_probe()),
            self.probe("spool health", self.spool_health_probe()),
        );

        Status::from_iter([sys_mem, auth, agg, proj])
    }
}

impl Service for HealthCheckService {
    type Interface = HealthCheck;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let (status_tx, status_rx) = watch::channel(Statuses::healthy());
        let check_interval = self.config.health_sys_info_refresh_interval();
        // Add 10% buffer to the internal timeouts to avoid race conditions.
        let status_timeout = (check_interval + self.config.health_probe_timeout()).mul_f64(1.1);

        tokio::spawn(async move {
            let shutdown = Controller::shutdown_handle();

            loop {
                let ready = relay_statsd::metric!(
                    timer(RelayTimers::HealthCheckDuration),
                    type = "readiness",
                    { self.check_readiness(&shutdown).await }
                );

                let _ = status_tx.send(Statuses {
                    live: Status::Healthy,
                    ready,
                    instant: Instant::now(),
                });

                tokio::time::sleep(check_interval).await;
            }
        });

        tokio::spawn(async move {
            while let Some(HealthCheck(message, sender)) = rx.recv().await {
                let statuses = status_rx.borrow();
                let is_valid = statuses.instant.elapsed() < status_timeout;

                sender.send(match (is_valid, message) {
                    (false, _) => Status::Unhealthy,
                    (_, IsHealthy::Liveness) => statuses.live,
                    (_, IsHealthy::Readiness) => statuses.ready,
                });
            }
        });
    }
}

/// A memory measurement.
#[derive(Debug)]
struct Memory {
    pub used: u64,
    pub total: u64,
}

impl Memory {
    /// Amount of used RAM in percent `0.0` to `1.0`.
    pub fn used_percent(&self) -> f32 {
        (self.used as f32 / self.total as f32).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status() {
        assert!(matches!(Status::from(true), Status::Healthy));
        assert!(matches!(Status::from(false), Status::Unhealthy));

        let s = [Status::Unhealthy, Status::Unhealthy].into_iter().collect();
        assert!(matches!(s, Status::Unhealthy));

        let s = [Status::Unhealthy, Status::Healthy].into_iter().collect();
        assert!(matches!(s, Status::Unhealthy));

        let s = [Status::Healthy, Status::Unhealthy].into_iter().collect();
        assert!(matches!(s, Status::Unhealthy));

        let s = [Status::Unhealthy].into_iter().collect();
        assert!(matches!(s, Status::Unhealthy));

        // The iterator should short circuit.
        let s = std::iter::repeat(Status::Unhealthy).collect();
        assert!(matches!(s, Status::Unhealthy));

        let s = [Status::Healthy, Status::Healthy].into_iter().collect();
        assert!(matches!(s, Status::Healthy));

        let s = [Status::Healthy].into_iter().collect();
        assert!(matches!(s, Status::Healthy));

        let s = [].into_iter().collect();
        assert!(matches!(s, Status::Healthy));
    }

    #[test]
    fn test_memory_used_percent_total_0() {
        let memory = Memory {
            used: 100,
            total: 0,
        };
        assert_eq!(memory.used_percent(), 1.0);
    }

    #[test]
    fn test_memory_used_percent_zero() {
        let memory = Memory {
            used: 0,
            total: 100,
        };
        assert_eq!(memory.used_percent(), 0.0);
    }

    #[test]
    fn test_memory_used_percent_half() {
        let memory = Memory {
            used: 50,
            total: 100,
        };
        assert_eq!(memory.used_percent(), 0.5);
    }
}
