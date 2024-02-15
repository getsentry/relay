use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use relay_config::{Config, RelayMode};
use relay_metrics::{AcceptsMetrics, Aggregator};
use relay_statsd::metric;
use relay_system::{Addr, AsyncResponse, Controller, FromMessage, Interface, Sender, Service};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};

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

impl IsHealthy {
    /// Returns the name of the variant, either `liveness` or `healthy`.
    fn variant(&self) -> &'static str {
        match self {
            Self::Liveness => "liveness",
            Self::Readiness => "readiness",
        }
    }
}

/// Service interface for the [`IsHealthy`] message.
pub struct HealthCheck(IsHealthy, Sender<bool>);

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
    aggregator: Addr<Aggregator>,
    upstream_relay: Addr<UpstreamRelay>,
    project_cache: Addr<ProjectCache>,
    system: Mutex<SystemInfo>,
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
            system: Mutex::new(SystemInfo::new(config.health_sys_info_refresh_interval())),
            config,
            aggregator,
            upstream_relay,
            project_cache,
        }
    }

    async fn system_memory_probe(&self) -> Status {
        let memory = {
            self.system
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .memory()
        };

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

    async fn project_cache_probe(&self) -> Status {
        self.project_cache
            .send(SpoolHealth)
            .await
            .map_or(Status::Unhealthy, Status::from)
    }

    async fn handle_is_healthy(&self, message: IsHealthy) -> Status {
        let upstream = self.upstream_relay.clone();

        if self.config.relay_mode() == RelayMode::Managed {
            let fut = upstream.send(IsNetworkOutage);
            tokio::spawn(async move {
                if let Ok(is_outage) = fut.await {
                    metric!(gauge(RelayGauges::NetworkOutage) = u64::from(is_outage));
                }
            });
        }

        if matches!(message, IsHealthy::Liveness) {
            return Status::Healthy;
        }

        if self.is_shutting_down.load(Ordering::Relaxed) {
            return Status::Unhealthy;
        }

        let (sys_mem, auth, agg, proj) = tokio::join!(
            self.system_memory_probe(),
            self.auth_probe(),
            self.aggregator_probe(),
            self.project_cache_probe(),
        );

        Status::combined(&[sys_mem, auth, agg, proj])
    }

    async fn handle_message(&self, message: HealthCheck) {
        let HealthCheck(message, sender) = message;

        let ty = message.variant();
        let response = relay_statsd::metric!(
            timer(RelayTimers::HealthCheckDuration),
            type = ty,
            { self.handle_is_healthy(message).await }
        );

        sender.send(response.to_bool());
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

#[derive(Debug, Copy, Clone)]
enum Status {
    Healthy,
    Unhealthy,
}

impl Status {
    fn combined(s: &[Status]) -> Self {
        s.iter()
            .copied()
            .reduce(Self::combine)
            .unwrap_or(Self::Unhealthy)
    }

    fn combine(self, other: Self) -> Self {
        if matches!(self, Self::Unhealthy) || matches!(other, Self::Unhealthy) {
            Self::Unhealthy
        } else {
            Self::Healthy
        }
    }

    fn to_bool(self) -> bool {
        matches!(self, Self::Healthy)
    }
}

impl From<bool> for Status {
    fn from(value: bool) -> Self {
        match value {
            true => Self::Healthy,
            false => Self::Unhealthy,
        }
    }
}

#[derive(Debug)]
struct SystemInfo {
    system: System,
    last_refresh: Instant,
    refresh_interval: Duration,
}

impl SystemInfo {
    pub fn new(refresh_interval: Duration) -> Self {
        let system = System::new_with_specifics(
            RefreshKind::new().with_memory(MemoryRefreshKind::everything()),
        );

        Self {
            system,
            last_refresh: Instant::now(),
            refresh_interval,
        }
    }

    pub fn memory(&mut self) -> Memory {
        self.refresh();

        // Use the cgroup if available in case Relay is running in a container.
        if let Some(cgroup) = self.system.cgroup_limits() {
            Memory {
                used: cgroup.total_memory.saturating_sub(cgroup.free_memory),
                total: cgroup.total_memory,
            }
        } else {
            Memory {
                used: self.system.used_memory(),
                total: self.system.total_memory(),
            }
        }
    }

    fn refresh(&mut self) {
        let now = Instant::now();
        if (now - self.last_refresh) >= self.refresh_interval {
            self.system
                .refresh_memory_specifics(MemoryRefreshKind::new().with_ram());

            self.last_refresh = Instant::now();
        }
    }
}

#[derive(Debug)]
pub struct Memory {
    pub used: u64,
    pub total: u64,
}

impl Memory {
    pub fn used_percent(&self) -> f32 {
        (self.used as f32 / self.total as f32).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
