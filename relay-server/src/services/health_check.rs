use std::sync::Arc;

use relay_config::Config;
use relay_system::{Addr, AsyncResponse, Controller, FromMessage, Interface, Sender, Service};
use std::future::Future;
use tokio::sync::watch;
use tokio::time::{Instant, timeout};

use crate::services::buffer::PartitionedEnvelopeBuffer;
use crate::services::metrics::RouterHandle;
use crate::services::upstream::{IsAuthenticated, UpstreamRelay};
use crate::statsd::RelayTimers;
use crate::utils::{MemoryCheck, MemoryChecker};

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
struct StatusUpdate {
    status: Status,
    instant: Instant,
}

impl StatusUpdate {
    pub fn new(status: Status) -> Self {
        Self {
            status,
            instant: Instant::now(),
        }
    }
}

/// Service implementing the [`HealthCheck`] interface.
#[derive(Debug)]
pub struct HealthCheckService {
    config: Arc<Config>,
    memory_checker: MemoryChecker,
    aggregator: Option<RouterHandle>,
    upstream_relay: Addr<UpstreamRelay>,
    envelope_buffer: Arc<PartitionedEnvelopeBuffer>,
}

impl HealthCheckService {
    /// Creates a new instance of the HealthCheck service.
    pub fn new(
        config: Arc<Config>,
        memory_checker: MemoryChecker,
        aggregator: Option<RouterHandle>,
        upstream_relay: Addr<UpstreamRelay>,
        envelope_buffer: Arc<PartitionedEnvelopeBuffer>,
    ) -> Self {
        Self {
            config,
            memory_checker,
            aggregator,
            upstream_relay,
            envelope_buffer,
        }
    }

    fn system_memory_probe(&mut self) -> Status {
        let config = self.config.current();
        if let MemoryCheck::Exceeded(memory) = self.memory_checker.check_memory_percent() {
            relay_log::error!(
                "Not enough memory, {} / {} ({:.2}% >= {:.2}%)",
                memory.used,
                memory.total,
                memory.used_percent() * 100.0,
                config.health_max_memory_watermark_percent() * 100.0,
            );
            return Status::Unhealthy;
        }

        if let MemoryCheck::Exceeded(memory) = self.memory_checker.check_memory_bytes() {
            relay_log::error!(
                "Not enough memory, {} / {} ({} >= {})",
                memory.used,
                memory.total,
                memory.used,
                config.health_max_memory_watermark_bytes(),
            );
            return Status::Unhealthy;
        }

        Status::Healthy
    }

    async fn auth_probe(&self) -> Status {
        if !self.config.current().requires_auth() {
            return Status::Healthy;
        }

        self.upstream_relay
            .send(IsAuthenticated)
            .await
            .map_or(Status::Unhealthy, Status::from)
    }

    async fn aggregator_probe(&self) -> Status {
        self.aggregator
            .as_ref()
            .map(|agg| Status::from(agg.can_accept_metrics()))
            .unwrap_or(Status::Healthy)
    }

    async fn spool_health_probe(&self) -> Status {
        match self.envelope_buffer.has_capacity() {
            true => Status::Healthy,
            false => Status::Unhealthy,
        }
    }

    async fn probe(&self, name: &'static str, fut: impl Future<Output = Status>) -> Status {
        match timeout(self.config.current().health_probe_timeout(), fut).await {
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

    async fn check_readiness(&mut self) -> Status {
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

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let (update_tx, update_rx) = watch::channel(StatusUpdate::new(Status::Unhealthy));
        let config = self.config.current();
        let check_interval = config.health_refresh_interval();
        // Add 10% buffer to the internal timeouts to avoid race conditions.
        let status_timeout = (check_interval + config.health_probe_timeout()).mul_f64(1.1);

        let update_task = relay_system::spawn!(async move {
            let shutdown = Controller::shutdown_handle();

            while shutdown.get().is_none() {
                tokio::select! {
                    _ = update_tx.closed() => break,
                    _ = async {
                        let check_result = relay_statsd::metric!(
                            timer(RelayTimers::HealthCheckDuration),
                            type = "readiness",
                            { self.check_readiness().await }
                        );
                        let _ = update_tx.send(StatusUpdate::new(check_result));
                        tokio::time::sleep(check_interval).await;
                    } => {}
                }
            }

            // Shutdown marks readiness health check as unhealthy.
            update_tx.send(StatusUpdate::new(Status::Unhealthy)).ok();
        });

        while let Some(HealthCheck(message, sender)) = rx.recv().await {
            let update = update_rx.borrow();

            sender.send(if matches!(message, IsHealthy::Liveness) {
                Status::Healthy
            } else if update.instant.elapsed() >= status_timeout {
                Status::Unhealthy
            } else {
                update.status
            });
        }

        // Join on the update task:
        drop(update_rx);
        let _ = update_task.await;
    }
}

#[cfg(test)]
mod tests {
    use relay_system::TokioServiceSpawn;

    use crate::MemoryStat;
    use crate::services::global_config;
    use crate::services::projects::cache::ProjectCacheHandle;

    use super::*;

    fn dummy_service(upstream: Addr<UpstreamRelay>) -> HealthCheckService {
        let config = Arc::new(
            Config::from_json_value(serde_json::json!({
                "health": { "max_memory_percent": 2.0 }
            }))
            .unwrap(),
        );
        assert!(config.current().requires_auth());
        let memory_stat = MemoryStat::default();
        let (_, global_config_rx) = watch::channel(global_config::Status::Pending);
        let buffer = PartitionedEnvelopeBuffer::create(
            1.try_into().unwrap(),
            config.clone(),
            memory_stat.clone(),
            global_config_rx,
            ProjectCacheHandle::for_test(),
            Addr::dummy(),
            Addr::dummy(),
            &TokioServiceSpawn,
        );
        HealthCheckService::new(
            config.clone(),
            MemoryChecker::new(memory_stat, config),
            None,
            upstream,
            buffer,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_shutdown_on_channel_close_joins_readiness_task() {
        let (upstream, mut upstream_rx) = Addr::custom();
        let service = dummy_service(upstream);
        let buffer = Arc::downgrade(&service.envelope_buffer);
        let (addr, rx) = relay_system::channel(HealthCheckService::name());
        let mut run = std::pin::pin!(service.run(rx));
        assert!(futures::poll!(&mut run).is_pending());

        // Queued requests must still receive responses before the worker is joined.
        let response = addr.send(IsHealthy::Liveness);
        drop(addr);
        run.await;
        assert!(matches!(response.await.unwrap(), Status::Healthy));
        assert!(buffer.upgrade().is_none());
        while upstream_rx.recv().await.is_some() {}
    }

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
}
