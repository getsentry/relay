use std::sync::Arc;

use relay_config::{Config, RelayMode};
use relay_redis::RedisPool;
use relay_statsd::metric;
use relay_system::{Addr, Service};
use tokio::time::interval;

use crate::services::upstream::{IsNetworkOutage, UpstreamRelay};
use crate::statsd::{RelayGauges, TokioGauges};

/// Relay Stats Service.
///
/// Service which collects stats periodically and emits them via statsd.
pub struct RelayStats {
    config: Arc<Config>,
    upstream_relay: Addr<UpstreamRelay>,
    redis_pool: Option<RedisPool>,
}

impl RelayStats {
    pub fn new(
        config: Arc<Config>,
        upstream_relay: Addr<UpstreamRelay>,
        redis_pool: Option<RedisPool>,
    ) -> Self {
        Self {
            config,
            upstream_relay,
            redis_pool,
        }
    }

    async fn tokio_metrics(&self) {
        let m = tokio::runtime::Handle::current().metrics();

        metric!(gauge(TokioGauges::ActiveTasksCount) = m.active_tasks_count() as u64);
        metric!(gauge(TokioGauges::BlockingQueueDepth) = m.blocking_queue_depth() as u64);
        metric!(gauge(TokioGauges::BudgetForcedYieldCount) = m.budget_forced_yield_count());
        metric!(gauge(TokioGauges::NumBlockingThreads) = m.num_blocking_threads() as u64);
        metric!(gauge(TokioGauges::NumIdleBlockingThreads) = m.num_idle_blocking_threads() as u64);
        metric!(gauge(TokioGauges::NumWorkers) = m.num_workers() as u64);
        for worker in 0..m.num_workers() {
            let worker_name = worker.to_string();
            metric!(
                gauge(TokioGauges::WorkerLocalQueueDepth) =
                    m.worker_local_queue_depth(worker) as u64,
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerLocalScheduleCount) =
                    m.worker_local_schedule_count(worker),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerMeanPollTime) =
                    m.worker_mean_poll_time(worker).as_secs_f64(),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerNoopCount) = m.worker_noop_count(worker),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerOverflowCount) = m.worker_overflow_count(worker),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerParkCount) = m.worker_park_count(worker),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerPollCount) = m.worker_poll_count(worker),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerStealCount) = m.worker_steal_count(worker),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerStealOperations) = m.worker_steal_operations(worker),
                worker = &worker_name,
            );
            metric!(
                gauge(TokioGauges::WorkerTotalBusyDuration) =
                    m.worker_total_busy_duration(worker).as_secs_f64(),
                worker = &worker_name,
            );
        }
    }

    async fn upstream_status(&self) {
        if self.config.relay_mode() == RelayMode::Managed {
            if let Ok(is_outage) = self.upstream_relay.send(IsNetworkOutage).await {
                metric!(gauge(RelayGauges::NetworkOutage) = u64::from(is_outage));
            }
        }
    }

    #[cfg(not(feature = "processing"))]
    async fn redis_pool(&self) {
        let _redis_pool = self.redis_pool; // silence unused warning
    }

    #[cfg(feature = "processing")]
    async fn redis_pool(&self) {
        let Some(ref redis_pool) = self.redis_pool else {
            return;
        };

        let state = redis_pool.state();
        metric!(gauge(RelayGauges::RedisPoolConnections) = u64::from(state.connections));
        metric!(gauge(RelayGauges::RedisPoolIdleConnections) = u64::from(state.idle_connections));
    }
}

impl Service for RelayStats {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        let Some(mut ticker) = self.config.metrics_periodic_interval().map(interval) else {
            return;
        };

        tokio::spawn(async move {
            loop {
                let _ = tokio::join!(
                    self.upstream_status(),
                    self.tokio_metrics(),
                    self.redis_pool(),
                );
                ticker.tick().await;
            }
        });
    }
}
