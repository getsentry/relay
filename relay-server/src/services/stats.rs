use std::sync::Arc;

use crate::services::upstream::{IsNetworkOutage, UpstreamRelay};
use crate::statsd::{RelayGauges, RuntimeCounters, RuntimeGauges};
use relay_config::{Config, RelayMode};
use relay_redis::AsyncRedisClient;
#[cfg(feature = "processing")]
use relay_redis::{RedisPool, RedisPools, Stats};
use relay_statsd::metric;
use relay_system::{Addr, RuntimeMetrics, Service};
use tokio::time::interval;

/// Relay Stats Service.
///
/// Service which collects stats periodically and emits them via statsd.
pub struct RelayStats {
    config: Arc<Config>,
    runtime: RuntimeMetrics,
    upstream_relay: Addr<UpstreamRelay>,
    #[cfg(feature = "processing")]
    redis_pools: Option<RedisPools>,
}

impl RelayStats {
    pub fn new(
        config: Arc<Config>,
        runtime: RuntimeMetrics,
        upstream_relay: Addr<UpstreamRelay>,
        #[cfg(feature = "processing")] redis_pools: Option<RedisPools>,
    ) -> Self {
        Self {
            config,
            upstream_relay,
            runtime,
            #[cfg(feature = "processing")]
            redis_pools,
        }
    }

    async fn tokio_metrics(&self) {
        metric!(gauge(RuntimeGauges::NumIdleThreads) = self.runtime.num_idle_threads() as u64);
        metric!(gauge(RuntimeGauges::NumAliveTasks) = self.runtime.num_alive_tasks() as u64);
        metric!(
            gauge(RuntimeGauges::BlockingQueueDepth) = self.runtime.blocking_queue_depth() as u64
        );
        metric!(
            gauge(RuntimeGauges::NumBlockingThreads) = self.runtime.num_blocking_threads() as u64
        );
        metric!(
            gauge(RuntimeGauges::NumIdleBlockingThreads) =
                self.runtime.num_idle_blocking_threads() as u64
        );

        metric!(
            counter(RuntimeCounters::BudgetForcedYieldCount) +=
                self.runtime.budget_forced_yield_count()
        );

        metric!(gauge(RuntimeGauges::NumWorkers) = self.runtime.num_workers() as u64);
        for worker in 0..self.runtime.num_workers() {
            let worker_name = worker.to_string();

            metric!(
                gauge(RuntimeGauges::WorkerLocalQueueDepth) =
                    self.runtime.worker_local_queue_depth(worker) as u64,
                worker = &worker_name,
            );
            metric!(
                gauge(RuntimeGauges::WorkerMeanPollTime) =
                    self.runtime.worker_mean_poll_time(worker).as_secs_f64(),
                worker = &worker_name,
            );

            metric!(
                counter(RuntimeCounters::WorkerLocalScheduleCount) +=
                    self.runtime.worker_local_schedule_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerNoopCount) += self.runtime.worker_noop_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerOverflowCount) +=
                    self.runtime.worker_overflow_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerParkCount) += self.runtime.worker_park_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerPollCount) += self.runtime.worker_poll_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerStealCount) +=
                    self.runtime.worker_steal_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerStealOperations) +=
                    self.runtime.worker_steal_operations(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerTotalBusyDuration) +=
                    self.runtime.worker_total_busy_duration(worker).as_millis() as u64,
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

    #[cfg(feature = "processing")]
    fn redis_pool(redis_pool: &RedisPool, name: &str) {
        Self::stats_metrics(redis_pool.stats(), name);
    }

    #[cfg(feature = "processing")]
    fn async_redis_connection(client: &AsyncRedisClient, name: &str) {
        Self::stats_metrics(client.stats(), name);
    }

    #[cfg(feature = "processing")]
    fn stats_metrics(stats: Stats, name: &str) {
        metric!(
            gauge(RelayGauges::RedisPoolConnections) = u64::from(stats.connections),
            pool = name
        );
        metric!(
            gauge(RelayGauges::RedisPoolIdleConnections) = u64::from(stats.idle_connections),
            pool = name
        );
    }

    #[cfg(not(feature = "processing"))]
    async fn redis_pools(&self) {}

    #[cfg(feature = "processing")]
    async fn redis_pools(&self) {
        if let Some(RedisPools {
            project_configs,
            cardinality,
            quotas,
        }) = self.redis_pools.as_ref()
        {
            Self::async_redis_connection(project_configs, "project_configs");
            Self::redis_pool(cardinality, "cardinality");
            Self::redis_pool(quotas, "quotas");
        }
    }
}

impl Service for RelayStats {
    type Interface = ();

    async fn run(self, _rx: relay_system::Receiver<Self::Interface>) {
        let Some(mut ticker) = self.config.metrics_periodic_interval().map(interval) else {
            return;
        };

        loop {
            let _ = tokio::join!(
                self.upstream_status(),
                self.tokio_metrics(),
                self.redis_pools(),
            );
            ticker.tick().await;
        }
    }
}
