use std::sync::Arc;

use relay_config::{Config, RelayMode};
#[cfg(feature = "processing")]
use relay_redis::{AsyncRedisClient, RedisClientStats, RedisClients};
use relay_statsd::metric;
use relay_system::{Addr, Handle, RuntimeMetrics, Service};
use relay_threading::AsyncPool;
use tokio::time::interval;

use crate::services::processor::EnvelopeProcessorServicePool;
#[cfg(feature = "processing")]
use crate::services::store::StoreServicePool;
use crate::services::upstream::{IsNetworkOutage, UpstreamRelay};
use crate::statsd::{RelayCounters, RelayGauges, RuntimeCounters, RuntimeGauges};

/// Relay Stats Service.
///
/// Service which collects stats periodically and emits them via statsd.
pub struct RelayStats {
    config: Arc<Config>,
    runtime: Handle,
    rt_metrics: RuntimeMetrics,
    upstream_relay: Addr<UpstreamRelay>,
    #[cfg(feature = "processing")]
    redis_pools: Option<RedisClients>,
    processor_pool: EnvelopeProcessorServicePool,
    #[cfg(feature = "processing")]
    store_pool: StoreServicePool,
}

impl RelayStats {
    pub fn new(
        config: Arc<Config>,
        runtime: Handle,
        upstream_relay: Addr<UpstreamRelay>,
        #[cfg(feature = "processing")] redis_pools: Option<RedisClients>,
        processor_pool: EnvelopeProcessorServicePool,
        #[cfg(feature = "processing")] store_pool: StoreServicePool,
    ) -> Self {
        Self {
            config,
            upstream_relay,
            rt_metrics: runtime.metrics(),
            runtime,
            #[cfg(feature = "processing")]
            redis_pools,
            processor_pool,
            #[cfg(feature = "processing")]
            store_pool,
        }
    }

    async fn service_metrics(&self) {
        for (service, metrics) in self.runtime.current_services_metrics().iter() {
            metric!(
                gauge(RelayGauges::ServiceUtilization) = metrics.utilization as u64,
                service = service.name(),
                instance_id = &service.instance_id().to_string(),
            );
        }
    }

    async fn tokio_metrics(&self) {
        metric!(gauge(RuntimeGauges::NumIdleThreads) = self.rt_metrics.num_idle_threads() as u64);
        metric!(gauge(RuntimeGauges::NumAliveTasks) = self.rt_metrics.num_alive_tasks() as u64);
        metric!(
            gauge(RuntimeGauges::BlockingQueueDepth) =
                self.rt_metrics.blocking_queue_depth() as u64
        );
        metric!(
            gauge(RuntimeGauges::NumBlockingThreads) =
                self.rt_metrics.num_blocking_threads() as u64
        );
        metric!(
            gauge(RuntimeGauges::NumIdleBlockingThreads) =
                self.rt_metrics.num_idle_blocking_threads() as u64
        );

        metric!(
            counter(RuntimeCounters::BudgetForcedYieldCount) +=
                self.rt_metrics.budget_forced_yield_count()
        );

        metric!(gauge(RuntimeGauges::NumWorkers) = self.rt_metrics.num_workers() as u64);
        for worker in 0..self.rt_metrics.num_workers() {
            let worker_name = worker.to_string();

            metric!(
                gauge(RuntimeGauges::WorkerLocalQueueDepth) =
                    self.rt_metrics.worker_local_queue_depth(worker) as u64,
                worker = &worker_name,
            );
            metric!(
                gauge(RuntimeGauges::WorkerMeanPollTime) =
                    self.rt_metrics.worker_mean_poll_time(worker).as_secs_f64(),
                worker = &worker_name,
            );

            metric!(
                counter(RuntimeCounters::WorkerLocalScheduleCount) +=
                    self.rt_metrics.worker_local_schedule_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerNoopCount) +=
                    self.rt_metrics.worker_noop_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerOverflowCount) +=
                    self.rt_metrics.worker_overflow_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerParkCount) +=
                    self.rt_metrics.worker_park_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerPollCount) +=
                    self.rt_metrics.worker_poll_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerStealCount) +=
                    self.rt_metrics.worker_steal_count(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerStealOperations) +=
                    self.rt_metrics.worker_steal_operations(worker),
                worker = &worker_name,
            );
            metric!(
                counter(RuntimeCounters::WorkerTotalBusyDuration) +=
                    self.rt_metrics
                        .worker_total_busy_duration(worker)
                        .as_millis() as u64,
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
    fn async_redis_connection(client: &AsyncRedisClient, name: &str) {
        Self::stats_metrics(client.stats(), name);
    }

    #[cfg(feature = "processing")]
    fn stats_metrics(stats: RedisClientStats, name: &str) {
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
        if let Some(RedisClients {
            project_configs,
            cardinality,
            quotas,
        }) = self.redis_pools.as_ref()
        {
            Self::async_redis_connection(project_configs, "project_configs");
            Self::async_redis_connection(cardinality, "cardinality");
            Self::async_redis_connection(quotas, "quotas");
        }
    }

    fn emit_async_pool_metrics<T>(async_pool: &AsyncPool<T>) {
        let metrics = async_pool.metrics();

        metric!(
            gauge(RelayGauges::AsyncPoolQueueSize) = metrics.queue_size(),
            pool = async_pool.name()
        );
        metric!(
            gauge(RelayGauges::AsyncPoolUtilization) = metrics.utilization() as f64,
            pool = async_pool.name()
        );
        metric!(
            counter(RelayCounters::AsyncPoolFinishedTasks) += metrics.finished_tasks(),
            pool = async_pool.name()
        );
    }

    async fn async_pools_metrics(&self) {
        Self::emit_async_pool_metrics(&self.processor_pool);
        #[cfg(feature = "processing")]
        Self::emit_async_pool_metrics(&self.store_pool);
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
                self.service_metrics(),
                self.tokio_metrics(),
                self.redis_pools(),
                self.async_pools_metrics()
            );
            ticker.tick().await;
        }
    }
}
