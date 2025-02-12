use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::{MetricOutcomes, MetricStats};
use crate::services::buffer::{
    ObservableEnvelopeBuffer, PartitionedEnvelopeBuffer, ProjectKeyPair,
};
use crate::services::cogs::{CogsService, CogsServiceRecorder};
use crate::services::global_config::{GlobalConfigManager, GlobalConfigService};
use crate::services::health_check::{HealthCheck, HealthCheckService};
use crate::services::internal_metrics::{InternalMetricsMessage, RelayMetricsService};
use crate::services::metrics::RouterService;
use crate::services::outcome::{OutcomeProducer, OutcomeProducerService, TrackOutcome};
use crate::services::outcome_aggregator::OutcomeAggregator;
use crate::services::processor::{self, EnvelopeProcessor, EnvelopeProcessorService};
use crate::services::projects::cache::{ProjectCacheHandle, ProjectCacheService};
use crate::services::projects::source::ProjectSource;
use crate::services::relays::{RelayCache, RelayCacheService};
use crate::services::stats::RelayStats;
#[cfg(feature = "processing")]
use crate::services::store::StoreService;
use crate::services::test_store::{TestStore, TestStoreService};
use crate::services::upstream::{UpstreamRelay, UpstreamRelayService};
use crate::utils::{MemoryChecker, MemoryStat, ThreadKind};
#[cfg(feature = "processing")]
use anyhow::Context;
use anyhow::Result;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use rayon::ThreadPool;
use relay_cogs::Cogs;
use relay_config::Config;
#[cfg(feature = "processing")]
use relay_config::{RedisConfigRef, RedisPoolConfigs};
#[cfg(feature = "processing")]
use relay_redis::redis::Script;
#[cfg(feature = "processing")]
use relay_redis::AsyncRedisClient;
#[cfg(feature = "processing")]
use relay_redis::{PooledClient, RedisError, RedisPool, RedisPools, RedisScripts};
use relay_system::{channel, Addr, Service, ServiceRunner};

/// Indicates the type of failure of the server.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// GeoIp construction failed.
    #[error("could not load the Geoip Db")]
    GeoIp,

    /// Initializing the Kafka producer failed.
    #[cfg(feature = "processing")]
    #[error("could not initialize kafka producer: {0}")]
    Kafka(String),

    /// Initializing the Redis cluster client failed.
    #[cfg(feature = "processing")]
    #[error("could not initialize redis cluster client")]
    Redis,
}

#[derive(Clone, Debug)]
pub struct Registry {
    pub health_check: Addr<HealthCheck>,
    pub outcome_producer: Addr<OutcomeProducer>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub processor: Addr<EnvelopeProcessor>,
    pub test_store: Addr<TestStore>,
    pub relay_cache: Addr<RelayCache>,
    pub global_config: Addr<GlobalConfigManager>,
    pub upstream_relay: Addr<UpstreamRelay>,
    pub envelope_buffer: PartitionedEnvelopeBuffer,

    pub project_cache_handle: ProjectCacheHandle,
    pub internal_metrics: Addr<InternalMetricsMessage>,
}

/// Constructs a Tokio [`relay_system::Runtime`] configured for running [services](relay_system::Service).
pub fn create_runtime(name: &'static str, threads: usize) -> relay_system::Runtime {
    relay_system::Runtime::builder(name)
        .worker_threads(threads)
        // Relay uses `spawn_blocking` only for Redis connections within the project
        // cache, those should never exceed 100 concurrent connections
        // (limited by connection pool).
        //
        // Relay also does not use other blocking operations from Tokio which require
        // this pool, no usage of `tokio::fs` and `tokio::io::{Stdin, Stdout, Stderr}`.
        //
        // We limit the maximum amount of threads here, we've seen that Tokio
        // expands this pool very very aggressively and basically never shrinks it
        // which leads to a massive resource waste.
        .max_blocking_threads(150)
        // We also lower down the default (10s) keep alive timeout for blocking
        // threads to encourage the runtime to not keep too many idle blocking threads
        // around.
        .thread_keep_alive(Duration::from_secs(1))
        .build()
}

fn create_processor_pool(config: &Config) -> Result<ThreadPool> {
    // Adjust thread count for small cpu counts to not have too many idle cores
    // and distribute workload better.
    let thread_count = match config.cpu_concurrency() {
        conc @ 0..=2 => conc.max(1),
        conc @ 3..=4 => conc - 1,
        conc => conc - 2,
    };
    relay_log::info!("starting {thread_count} envelope processing workers");

    let pool = crate::utils::ThreadPoolBuilder::new("processor")
        .num_threads(thread_count)
        .thread_kind(ThreadKind::Worker)
        .runtime(tokio::runtime::Handle::current())
        .build()?;

    Ok(pool)
}

#[cfg(feature = "processing")]
fn create_store_pool(config: &Config) -> Result<ThreadPool> {
    // Spawn a store worker for every 12 threads in the processor pool.
    // This ratio was found empirically and may need adjustments in the future.
    //
    // Ideally in the future the store will be single threaded again, after we move
    // all the heavy processing (de- and re-serialization) into the processor.
    let thread_count = config.cpu_concurrency().div_ceil(12);
    relay_log::info!("starting {thread_count} store workers");

    let pool = crate::utils::ThreadPoolBuilder::new("store")
        .num_threads(thread_count)
        .runtime(tokio::runtime::Handle::current())
        .build()?;

    Ok(pool)
}

#[derive(Debug)]
struct StateInner {
    config: Arc<Config>,
    memory_checker: MemoryChecker,
    registry: Registry,
}

/// Server state.
#[derive(Clone, Debug)]
pub struct ServiceState {
    inner: Arc<StateInner>,
}

impl ServiceState {
    /// Starts all services and returns addresses to all of them.
    pub async fn start(
        rt_metrics: relay_system::RuntimeMetrics,
        config: Arc<Config>,
    ) -> Result<(Self, ServiceRunner)> {
        let mut runner = ServiceRunner::new();
        let upstream_relay = runner.start(UpstreamRelayService::new(config.clone()));
        let test_store = runner.start(TestStoreService::new(config.clone()));

        #[cfg(feature = "processing")]
        let redis_pools = match config.redis().filter(|_| config.processing_enabled()) {
            Some(config) => Some(create_redis_pools(config).await),
            None => None,
        }
        .transpose()
        .context(ServiceError::Redis)?;

        // If we have Redis configured, we want to initialize all the scripts by loading them in
        // the scripts cache if not present. Our custom ConnectionLike implementation relies on this
        // initialization to work properly since it assumes that scripts are loaded across all Redis
        // instances.
        #[cfg(feature = "processing")]
        if let Some(redis_pools) = &redis_pools {
            initialize_redis_scripts_for_pools(redis_pools).context(ServiceError::Redis)?;
        }

        // We create an instance of `MemoryStat` which can be supplied composed with any arbitrary
        // configuration object down the line.
        let memory_stat = MemoryStat::new(config.memory_stat_refresh_frequency_ms());

        // Create an address for the `EnvelopeProcessor`, which can be injected into the
        // other services.
        let (processor, processor_rx) = channel(EnvelopeProcessorService::name());
        let outcome_producer = runner.start(OutcomeProducerService::create(
            config.clone(),
            upstream_relay.clone(),
            processor.clone(),
        )?);
        let outcome_aggregator =
            runner.start(OutcomeAggregator::new(&config, outcome_producer.clone()));

        let internal_metrics = runner.start(RelayMetricsService::new(memory_stat.clone()));

        let (global_config, global_config_rx) =
            GlobalConfigService::new(config.clone(), upstream_relay.clone());
        let global_config_handle = global_config.handle();
        // The global config service must start before dependant services are
        // started. Messages like subscription requests to the global config
        // service fail if the service is not running.
        let global_config = runner.start(global_config);

        let project_source = ProjectSource::start_in(
            &mut runner,
            Arc::clone(&config),
            upstream_relay.clone(),
            #[cfg(feature = "processing")]
            redis_pools.clone(),
        )
        .await;
        let project_cache_handle =
            ProjectCacheService::new(Arc::clone(&config), project_source).start_in(&mut runner);

        let aggregator = RouterService::new(
            config.default_aggregator_config().clone(),
            config.secondary_aggregator_configs().clone(),
            Some(processor.clone().recipient()),
            project_cache_handle.clone(),
        );
        let aggregator_handle = aggregator.handle();
        let aggregator = runner.start(aggregator);

        let metric_stats = MetricStats::new(
            config.clone(),
            global_config_handle.clone(),
            aggregator.clone(),
        );

        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator.clone());

        #[cfg(feature = "processing")]
        let store = config
            .processing_enabled()
            .then(|| {
                StoreService::create(
                    create_store_pool(&config)?,
                    config.clone(),
                    global_config_handle.clone(),
                    outcome_aggregator.clone(),
                    metric_outcomes.clone(),
                )
                .map(|s| runner.start(s))
            })
            .transpose()?;

        let cogs = CogsService::new(&config);
        let cogs = Cogs::new(CogsServiceRecorder::new(&config, runner.start(cogs)));

        runner.start_with(
            EnvelopeProcessorService::new(
                create_processor_pool(&config)?,
                config.clone(),
                global_config_handle,
                project_cache_handle.clone(),
                cogs,
                #[cfg(feature = "processing")]
                redis_pools.clone(),
                processor::Addrs {
                    outcome_aggregator: outcome_aggregator.clone(),
                    upstream_relay: upstream_relay.clone(),
                    test_store: test_store.clone(),
                    #[cfg(feature = "processing")]
                    store_forwarder: store.clone(),
                    aggregator: aggregator.clone(),
                    internal_metrics: internal_metrics.clone(),
                },
                metric_outcomes.clone(),
            ),
            processor_rx,
        );

        let envelope_buffer = PartitionedEnvelopeBuffer::create(
            config.spool_partitions(),
            config.clone(),
            memory_stat.clone(),
            global_config_rx.clone(),
            project_cache_handle.clone(),
            processor.clone(),
            outcome_aggregator.clone(),
            internal_metrics.clone(),
            test_store.clone(),
            &mut runner,
        );

        let health_check = runner.start(HealthCheckService::new(
            config.clone(),
            MemoryChecker::new(memory_stat.clone(), config.clone()),
            aggregator_handle,
            upstream_relay.clone(),
            envelope_buffer.clone(),
        ));

        runner.start(RelayStats::new(
            config.clone(),
            rt_metrics,
            upstream_relay.clone(),
            #[cfg(feature = "processing")]
            redis_pools.clone(),
        ));

        let relay_cache = runner.start(RelayCacheService::new(
            config.clone(),
            upstream_relay.clone(),
        ));

        let registry = Registry {
            processor,
            health_check,
            outcome_producer,
            outcome_aggregator,
            test_store,
            relay_cache,
            global_config,
            project_cache_handle,
            upstream_relay,
            envelope_buffer,
            internal_metrics,
        };

        let state = StateInner {
            config: config.clone(),
            memory_checker: MemoryChecker::new(memory_stat, config.clone()),
            registry,
        };

        Ok((
            ServiceState {
                inner: Arc::new(state),
            },
            runner,
        ))
    }

    /// Returns a reference to the Relay configuration.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Returns a reference to the [`MemoryChecker`] which is a [`Config`] aware wrapper on the
    /// [`MemoryStat`] which gives utility methods to determine whether memory usage is above
    /// thresholds set in the [`Config`].
    pub fn memory_checker(&self) -> &MemoryChecker {
        &self.inner.memory_checker
    }

    pub fn keda_metrics(&self) -> &Addr<InternalMetricsMessage> {
        &self.inner.registry.internal_metrics
    }

    /// Returns the V2 envelope buffer, if present.
    pub fn envelope_buffer(&self, project_key_pair: ProjectKeyPair) -> &ObservableEnvelopeBuffer {
        self.inner.registry.envelope_buffer.buffer(project_key_pair)
    }

    /// Returns a [`ProjectCacheHandle`].
    pub fn project_cache_handle(&self) -> &ProjectCacheHandle {
        &self.inner.registry.project_cache_handle
    }

    /// Returns the address of the [`RelayCache`] service.
    pub fn relay_cache(&self) -> &Addr<RelayCache> {
        &self.inner.registry.relay_cache
    }

    /// Returns the address of the [`HealthCheck`] service.
    pub fn health_check(&self) -> &Addr<HealthCheck> {
        &self.inner.registry.health_check
    }

    /// Returns the address of the [`OutcomeProducer`] service.
    pub fn outcome_producer(&self) -> &Addr<OutcomeProducer> {
        &self.inner.registry.outcome_producer
    }

    /// Returns the address of the [`OutcomeProducer`] service.
    pub fn test_store(&self) -> &Addr<TestStore> {
        &self.inner.registry.test_store
    }

    /// Returns the address of the [`OutcomeProducer`] service.
    pub fn upstream_relay(&self) -> &Addr<UpstreamRelay> {
        &self.inner.registry.upstream_relay
    }

    /// Returns the address of the [`OutcomeProducer`] service.
    pub fn processor(&self) -> &Addr<EnvelopeProcessor> {
        &self.inner.registry.processor
    }

    /// Returns the address of the [`GlobalConfigService`] service.
    pub fn global_config(&self) -> &Addr<GlobalConfigManager> {
        &self.inner.registry.global_config
    }

    /// Returns the address of the [`OutcomeProducer`] service.
    pub fn outcome_aggregator(&self) -> &Addr<TrackOutcome> {
        &self.inner.registry.outcome_aggregator
    }
}

#[cfg(feature = "processing")]
fn create_redis_pool(redis_config: RedisConfigRef) -> Result<RedisPool, RedisError> {
    match redis_config {
        RedisConfigRef::Cluster {
            cluster_nodes,
            options,
        } => RedisPool::cluster(cluster_nodes.iter().map(|s| s.as_str()), options),
        RedisConfigRef::MultiWrite { configs } => {
            let mut configs = configs.into_iter();
            let primary = create_redis_pool(configs.next().ok_or(RedisError::Configuration)?)?;
            let secondaries = configs
                .map(|s| create_redis_pool(s).map_err(|_| RedisError::Configuration))
                .collect::<Result<Vec<_>, _>>()?;

            RedisPool::multi_write(primary, secondaries)
        }
        RedisConfigRef::Single { server, options } => RedisPool::single(server, options),
    }
}

/// Creates Redis pools from the given `configs`.
///
/// If `configs` is [`Unified`](RedisPoolConfigs::Unified), one pool is created and then cloned
/// for cardinality and quotas, meaning that they really use the same pool.
/// `project_config` uses an async pool so it cannot be shared with the other two.
///
/// If it is [`Individual`](RedisPoolConfigs::Individual), an actual separate pool
/// is created for each use case.
#[cfg(feature = "processing")]
pub async fn create_redis_pools(configs: RedisPoolConfigs<'_>) -> Result<RedisPools, RedisError> {
    match configs {
        RedisPoolConfigs::Unified(pool) => {
            let project_configs = create_async_connection(&pool).await?;
            let pool = create_redis_pool(pool)?;
            Ok(RedisPools {
                project_configs,
                cardinality: pool.clone(),
                quotas: pool.clone(),
            })
        }
        RedisPoolConfigs::Individual {
            project_configs,
            cardinality,
            quotas,
        } => {
            let project_configs = create_async_connection(&project_configs).await?;
            let cardinality = create_redis_pool(cardinality)?;
            let quotas = create_redis_pool(quotas)?;

            Ok(RedisPools {
                project_configs,
                cardinality,
                quotas,
            })
        }
    }
}

#[cfg(feature = "processing")]
async fn create_async_connection(
    config: &RedisConfigRef<'_>,
) -> Result<AsyncRedisClient, RedisError> {
    match config {
        RedisConfigRef::Cluster {
            cluster_nodes,
            options,
        } => AsyncRedisClient::cluster(cluster_nodes.iter().map(|s| s.as_str()), options).await,
        RedisConfigRef::Single { server, options } => {
            AsyncRedisClient::single(server, options).await
        }
        RedisConfigRef::MultiWrite { .. } => {
            Err(RedisError::MultiWriteNotSupported("projectconfig"))
        }
    }
}

#[cfg(feature = "processing")]
fn initialize_redis_scripts_for_pools(redis_pools: &RedisPools) -> Result<(), RedisError> {
    let cardinality = redis_pools.cardinality.client()?;
    let quotas = redis_pools.quotas.client()?;

    let scripts = RedisScripts::all();

    let pools = [cardinality, quotas];
    for pool in pools {
        initialize_redis_scripts(pool, &scripts)?;
    }

    Ok(())
}

#[cfg(feature = "processing")]
fn initialize_redis_scripts(
    mut pooled_client: PooledClient,
    scripts: &[&Script; 3],
) -> Result<(), RedisError> {
    let mut connection = pooled_client.connection()?;

    for script in scripts {
        // We load on all instances without checking if the script is already in cache because of a
        // limitation in the connection implementation.
        script
            .prepare_invoke()
            .load(&mut connection)
            .map_err(RedisError::Redis)?;
    }

    Ok(())
}

impl FromRequestParts<Self> for ServiceState {
    type Rejection = Infallible;

    async fn from_request_parts(_: &mut Parts, state: &Self) -> Result<Self, Self::Rejection> {
        Ok(state.clone())
    }
}
