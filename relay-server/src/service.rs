use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::MetricOutcomes;
use crate::services::autoscaling::{AutoscalingMetricService, AutoscalingMetrics};
use crate::services::buffer::{
    ObservableEnvelopeBuffer, PartitionedEnvelopeBuffer, ProjectKeyPair,
};
use crate::services::cogs::{CogsService, CogsServiceRecorder};
use crate::services::global_config::{GlobalConfigManager, GlobalConfigService};
#[cfg(feature = "processing")]
use crate::services::global_rate_limits::GlobalRateLimitsService;
use crate::services::health_check::{HealthCheck, HealthCheckService};
use crate::services::metrics::RouterService;
use crate::services::outcome::{OutcomeProducer, OutcomeProducerService, TrackOutcome};
use crate::services::outcome_aggregator::OutcomeAggregator;
use crate::services::processor::{
    self, EnvelopeProcessor, EnvelopeProcessorService, EnvelopeProcessorServicePool,
};
use crate::services::projects::cache::{ProjectCacheHandle, ProjectCacheService};
use crate::services::projects::source::ProjectSource;
use crate::services::proxy_processor::{ProxyAddrs, ProxyProcessorService};
use crate::services::relays::{RelayCache, RelayCacheService};
use crate::services::stats::RelayStats;
#[cfg(feature = "processing")]
use crate::services::store::{StoreService, StoreServicePool};
#[cfg(feature = "processing")]
use crate::services::upload::UploadService;
use crate::services::upstream::{UpstreamRelay, UpstreamRelayService};
use crate::utils::{MemoryChecker, MemoryStat, ThreadKind};
#[cfg(feature = "processing")]
use anyhow::Context;
use anyhow::Result;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use relay_cogs::Cogs;
use relay_config::Config;
#[cfg(feature = "processing")]
use relay_config::{RedisConfigRef, RedisConfigsRef};
#[cfg(feature = "processing")]
use relay_redis::AsyncRedisClient;
#[cfg(feature = "processing")]
use relay_redis::redis::Script;
#[cfg(feature = "processing")]
use relay_redis::{RedisClients, RedisError, RedisScripts};
use relay_system::{Addr, Service, ServiceSpawn, ServiceSpawnExt as _, channel};

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

    /// Initializing the Redis client failed.
    #[cfg(feature = "processing")]
    #[error("could not initialize redis client during startup")]
    Redis,
}

#[derive(Clone, Debug)]
pub struct Registry {
    pub health_check: Addr<HealthCheck>,
    pub outcome_producer: Addr<OutcomeProducer>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub processor: Addr<EnvelopeProcessor>,
    pub relay_cache: Addr<RelayCache>,
    pub global_config: Addr<GlobalConfigManager>,
    pub upstream_relay: Addr<UpstreamRelay>,
    pub envelope_buffer: PartitionedEnvelopeBuffer,
    pub project_cache_handle: ProjectCacheHandle,
    pub autoscaling: Option<Addr<AutoscalingMetrics>>,
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

fn create_processor_pool(config: &Config) -> Result<EnvelopeProcessorServicePool> {
    // Adjust thread count for small cpu counts to not have too many idle cores
    // and distribute workload better.
    let thread_count = match config.cpu_concurrency() {
        conc @ 0..=2 => conc.max(1),
        conc @ 3..=4 => conc - 1,
        conc => conc - 2,
    };
    relay_log::info!("starting {thread_count} envelope processing workers");

    let pool = crate::utils::ThreadPoolBuilder::new("processor", tokio::runtime::Handle::current())
        .num_threads(thread_count)
        .max_concurrency(config.pool_concurrency())
        .thread_kind(ThreadKind::Worker)
        .build()?;

    Ok(pool)
}

#[cfg(feature = "processing")]
fn create_store_pool(config: &Config) -> Result<StoreServicePool> {
    // Spawn a store worker for every 12 threads in the processor pool.
    // This ratio was found empirically and may need adjustments in the future.
    //
    // Ideally in the future the store will be single threaded again, after we move
    // all the heavy processing (de- and re-serialization) into the processor.
    let thread_count = config.cpu_concurrency().div_ceil(12);
    relay_log::info!("starting {thread_count} store workers");

    let pool = crate::utils::ThreadPoolBuilder::new("store", tokio::runtime::Handle::current())
        .num_threads(thread_count)
        .max_concurrency(config.pool_concurrency())
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
        handle: &relay_system::Handle,
        services: &dyn ServiceSpawn,
        config: Arc<Config>,
    ) -> Result<Self> {
        let upstream_relay = services.start(UpstreamRelayService::new(config.clone()));

        #[cfg(feature = "processing")]
        let redis_clients = config
            .redis()
            .filter(|_| config.processing_enabled())
            .map(create_redis_clients)
            .transpose()
            .context(ServiceError::Redis)?;

        // If we have Redis configured, we want to initialize all the scripts by loading them in
        // the scripts cache if not present. Our custom ConnectionLike implementation relies on this
        // initialization to work properly since it assumes that scripts are loaded across all Redis
        // instances.
        #[cfg(feature = "processing")]
        if let Some(redis_clients) = &redis_clients {
            initialize_redis_scripts_for_client(redis_clients)
                .await
                .context(ServiceError::Redis)?;
        }

        // We create an instance of `MemoryStat` which can be supplied composed with any arbitrary
        // configuration object down the line.
        let memory_stat = MemoryStat::new(config.memory_stat_refresh_frequency_ms());

        // Create an address for the `EnvelopeProcessor`, which can be injected into the
        // other services.
        let (processor, processor_rx) = match config.relay_mode() {
            relay_config::RelayMode::Proxy => channel(ProxyProcessorService::name()),
            relay_config::RelayMode::Managed => channel(EnvelopeProcessorService::name()),
        };

        let outcome_producer = services.start(OutcomeProducerService::create(
            config.clone(),
            upstream_relay.clone(),
            processor.clone(),
        )?);
        let outcome_aggregator =
            services.start(OutcomeAggregator::new(&config, outcome_producer.clone()));

        let (global_config, global_config_rx) =
            GlobalConfigService::new(config.clone(), upstream_relay.clone());
        let global_config_handle = global_config.handle();
        // The global config service must start before dependant services are
        // started. Messages like subscription requests to the global config
        // service fail if the service is not running.
        let global_config = services.start(global_config);

        let project_source = ProjectSource::start_in(
            services,
            Arc::clone(&config),
            upstream_relay.clone(),
            #[cfg(feature = "processing")]
            redis_clients.clone(),
        )
        .await;
        let project_cache_handle =
            ProjectCacheService::new(Arc::clone(&config), project_source).start_in(services);

        let metric_outcomes = MetricOutcomes::new(outcome_aggregator.clone());

        #[cfg(feature = "processing")]
        let store_pool = create_store_pool(&config)?;
        #[cfg(feature = "processing")]
        let store = config
            .processing_enabled()
            .then(|| {
                let upload = services.start(UploadService::new(config.upload()));
                StoreService::create(
                    store_pool.clone(),
                    config.clone(),
                    global_config_handle.clone(),
                    outcome_aggregator.clone(),
                    metric_outcomes.clone(),
                    upload,
                )
                .map(|s| services.start(s))
            })
            .transpose()?;

        #[cfg(feature = "processing")]
        let global_rate_limits = redis_clients
            .as_ref()
            .map(|p| services.start(GlobalRateLimitsService::new(p.quotas.clone())));

        let envelope_buffer = PartitionedEnvelopeBuffer::create(
            config.spool_partitions(),
            config.clone(),
            memory_stat.clone(),
            global_config_rx.clone(),
            project_cache_handle.clone(),
            processor.clone(),
            outcome_aggregator.clone(),
            services,
        );

        let (processor_pool, aggregator_handle, autoscaling) = match config.relay_mode() {
            relay_config::RelayMode::Proxy => {
                services.start_with(
                    ProxyProcessorService::new(
                        config.clone(),
                        project_cache_handle.clone(),
                        ProxyAddrs {
                            outcome_aggregator: outcome_aggregator.clone(),
                            upstream_relay: upstream_relay.clone(),
                        },
                    ),
                    processor_rx,
                );
                (None, None, None)
            }
            relay_config::RelayMode::Managed => {
                let processor_pool = create_processor_pool(&config)?;

                let aggregator = RouterService::new(
                    handle.clone(),
                    config.default_aggregator_config().clone(),
                    config.secondary_aggregator_configs().clone(),
                    Some(processor.clone().recipient()),
                    project_cache_handle.clone(),
                );
                let aggregator_handle = aggregator.handle();
                let aggregator = services.start(aggregator);

                let cogs = CogsService::new(&config);
                let cogs = Cogs::new(CogsServiceRecorder::new(&config, services.start(cogs)));

                services.start_with(
                    EnvelopeProcessorService::new(
                        processor_pool.clone(),
                        config.clone(),
                        global_config_handle,
                        project_cache_handle.clone(),
                        cogs,
                        #[cfg(feature = "processing")]
                        redis_clients.clone(),
                        processor::Addrs {
                            outcome_aggregator: outcome_aggregator.clone(),
                            upstream_relay: upstream_relay.clone(),
                            #[cfg(feature = "processing")]
                            store_forwarder: store.clone(),
                            aggregator: aggregator.clone(),
                            #[cfg(feature = "processing")]
                            global_rate_limits,
                        },
                        metric_outcomes.clone(),
                    ),
                    processor_rx,
                );

                let autoscaling = services.start(AutoscalingMetricService::new(
                    memory_stat.clone(),
                    envelope_buffer.clone(),
                    handle.clone(),
                    processor_pool.clone(),
                ));

                (
                    Some(processor_pool),
                    Some(aggregator_handle),
                    Some(autoscaling),
                )
            }
        };

        let health_check = services.start(HealthCheckService::new(
            config.clone(),
            MemoryChecker::new(memory_stat.clone(), config.clone()),
            aggregator_handle,
            upstream_relay.clone(),
            envelope_buffer.clone(),
        ));

        services.start(RelayStats::new(
            config.clone(),
            handle.clone(),
            upstream_relay.clone(),
            #[cfg(feature = "processing")]
            redis_clients.clone(),
            processor_pool,
            #[cfg(feature = "processing")]
            store_pool,
        ));

        let relay_cache = services.start(RelayCacheService::new(
            config.clone(),
            upstream_relay.clone(),
        ));

        let registry = Registry {
            processor,
            health_check,
            outcome_producer,
            outcome_aggregator,
            relay_cache,
            global_config,
            project_cache_handle,
            upstream_relay,
            envelope_buffer,
            autoscaling,
        };

        let state = StateInner {
            config: config.clone(),
            memory_checker: MemoryChecker::new(memory_stat, config.clone()),
            registry,
        };

        Ok(ServiceState {
            inner: Arc::new(state),
        })
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

    pub fn autoscaling(&self) -> Option<&Addr<AutoscalingMetrics>> {
        self.inner.registry.autoscaling.as_ref()
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

/// Creates Redis clients from the given `configs`.
///
/// If `configs` is [`Unified`](RedisConfigsRef::Unified), one client is created and then cloned
/// for project configs, cardinality, and quotas, meaning that they really use the same client.
///
/// If it is [`Individual`](RedisConfigsRef::Individual), an actual separate client
/// is created for each use case.
#[cfg(feature = "processing")]
pub fn create_redis_clients(configs: RedisConfigsRef<'_>) -> Result<RedisClients, RedisError> {
    const CARDINALITY_REDIS_CLIENT: &str = "cardinality";
    const PROJECT_CONFIG_REDIS_CLIENT: &str = "projectconfig";
    const QUOTA_REDIS_CLIENT: &str = "quotas";
    const UNIFIED_REDIS_CLIENT: &str = "unified";

    match configs {
        RedisConfigsRef::Unified(unified) => {
            let client = create_async_redis_client(UNIFIED_REDIS_CLIENT, &unified)?;

            Ok(RedisClients {
                project_configs: client.clone(),
                cardinality: client.clone(),
                quotas: client,
            })
        }
        RedisConfigsRef::Individual {
            project_configs,
            cardinality,
            quotas,
        } => {
            let project_configs =
                create_async_redis_client(PROJECT_CONFIG_REDIS_CLIENT, &project_configs)?;
            let cardinality = create_async_redis_client(CARDINALITY_REDIS_CLIENT, &cardinality)?;
            let quotas = create_async_redis_client(QUOTA_REDIS_CLIENT, &quotas)?;

            Ok(RedisClients {
                project_configs,
                cardinality,
                quotas,
            })
        }
    }
}

#[cfg(feature = "processing")]
fn create_async_redis_client(
    name: &'static str,
    config: &RedisConfigRef<'_>,
) -> Result<AsyncRedisClient, RedisError> {
    match config {
        RedisConfigRef::Cluster {
            cluster_nodes,
            options,
        } => AsyncRedisClient::cluster(name, cluster_nodes.iter().map(|s| s.as_str()), options),
        RedisConfigRef::Single { server, options } => {
            AsyncRedisClient::single(name, server, options)
        }
    }
}

#[cfg(feature = "processing")]
async fn initialize_redis_scripts_for_client(
    redis_clients: &RedisClients,
) -> Result<(), RedisError> {
    let scripts = RedisScripts::all();

    let clients = [&redis_clients.cardinality, &redis_clients.quotas];
    for client in clients {
        initialize_redis_scripts(client, &scripts).await?;
    }

    Ok(())
}

#[cfg(feature = "processing")]
async fn initialize_redis_scripts(
    client: &AsyncRedisClient,
    scripts: &[&Script; 3],
) -> Result<(), RedisError> {
    let mut connection = client.get_connection().await?;

    for script in scripts {
        // We load on all instances without checking if the script is already in cache because of a
        // limitation in the connection implementation.
        script
            .prepare_invoke()
            .load_async(&mut connection)
            .await
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
