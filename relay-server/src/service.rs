use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::{MetricOutcomes, MetricStats};
use crate::services::stats::RelayStats;
use anyhow::{Context, Result};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use rayon::ThreadPool;
use relay_cogs::Cogs;
use relay_config::{Config, RedisConnection, RedisPoolConfigs};
use relay_redis::{RedisConfigOptions, RedisError, RedisPool, RedisPools};
use relay_system::{channel, Addr, Service};
use tokio::runtime::Runtime;

use crate::services::cogs::{CogsService, CogsServiceRecorder};
use crate::services::global_config::{GlobalConfigManager, GlobalConfigService};
use crate::services::health_check::{HealthCheck, HealthCheckService};
use crate::services::metrics::{Aggregator, RouterService};
use crate::services::outcome::{OutcomeProducer, OutcomeProducerService, TrackOutcome};
use crate::services::outcome_aggregator::OutcomeAggregator;
use crate::services::processor::{self, EnvelopeProcessor, EnvelopeProcessorService};
use crate::services::project_cache::{ProjectCache, ProjectCacheService, Services};
use crate::services::relays::{RelayCache, RelayCacheService};
#[cfg(feature = "processing")]
use crate::services::store::StoreService;
use crate::services::test_store::{TestStore, TestStoreService};
use crate::services::upstream::{UpstreamRelay, UpstreamRelayService};
use crate::utils::BufferGuard;

/// Indicates the type of failure of the server.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ServiceError {
    /// GeoIp construction failed.
    #[error("could not load the Geoip Db")]
    GeoIp,

    /// Initializing the Kafka producer failed.
    #[cfg(feature = "processing")]
    #[error("could not initialize kafka producer")]
    Kafka,

    /// Initializing the Redis cluster client failed.
    #[error("could not initialize redis cluster client")]
    Redis,
}

#[derive(Clone)]
pub struct Registry {
    pub aggregator: Addr<Aggregator>,
    pub health_check: Addr<HealthCheck>,
    pub outcome_producer: Addr<OutcomeProducer>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub processor: Addr<EnvelopeProcessor>,
    pub test_store: Addr<TestStore>,
    pub relay_cache: Addr<RelayCache>,
    pub global_config: Addr<GlobalConfigManager>,
    pub project_cache: Addr<ProjectCache>,
    pub upstream_relay: Addr<UpstreamRelay>,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("aggregator", &self.aggregator)
            .field("health_check", &self.health_check)
            .field("outcome_producer", &self.outcome_producer)
            .field("outcome_aggregator", &self.outcome_aggregator)
            .field("processor", &format_args!("Addr<Processor>"))
            .finish()
    }
}

/// Constructs a tokio [`Runtime`] configured for running [services](relay_system::Service).
pub fn create_runtime(name: &str, threads: usize) -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(name)
        .worker_threads(threads)
        // Relay uses `spawn_blocking` only for Redis connections within the project
        // cache, those should never exceed 100 concurrent connections
        // (limited by connection pool).
        //
        // Relay also does not use other blocking opertions from Tokio which require
        // this pool, no usage of `tokio::fs` and `tokio::io::{Stdin, Stdout, Stderr}`.
        //
        // We limit the maximum amount of threads here, we've seen that Tokio
        // expands this pool very very aggressively and basically never shrinks it
        // which leads to a massive resource waste.
        .max_blocking_threads(150)
        // As with the maximum amount of threads used by the runtime, we want
        // to encourage the runtime to terminate blocking threads again.
        .thread_keep_alive(Duration::from_secs(1))
        .enable_all()
        .build()
        .unwrap()
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
        .runtime(tokio::runtime::Handle::current())
        .build()?;

    Ok(pool)
}

#[derive(Debug)]
struct StateInner {
    config: Arc<Config>,
    buffer_guard: Arc<BufferGuard>,
    registry: Registry,
}

/// Server state.
#[derive(Clone, Debug)]
pub struct ServiceState {
    inner: Arc<StateInner>,
}

impl ServiceState {
    /// Starts all services and returns addresses to all of them.
    pub fn start(config: Arc<Config>) -> Result<Self> {
        let upstream_relay = UpstreamRelayService::new(config.clone()).start();
        let test_store = TestStoreService::new(config.clone()).start();

        let redis_pools = config
            .redis()
            .filter(|_| config.processing_enabled())
            .map(create_redis_pools)
            .transpose()
            .context(ServiceError::Redis)?;

        let buffer_guard = Arc::new(BufferGuard::new(config.envelope_buffer_size()));

        // Create an address for the `EnvelopeProcessor`, which can be injected into the
        // other services.
        let (processor, processor_rx) = channel(EnvelopeProcessorService::name());
        let outcome_producer = OutcomeProducerService::create(
            config.clone(),
            upstream_relay.clone(),
            processor.clone(),
        )?
        .start();
        let outcome_aggregator = OutcomeAggregator::new(&config, outcome_producer.clone()).start();

        let global_config = GlobalConfigService::new(config.clone(), upstream_relay.clone());
        let global_config_handle = global_config.handle();
        // The global config service must start before dependant services are
        // started. Messages like subscription requests to the global config
        // service fail if the service is not running.
        let global_config = global_config.start();

        let (project_cache, project_cache_rx) = channel(ProjectCacheService::name());

        let aggregator = RouterService::new(
            config.default_aggregator_config().clone(),
            config.secondary_aggregator_configs().clone(),
            Some(project_cache.clone().recipient()),
        )
        .start();

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
                    config.clone(),
                    global_config_handle.clone(),
                    outcome_aggregator.clone(),
                    metric_outcomes.clone(),
                )
                .map(|s| s.start())
            })
            .transpose()?;

        let cogs = CogsService::new(
            &config,
            #[cfg(feature = "processing")]
            store.clone(),
        );
        let cogs = Cogs::new(CogsServiceRecorder::new(&config, cogs.start()));

        EnvelopeProcessorService::new(
            create_processor_pool(&config)?,
            config.clone(),
            global_config_handle,
            cogs,
            #[cfg(feature = "processing")]
            redis_pools.clone(),
            processor::Addrs {
                project_cache: project_cache.clone(),
                outcome_aggregator: outcome_aggregator.clone(),
                upstream_relay: upstream_relay.clone(),
                test_store: test_store.clone(),
                #[cfg(feature = "processing")]
                store_forwarder: store.clone(),
            },
            metric_outcomes.clone(),
        )
        .spawn_handler(processor_rx);

        // Keep all the services in one context.
        let project_cache_services = Services::new(
            aggregator.clone(),
            processor.clone(),
            outcome_aggregator.clone(),
            project_cache.clone(),
            test_store.clone(),
            upstream_relay.clone(),
            global_config.clone(),
        );
        ProjectCacheService::new(
            config.clone(),
            buffer_guard.clone(),
            project_cache_services,
            metric_outcomes,
            redis_pools
                .as_ref()
                .map(|pools| pools.project_configs.clone()),
        )
        .spawn_handler(project_cache_rx);

        let health_check = HealthCheckService::new(
            config.clone(),
            aggregator.clone(),
            upstream_relay.clone(),
            project_cache.clone(),
        )
        .start();

        RelayStats::new(
            config.clone(),
            upstream_relay.clone(),
            #[cfg(feature = "processing")]
            redis_pools.clone(),
        )
        .start();

        let relay_cache = RelayCacheService::new(config.clone(), upstream_relay.clone()).start();

        let registry = Registry {
            aggregator,
            processor,
            health_check,
            outcome_producer,
            outcome_aggregator,
            test_store,
            relay_cache,
            global_config,
            project_cache,
            upstream_relay,
        };

        let state = StateInner {
            buffer_guard,
            config,
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

    /// Returns a reference to the guard of the envelope buffer.
    ///
    /// This can be used to enter new envelopes into the processing queue and reserve a slot in the
    /// buffer. See [`BufferGuard`] for more information.
    pub fn buffer_guard(&self) -> &BufferGuard {
        &self.inner.buffer_guard
    }

    /// Returns the address of the [`ProjectCache`] service.
    pub fn project_cache(&self) -> &Addr<ProjectCache> {
        &self.inner.registry.project_cache
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

fn create_redis_pool(
    connection: &RedisConnection,
    options: RedisConfigOptions,
) -> Result<RedisPool, RedisError> {
    match connection {
        RedisConnection::Cluster(servers) => {
            RedisPool::cluster(servers.iter().map(|s| s.as_str()), options)
        }
        RedisConnection::Single(server) => RedisPool::single(server, options),
    }
}

pub fn create_redis_pools(configs: RedisPoolConfigs) -> Result<RedisPools, RedisError> {
    let project_configs = create_redis_pool(configs.project_configs.0, configs.project_configs.1)?;
    let cardinality = create_redis_pool(configs.cardinality.0, configs.cardinality.1)?;
    let quotas = create_redis_pool(configs.quotas.0, configs.quotas.1)?;
    let misc = create_redis_pool(configs.misc.0, configs.misc.1)?;

    Ok(RedisPools {
        project_configs,
        cardinality,
        quotas,
        misc,
    })
}

#[axum::async_trait]
impl FromRequestParts<Self> for ServiceState {
    type Rejection = Infallible;

    async fn from_request_parts(_: &mut Parts, state: &Self) -> Result<Self, Self::Rejection> {
        Ok(state.clone())
    }
}
