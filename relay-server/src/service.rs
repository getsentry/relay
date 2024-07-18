use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use crate::metrics::{MetricOutcomes, MetricStats};
use crate::services::stats::RelayStats;
use anyhow::{Context, Result};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use relay_cogs::Cogs;
use relay_config::{Config, RedisConnection};
use relay_redis::RedisPool;
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
use crate::utils::{MemoryChecker, MemoryStat};

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
        .enable_all()
        .build()
        .unwrap()
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
    pub fn start(config: Arc<Config>) -> Result<Self> {
        let upstream_relay = UpstreamRelayService::new(config.clone()).start();
        let test_store = TestStoreService::new(config.clone()).start();

        let redis_pool = config
            .redis()
            .filter(|_| config.processing_enabled())
            .map(|redis| match redis {
                (RedisConnection::Single(server), options) => RedisPool::single(server, options),
                (RedisConnection::Cluster(servers), options) => {
                    RedisPool::cluster(servers.iter().map(|s| s.as_str()), options)
                }
            })
            .transpose()
            .context(ServiceError::Redis)?;

        // We create an instance of `MemoryStat` which can be supplied composed with any arbitrary
        // configuration object down the line.
        let memory_stat = MemoryStat::new();

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
            config.clone(),
            global_config_handle,
            cogs,
            #[cfg(feature = "processing")]
            redis_pool.clone(),
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
            memory_stat.init_checker(config.clone()),
            project_cache_services,
            metric_outcomes,
            redis_pool.clone(),
        )
        .spawn_handler(project_cache_rx);

        let health_check = HealthCheckService::new(
            config.clone(),
            memory_stat.init_checker(config.clone()),
            aggregator.clone(),
            upstream_relay.clone(),
            project_cache.clone(),
        )
        .start();

        RelayStats::new(
            config.clone(),
            upstream_relay.clone(),
            #[cfg(feature = "processing")]
            redis_pool,
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
            config: config.clone(),
            memory_checker: memory_stat.init_checker(config),
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

#[axum::async_trait]
impl FromRequestParts<Self> for ServiceState {
    type Rejection = Infallible;

    async fn from_request_parts(_: &mut Parts, state: &Self) -> Result<Self, Self::Rejection> {
        Ok(state.clone())
    }
}
