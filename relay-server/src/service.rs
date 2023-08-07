use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use relay_aws_extension::AwsExtension;
use relay_config::Config;
use relay_metrics::Aggregator;
use relay_redis::RedisPool;
use relay_system::{channel, Addr, Service};
use tokio::runtime::Runtime;

use crate::actors::envelopes::{EnvelopeManager, EnvelopeManagerService};
use crate::actors::health_check::{HealthCheck, HealthCheckService};
use crate::actors::outcome::{OutcomeProducer, OutcomeProducerService, TrackOutcome};
use crate::actors::outcome_aggregator::OutcomeAggregator;
use crate::actors::processor::{EnvelopeProcessor, EnvelopeProcessorService};
use crate::actors::project_cache::{ProjectCache, ProjectCacheService, Services};
use crate::actors::relays::{RelayCache, RelayCacheService};
#[cfg(feature = "processing")]
use crate::actors::store::StoreService;
use crate::actors::test_store::{TestStore, TestStoreService};
use crate::actors::upstream::{UpstreamRelay, UpstreamRelayService};
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
    pub envelope_manager: Addr<EnvelopeManager>,
    pub test_store: Addr<TestStore>,
    pub relay_cache: Addr<RelayCache>,
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
    buffer_guard: Arc<BufferGuard>,
    registry: Registry,
    _aggregator_runtime: Arc<Runtime>,
    _outcome_runtime: Arc<Runtime>,
    _project_runtime: Arc<Runtime>,
    _upstream_runtime: Arc<Runtime>,
    _store_runtime: Option<Arc<Runtime>>,
}

/// Server state.
#[derive(Clone, Debug)]
pub struct ServiceState {
    inner: Arc<StateInner>,
}

impl ServiceState {
    /// Starts all services and returns addresses to all of them.
    pub fn start(config: Arc<Config>) -> Result<Self> {
        let upstream_runtime = create_runtime("upstream-rt", 1);
        let project_runtime = create_runtime("project-rt", 1);
        let aggregator_runtime = create_runtime("aggregator-rt", 1);
        let outcome_runtime = create_runtime("outcome-rt", 1);
        let mut _store_runtime = None;

        let upstream_relay = UpstreamRelayService::new(config.clone()).start_in(&upstream_runtime);
        let test_store = TestStoreService::new(config.clone()).start();

        let redis_pool = match config.redis() {
            Some(redis_config) if config.processing_enabled() => {
                Some(RedisPool::new(redis_config).context(ServiceError::Redis)?)
            }
            _ => None,
        };

        let buffer = Arc::new(BufferGuard::new(config.envelope_buffer_size()));

        // Create an address for the `EnvelopeManagerService`, which can be injected into the
        // other services. This also solves the issue of circular dependencies with `EnvelopeProcessorService`.
        let (envelope_manager, envelope_manager_rx) = channel(EnvelopeManagerService::name());
        let outcome_producer = OutcomeProducerService::create(
            config.clone(),
            upstream_relay.clone(),
            envelope_manager.clone(),
        )?
        .start_in(&outcome_runtime);
        let outcome_aggregator =
            OutcomeAggregator::new(&config, outcome_producer.clone()).start_in(&outcome_runtime);

        let (project_cache, project_cache_rx) = channel(ProjectCacheService::name());
        let processor = EnvelopeProcessorService::new(
            config.clone(),
            redis_pool.clone(),
            envelope_manager.clone(),
            outcome_aggregator.clone(),
            project_cache.clone(),
            upstream_relay.clone(),
        )
        .start();

        let aggregator = relay_metrics::RouterService::new(
            config.aggregator_config().clone(),
            config.secondary_aggregator_configs().clone(),
            Some(project_cache.clone().recipient()),
        )
        .start_in(&aggregator_runtime);

        #[allow(unused_mut)]
        let mut envelope_manager_service = EnvelopeManagerService::new(
            config.clone(),
            aggregator.clone(),
            processor.clone(),
            project_cache.clone(),
            test_store.clone(),
            upstream_relay.clone(),
        );

        #[cfg(feature = "processing")]
        if config.processing_enabled() {
            let rt = create_runtime("store-rt", 1);
            let store = StoreService::create(config.clone())?.start_in(&rt);
            envelope_manager_service.set_store_forwarder(store);
            _store_runtime = Some(rt);
        }

        envelope_manager_service.spawn_handler(envelope_manager_rx);

        // Keep all the services in one context.
        let project_cache_services = Services::new(
            aggregator.clone(),
            processor.clone(),
            envelope_manager.clone(),
            outcome_aggregator.clone(),
            project_cache.clone(),
            test_store.clone(),
            upstream_relay.clone(),
        );
        let guard = project_runtime.enter();
        ProjectCacheService::new(
            config.clone(),
            buffer.clone(),
            project_cache_services,
            redis_pool,
        )
        .spawn_handler(project_cache_rx);
        drop(guard);

        let health_check = HealthCheckService::new(
            config.clone(),
            aggregator.clone(),
            upstream_relay.clone(),
            project_cache.clone(),
        )
        .start();
        let relay_cache = RelayCacheService::new(config.clone(), upstream_relay.clone()).start();

        if let Some(aws_api) = config.aws_runtime_api() {
            if let Ok(aws_extension) = AwsExtension::new(aws_api) {
                aws_extension.start();
            }
        }

        let registry = Registry {
            aggregator,
            processor,
            health_check,
            outcome_producer,
            outcome_aggregator,
            envelope_manager,
            test_store,
            relay_cache,
            project_cache,
            upstream_relay,
        };

        let state = StateInner {
            buffer_guard: buffer,
            config,
            registry,
            _aggregator_runtime: Arc::new(aggregator_runtime),
            _outcome_runtime: Arc::new(outcome_runtime),
            _project_runtime: Arc::new(project_runtime),
            _upstream_runtime: Arc::new(upstream_runtime),
            _store_runtime: _store_runtime.map(Arc::new),
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
