use std::fmt;
use std::sync::Arc;

use anyhow::{Context, Result};
use once_cell::race::OnceBox;
use relay_aws_extension::AwsExtension;
use relay_config::Config;
use relay_metrics::{Aggregator, AggregatorService};
use relay_redis::RedisPool;
use relay_system::{Addr, Service};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::actors::envelopes::{EnvelopeManager, EnvelopeManagerService};
use crate::actors::health_check::{HealthCheck, HealthCheckService};
use crate::actors::outcome::{OutcomeProducer, OutcomeProducerService, TrackOutcome};
use crate::actors::outcome_aggregator::OutcomeAggregator;
use crate::actors::processor::{EnvelopeProcessor, EnvelopeProcessorService};
use crate::actors::project_cache::{ProjectCache, ProjectCacheService};
use crate::actors::relays::{RelayCache, RelayCacheService};
#[cfg(feature = "processing")]
use crate::actors::store::StoreService;
use crate::actors::test_store::{TestStore, TestStoreService};
use crate::actors::upstream::{UpstreamRelay, UpstreamRelayService};
use crate::utils::BufferGuard;

pub static REGISTRY: OnceBox<Registry> = OnceBox::new();

/// Indicates the type of failure of the server.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ServiceError {
    /// GeoIp construction failed.
    #[cfg(feature = "processing")]
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

impl Registry {
    /// Get the [`AggregatorService`] address from the registry.
    ///
    /// TODO(actix): this is temporary solution while migrating `ProjectCache` actor to the new tokio
    /// runtime and follow up refactoring of the dependencies.
    pub fn aggregator() -> Addr<Aggregator> {
        REGISTRY.get().unwrap().aggregator.clone()
    }
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

/// Server state.
#[derive(Clone)]
pub struct ServiceState {
    config: Arc<Config>,
    buffer_guard: Arc<BufferGuard>,
    _aggregator_runtime: Arc<Runtime>,
    _outcome_runtime: Arc<Runtime>,
    _project_runtime: Arc<Runtime>,
    _upstream_runtime: Arc<Runtime>,
    _store_runtime: Option<Arc<Runtime>>,
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

        let redis_pool = match config.redis() {
            Some(redis_config) if config.processing_enabled() => {
                Some(RedisPool::new(redis_config).context(ServiceError::Redis)?)
            }
            _ => None,
        };

        let buffer = Arc::new(BufferGuard::new(config.envelope_buffer_size()));
        let processor = EnvelopeProcessorService::new(config.clone(), redis_pool.clone())?.start();
        #[allow(unused_mut)]
        let mut envelope_manager = EnvelopeManagerService::new(config.clone());

        #[cfg(feature = "processing")]
        if config.processing_enabled() {
            let rt = create_runtime("store-rt", 1);
            let store = StoreService::create(config.clone())?.start_in(&rt);
            envelope_manager.set_store_forwarder(store);
            _store_runtime = Some(rt);
        }

        let envelope_manager = envelope_manager.start();
        let test_store = TestStoreService::new(config.clone()).start();

        let (aggregator_sender, aggregator_receiver) = mpsc::unbounded_channel();
        let project_cache = ProjectCacheService::new(
            config.clone(),
            processor.clone(),
            envelope_manager.clone(),
            upstream_relay.clone(),
            redis_pool,
            aggregator_sender,
        )
        .start_in(&project_runtime);

        let health_check = HealthCheckService::new(config.clone()).start();
        let relay_cache = RelayCacheService::new(config.clone()).start();

        if let Some(aws_api) = config.aws_runtime_api() {
            if let Ok(aws_extension) = AwsExtension::new(aws_api) {
                aws_extension.start();
            }
        }

        let aggregator = AggregatorService::new(
            config.aggregator_config().clone(),
            Some(project_cache.clone().recipient()),
            aggregator_receiver,
        )
        .start_in(&aggregator_runtime);

        let outcome_producer = OutcomeProducerService::create(
            config.clone(),
            upstream_relay.clone(),
            envelope_manager.clone(),
        )?
        .start_in(&outcome_runtime);
        let outcome_aggregator =
            OutcomeAggregator::new(&config, outcome_producer.clone()).start_in(&outcome_runtime);

        REGISTRY
            .set(Box::new(Registry {
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
            }))
            .unwrap();

        Ok(ServiceState {
            buffer_guard: buffer,
            config,
            _aggregator_runtime: Arc::new(aggregator_runtime),
            _outcome_runtime: Arc::new(outcome_runtime),
            _project_runtime: Arc::new(project_runtime),
            _upstream_runtime: Arc::new(upstream_runtime),
            _store_runtime: _store_runtime.map(Arc::new),
        })
    }

    /// Returns a reference to the Relay configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a reference to the guard of the envelope buffer.
    ///
    /// This can be used to enter new envelopes into the processing queue and reserve a slot in the
    /// buffer. See [`BufferGuard`] for more information.
    pub fn buffer_guard(&self) -> &BufferGuard {
        &self.buffer_guard
    }
}
