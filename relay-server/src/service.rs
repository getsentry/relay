use std::fmt;
use std::sync::Arc;

use actix::Recipient;
use actix_web::server::StopServer;
use actix_web::{server, App};
use anyhow::{Context, Result};
use futures01::Future;
use listenfd::ListenFd;
use once_cell::race::OnceBox;
use tokio::runtime::Runtime;

use relay_aws_extension::AwsExtension;
use relay_config::Config;
use relay_metrics::{Aggregator, AggregatorService};
use relay_redis::RedisPool;
use relay_system::{Addr, Configure, Controller, Service};

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
use crate::middlewares::{
    AddCommonHeaders, ErrorHandlers, Metrics, ReadRequestMiddleware, SentryMiddleware,
};
use crate::utils::BufferGuard;

pub static REGISTRY: OnceBox<Registry> = OnceBox::new();

/// Indicates the type of failure of the server.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ServerError {
    /// Binding failed.
    #[error("bind to interface failed")]
    BindFailed,

    /// Listening on the HTTP socket failed.
    #[error("listening failed")]
    ListenFailed,

    /// A TLS error ocurred.
    #[error("could not initialize the TLS server")]
    TlsInitFailed,

    /// TLS support was not compiled in.
    #[cfg(not(feature = "ssl"))]
    #[error("compile with the `ssl` feature to enable SSL support")]
    TlsNotSupported,

    /// GeoIp construction failed.
    #[cfg(feature = "processing")]
    #[error("could not load the Geoip Db")]
    GeoIpError,

    /// Initializing the Kafka producer failed.
    #[cfg(feature = "processing")]
    #[error("could not initialize kafka producer")]
    KafkaError,

    /// Initializing the Redis cluster client failed.
    #[error("could not initialize redis cluster client")]
    RedisError,
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

        let guard = upstream_runtime.enter();
        let upstream_relay = UpstreamRelayService::new(config.clone()).start();
        drop(guard);

        let guard = outcome_runtime.enter();
        let outcome_producer = OutcomeProducerService::create(config.clone())?.start();
        let outcome_aggregator = OutcomeAggregator::new(&config, outcome_producer.clone()).start();
        drop(guard);

        let redis_pool = match config.redis() {
            Some(redis_config) if config.processing_enabled() => {
                Some(RedisPool::new(redis_config).context(ServerError::RedisError)?)
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
            let _guard = rt.enter();
            let store = StoreService::create(config.clone())?.start();
            envelope_manager.set_store_forwarder(store);
            _store_runtime = Some(rt);
        }

        let envelope_manager = envelope_manager.start();
        let test_store = TestStoreService::new(config.clone()).start();

        let guard = project_runtime.enter();
        let project_cache = ProjectCacheService::new(config.clone(), redis_pool).start();
        drop(guard);

        let health_check = HealthCheckService::new(config.clone()).start();
        let relay_cache = RelayCacheService::new(config.clone()).start();

        if let Some(aws_api) = config.aws_runtime_api() {
            if let Ok(aws_extension) = AwsExtension::new(aws_api) {
                aws_extension.start();
            }
        }

        let guard = aggregator_runtime.enter();
        let aggregator = AggregatorService::new(
            config.aggregator_config().clone(),
            Some(project_cache.clone().recipient()),
        )
        .start();
        drop(guard);

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

    /// Returns an atomically counted reference to the config.
    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
    }

    /// Returns a reference to the guard of the envelope buffer.
    ///
    /// This can be used to enter new envelopes into the processing queue and reserve a slot in the
    /// buffer. See [`BufferGuard`] for more information.
    pub fn buffer_guard(&self) -> Arc<BufferGuard> {
        self.buffer_guard.clone()
    }
}

/// The actix app type for the relay web service.
pub type ServiceApp = App<ServiceState>;

fn make_app(state: ServiceState) -> ServiceApp {
    App::with_state(state)
        .middleware(SentryMiddleware::new())
        .middleware(Metrics)
        .middleware(AddCommonHeaders)
        .middleware(ErrorHandlers)
        .middleware(ReadRequestMiddleware)
        .configure(crate::endpoints::configure_app)
}

fn dump_listen_infos<H, F>(server: &server::HttpServer<H, F>)
where
    H: server::IntoHttpHandler + 'static,
    F: Fn() -> H + Send + Clone + 'static,
{
    relay_log::info!("spawning http server");
    for (addr, scheme) in server.addrs_with_scheme() {
        relay_log::info!("  listening on: {}://{}/", scheme, addr);
    }
}

fn listen<H, F>(
    server: server::HttpServer<H, F>,
    config: &Config,
) -> Result<server::HttpServer<H, F>>
where
    H: server::IntoHttpHandler + 'static,
    F: Fn() -> H + Send + Clone + 'static,
{
    Ok(
        match ListenFd::from_env()
            .take_tcp_listener(0)
            .context(ServerError::ListenFailed)?
        {
            Some(listener) => server.listen(listener),
            None => server
                .bind(config.listen_addr())
                .context(ServerError::BindFailed)?,
        },
    )
}

#[cfg(feature = "ssl")]
fn listen_ssl<H, F>(
    mut server: server::HttpServer<H, F>,
    config: &Config,
) -> Result<server::HttpServer<H, F>>
where
    H: server::IntoHttpHandler + 'static,
    F: Fn() -> H + Send + Clone + 'static,
{
    if let (Some(addr), Some(path), Some(password)) = (
        config.tls_listen_addr(),
        config.tls_identity_path(),
        config.tls_identity_password(),
    ) {
        use native_tls::{Identity, TlsAcceptor};
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(path).unwrap();
        let mut data = vec![];
        file.read_to_end(&mut data).unwrap();
        let identity =
            Identity::from_pkcs12(&data, password).context(ServerError::TlsInitFailed)?;

        let acceptor = TlsAcceptor::builder(identity)
            .build()
            .context(ServerError::TlsInitFailed)?;

        server = server
            .bind_tls(addr, acceptor)
            .context(ServerError::BindFailed)?;
    }

    Ok(server)
}

#[cfg(not(feature = "ssl"))]
fn listen_ssl<H, F>(
    server: server::HttpServer<H, F>,
    config: &Config,
) -> Result<server::HttpServer<H, F>, ServerError>
where
    H: server::IntoHttpHandler + 'static,
    F: Fn() -> H + Send + Clone + 'static,
{
    if config.tls_listen_addr().is_some()
        || config.tls_identity_path().is_some()
        || config.tls_identity_password().is_some()
    {
        Err(ServerError::TlsNotSupported.into())
    } else {
        Ok(server)
    }
}

/// Keeps the address to the running http servers and helps with start/stop handling.
pub struct HttpServer(Recipient<StopServer>);

impl HttpServer {
    /// Given a relay config spawns the server together with all actors and lets them run forever.
    ///
    /// Effectively this boots the server.
    pub fn start(config: Config) -> Result<Self> {
        let config = Arc::new(config);

        Controller::from_registry().do_send(Configure {
            shutdown_timeout: config.shutdown_timeout(),
        });

        let state = ServiceState::start(config.clone())?;
        let mut server = server::new(move || make_app(state.clone()));
        server = server
            .workers(config.cpu_concurrency())
            .shutdown_timeout(config.shutdown_timeout().as_secs() as u16)
            .keep_alive(config.keepalive_timeout().as_secs() as usize)
            .maxconn(config.max_connections())
            .maxconnrate(config.max_connection_rate())
            .backlog(config.max_pending_connections())
            .disable_signals();

        server = listen(server, &config)?;
        server = listen_ssl(server, &config)?;

        dump_listen_infos(&server);
        let recipient = server.start().recipient();
        Ok(Self(recipient))
    }

    /// Triggers the shutdown process by sending [`actix_web::server::StopServer`] to the running http server.
    pub fn shutdown(&self, graceful: bool) {
        let Self(recipient) = self;
        relay_log::info!("Shutting down HTTP server");
        recipient.send(StopServer { graceful }).wait().ok();
    }
}

impl fmt::Debug for HttpServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HttpServer")
            .field(&"actix::Recipient<actix_web::server::StopServer>")
            .finish()
    }
}
