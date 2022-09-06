use std::fmt;
use std::sync::Arc;

use actix::prelude::*;
use actix_web::{server, App};
use failure::ResultExt;
use failure::{Backtrace, Context, Fail};
use listenfd::ListenFd;
use once_cell::race::OnceBox;

use relay_aws_extension::AwsExtension;
use relay_config::Config;
use relay_metrics::Aggregator;
use relay_redis::RedisPool;
use relay_system::Addr;
use relay_system::{Configure, Controller};

use crate::actors::envelopes::EnvelopeManager;
use crate::actors::healthcheck::Healthcheck;
use crate::actors::outcome::OutcomeProducer;
use crate::actors::outcome_aggregator::OutcomeAggregator;
use crate::actors::processor::EnvelopeProcessor;
use crate::actors::project_cache::ProjectCache;
use crate::actors::relays::RelayCache;
use crate::actors::upstream::UpstreamRelay;
use crate::middlewares::{
    AddCommonHeaders, ErrorHandlers, Metrics, ReadRequestMiddleware, SentryMiddleware,
};
use crate::utils::BufferGuard;
use crate::{endpoints, utils};

pub static REGISTRY: OnceBox<Registry> = OnceBox::new();

/// Common error type for the relay server.
#[derive(Debug)]
pub struct ServerError {
    inner: Context<ServerErrorKind>,
}

/// Indicates the type of failure of the server.
#[derive(Debug, Fail, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ServerErrorKind {
    /// Binding failed.
    #[fail(display = "bind to interface failed")]
    BindFailed,

    /// Listening on the HTTP socket failed.
    #[fail(display = "listening failed")]
    ListenFailed,

    /// A TLS error ocurred.
    #[fail(display = "could not initialize the TLS server")]
    TlsInitFailed,

    /// TLS support was not compiled in.
    #[fail(display = "compile with the `ssl` feature to enable SSL support")]
    TlsNotSupported,

    /// GeoIp construction failed.
    #[fail(display = "could not load the Geoip Db")]
    GeoIpError,

    /// Configuration failed.
    #[fail(display = "configuration error")]
    ConfigError,

    /// Initializing the Kafka producer failed.
    #[fail(display = "could not initialize kafka producer")]
    KafkaError,

    /// Initializing the Redis cluster client failed.
    #[fail(display = "could not initialize redis cluster client")]
    RedisError,
}

impl Fail for ServerError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl ServerError {
    /// Returns the error kind of the error.
    pub fn kind(&self) -> ServerErrorKind {
        *self.inner.get_context()
    }
}

impl From<ServerErrorKind> for ServerError {
    fn from(kind: ServerErrorKind) -> ServerError {
        ServerError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ServerErrorKind>> for ServerError {
    fn from(inner: Context<ServerErrorKind>) -> ServerError {
        ServerError { inner }
    }
}

#[derive(Clone)]
pub struct Registry {
    pub healthcheck: Addr<Healthcheck>,
    pub outcome_producer: Addr<OutcomeProducer>,
    pub outcome_aggregator: Addr<OutcomeAggregator>,
    pub processor: actix::Addr<EnvelopeProcessor>,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("healthcheck", &self.healthcheck)
            .field("processor", &format_args!("Addr<Processor>"))
            .finish()
    }
}

/// Server state.
#[derive(Clone)]
pub struct ServiceState {
    config: Arc<Config>,
    buffer_guard: Arc<BufferGuard>,
    _runtime: Arc<tokio::runtime::Runtime>,
}

impl ServiceState {
    /// Starts all services and returns addresses to all of them.
    pub fn start(config: Arc<Config>) -> Result<Self, ServerError> {
        let system = System::current();
        let registry = system.registry();

        let upstream_relay = UpstreamRelay::new(config.clone());
        registry.set(Arbiter::start(|_| upstream_relay));

        let outcome_runtime = utils::tokio_runtime_with_actix();
        let guard = outcome_runtime.enter();
        let outcome_producer = OutcomeProducer::create(config.clone())?.start();
        let outcome_aggregator = OutcomeAggregator::new(&config, outcome_producer.clone()).start();
        drop(guard);

        let redis_pool = match config.redis() {
            Some(redis_config) if config.processing_enabled() => {
                Some(RedisPool::new(redis_config).context(ServerErrorKind::RedisError)?)
            }
            _ => None,
        };

        // Enter and enter the tokio runtime so we can start spawning tasks from the outside.
        let main_runtime = utils::tokio_runtime_with_actix();
        let _guard = main_runtime.enter();

        let buffer = Arc::new(BufferGuard::new(config.envelope_buffer_size()));
        let processor = EnvelopeProcessor::start(config.clone(), redis_pool.clone())?;
        let envelope_manager = EnvelopeManager::create(config.clone())?;
        registry.set(Arbiter::start(|_| envelope_manager));

        let project_cache = ProjectCache::new(config.clone(), redis_pool);
        let project_cache = Arbiter::start(|_| project_cache);
        registry.set(project_cache.clone());

        let healthcheck = Healthcheck::new(config.clone()).start();
        registry.set(RelayCache::new(config.clone()).start());

        let aggregator = Aggregator::new(config.aggregator_config(), project_cache.recipient());
        registry.set(Arbiter::start(|_| aggregator));

        if let Some(aws_api) = config.aws_runtime_api() {
            if let Ok(aws_extension) = AwsExtension::new(aws_api) {
                Arbiter::start(|_| aws_extension);
            }
        }

        REGISTRY
            .set(Box::new(Registry {
                processor,
                healthcheck,
                outcome_producer,
                outcome_aggregator,
            }))
            .unwrap();

        Ok(ServiceState {
            buffer_guard: buffer,
            config,
            _runtime: Arc::new(main_runtime),
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
        .configure(endpoints::configure_app)
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
) -> Result<server::HttpServer<H, F>, ServerError>
where
    H: server::IntoHttpHandler + 'static,
    F: Fn() -> H + Send + Clone + 'static,
{
    Ok(
        match ListenFd::from_env()
            .take_tcp_listener(0)
            .context(ServerErrorKind::BindFailed)?
        {
            Some(listener) => server.listen(listener),
            None => server
                .bind(config.listen_addr())
                .context(ServerErrorKind::BindFailed)?,
        },
    )
}

#[cfg(feature = "ssl")]
fn listen_ssl<H, F>(
    mut server: server::HttpServer<H, F>,
    config: &Config,
) -> Result<server::HttpServer<H, F>, ServerError>
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
            Identity::from_pkcs12(&data, password).context(ServerErrorKind::TlsInitFailed)?;

        let acceptor = TlsAcceptor::builder(identity)
            .build()
            .context(ServerErrorKind::TlsInitFailed)?;

        server = server
            .bind_tls(addr, acceptor)
            .context(ServerErrorKind::BindFailed)?;
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
        Err(ServerErrorKind::TlsNotSupported.into())
    } else {
        Ok(server)
    }
}

/// Given a relay config spawns the server together with all actors and lets them run forever.
///
/// Effectively this boots the server.
pub fn start(config: Config) -> Result<Recipient<server::StopServer>, ServerError> {
    let config = Arc::new(config);

    Controller::from_registry().do_send(Configure {
        shutdown_timeout: config.shutdown_timeout(),
    });

    let state = ServiceState::start(config.clone())?;
    let mut server = server::new(move || make_app(state.clone()));
    server = server
        .workers(config.cpu_concurrency())
        .shutdown_timeout(config.shutdown_timeout().as_secs() as u16)
        .maxconn(config.max_connections())
        .maxconnrate(config.max_connection_rate())
        .backlog(config.max_pending_connections())
        .disable_signals();

    server = listen(server, &config)?;
    server = listen_ssl(server, &config)?;

    dump_listen_infos(&server);
    Ok(server.start().recipient())
}
