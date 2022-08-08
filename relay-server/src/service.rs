use std::fmt;
use std::sync::Arc;

use actix::prelude::*;
use actix_web::{server, App};
use failure::ResultExt;
use failure::{Backtrace, Context, Fail};
use listenfd::ListenFd;

use relay_aws_extension::AwsExtension;
use relay_common::clone;
use relay_config::Config;
use relay_metrics::Aggregator;
use relay_system::{Configure, Controller};

use crate::actors::envelopes::{EnvelopeManager, EnvelopeProcessor};
use crate::actors::healthcheck::Healthcheck;
use crate::actors::outcome::OutcomeProducer;
use crate::actors::outcome_aggregator::OutcomeAggregator;
use crate::actors::project_cache::ProjectCache;
use crate::actors::relays::RelayCache;
use crate::actors::upstream::UpstreamRelay;
use crate::endpoints;
use crate::middlewares::{
    AddCommonHeaders, ErrorHandlers, Metrics, ReadRequestMiddleware, SentryMiddleware,
};

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

/// Server state.
#[derive(Clone)]
pub struct ServiceState {
    config: Arc<Config>,
    _runtime: Arc<tokio::runtime::Runtime>,
}

impl ServiceState {
    /// Starts all services and returns addresses to all of them.
    pub fn start(config: Arc<Config>) -> Result<Self, ServerError> {
        let system = System::current();
        let registry = system.registry();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .on_thread_start(clone!(system, || System::set_current(system.clone())))
            .build()
            .unwrap();

        // Enter the tokio runtime so we can start spawning tasks from the outside.
        let _guard = runtime.enter();

        let upstream_relay = UpstreamRelay::new(config.clone());
        registry.set(Arbiter::start(|_| upstream_relay));

        let outcome_producer = OutcomeProducer::create(config.clone())?;
        let outcome_producer = Arbiter::start(|_| outcome_producer);
        registry.set(outcome_producer.clone());

        let outcome_aggregator = OutcomeAggregator::new(&config, outcome_producer.recipient());
        registry.set(outcome_aggregator.start());

        let processor = EnvelopeProcessor::start(config.clone())?;
        let envelope_manager = EnvelopeManager::create(config.clone(), processor)?;
        registry.set(Arbiter::start(|_| envelope_manager));

        let project_cache = ProjectCache::new(config.clone())?;
        let project_cache = Arbiter::start(|_| project_cache);
        registry.set(project_cache.clone());

        Healthcheck::new(config.clone()).start(); // TODO(tobias): Registry is implicit
        registry.set(RelayCache::new(config.clone()).start());

        let aggregator = Aggregator::new(config.aggregator_config(), project_cache.recipient());
        registry.set(Arbiter::start(|_| aggregator));

        if let Some(aws_api) = config.aws_runtime_api() {
            if let Ok(aws_extension) = AwsExtension::new(aws_api) {
                Arbiter::start(|_| aws_extension);
            }
        }

        Ok(ServiceState {
            config,
            _runtime: Arc::new(runtime),
        })
    }

    /// Returns an atomically counted reference to the config.
    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
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
