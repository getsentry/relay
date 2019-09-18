use std::fmt;
use std::sync::Arc;

use actix::prelude::*;
use actix_web::client::ClientConnector;
use actix_web::{server, App, HttpResponse};
use failure::ResultExt;
use failure::{Backtrace, Context, Fail};
use listenfd::ListenFd;
use sentry_actix::SentryMiddleware;

use semaphore_common::Config;

use crate::actors::events::EventManager;
use crate::actors::keys::KeyCache;
use crate::actors::outcome::OutcomeProducer;
use crate::actors::project::ProjectCache;
use crate::actors::upstream::UpstreamRelay;
use crate::constants::SHUTDOWN_TIMEOUT;
use crate::endpoints;
use crate::middlewares::{AddCommonHeaders, ErrorHandlers, Metrics};

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
    #[fail(display = "compile with the `with_ssl` feature to enable SSL support")]
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
    key_cache: Addr<KeyCache>,
    project_cache: Addr<ProjectCache>,
    upstream_relay: Addr<UpstreamRelay>,
    event_manager: Addr<EventManager>,
    outcome_producer: Addr<OutcomeProducer>,
}

impl ServiceState {
    /// Starts all services and returns addresses to all of them.
    pub fn start(config: Config) -> Result<Self, ServerError> {
        let config = Arc::new(config);
        let upstream_relay = UpstreamRelay::new(config.clone()).start();
        let outcome_producer = OutcomeProducer::create(config.clone())?.start();

        let event_manager = EventManager::create(
            config.clone(),
            upstream_relay.clone(),
            outcome_producer.clone(),
        )
        .context(ServerErrorKind::ConfigError)?
        .start();

        Ok(ServiceState {
            config: config.clone(),
            upstream_relay: upstream_relay.clone(),
            key_cache: KeyCache::new(config.clone(), upstream_relay.clone()).start(),
            project_cache: ProjectCache::new(config.clone(), upstream_relay.clone()).start(),
            event_manager,
            outcome_producer,
        })
    }

    /// Returns an atomically counted reference to the config.
    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
    }

    /// Returns the current relay public key cache.
    pub fn key_cache(&self) -> Addr<KeyCache> {
        self.key_cache.clone()
    }

    /// Returns the actor for upstream relay.
    pub fn upstream_relay(&self) -> Addr<UpstreamRelay> {
        self.upstream_relay.clone()
    }

    /// Returns the current project cache.
    pub fn project_cache(&self) -> Addr<ProjectCache> {
        self.project_cache.clone()
    }

    /// Returns the current event manager.
    pub fn event_manager(&self) -> Addr<EventManager> {
        self.event_manager.clone()
    }

    pub fn outcome_producer(&self) -> Addr<OutcomeProducer> {
        self.outcome_producer.clone()
    }
}

/// The actix app type for the relay web service.
pub type ServiceApp = App<ServiceState>;

fn make_app(state: ServiceState) -> ServiceApp {
    let mut app = App::with_state(state)
        .middleware(SentryMiddleware::new())
        .middleware(Metrics)
        .middleware(AddCommonHeaders)
        .middleware(ErrorHandlers);

    app = app.scope("/api/relay", |mut scope| {
        scope = endpoints::healthcheck::configure_scope(scope);
        scope = endpoints::events::configure_scope(scope);
        // never forward /api/relay, as that prefix is used for stuff like healthchecks
        scope.default_resource(|r| r.f(|_| HttpResponse::NotFound()))
    });

    app = endpoints::project_configs::configure_app(app);
    app = endpoints::public_keys::configure_app(app);
    app = endpoints::store::configure_app(app);
    app = endpoints::forward::configure_app(app);

    app
}

fn dump_listen_infos<H, F>(server: &server::HttpServer<H, F>)
where
    H: server::IntoHttpHandler + 'static,
    F: Fn() -> H + Send + Clone + 'static,
{
    log::info!("spawning http server");
    for (addr, scheme) in server.addrs_with_scheme() {
        log::info!("  listening on: {}://{}/", scheme, addr);
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

#[cfg(feature = "with_ssl")]
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

#[cfg(not(feature = "with_ssl"))]
fn listen_ssl<H>(
    server: server::HttpServer<H>,
    config: &Config,
) -> Result<server::HttpServer<H>, ServerError>
where
    H: server::IntoHttpHandler + 'static,
{
    if config.tls_listen_addr().is_some()
        || config.tls_identity_path().is_some()
        || config.tls_identity_password().is_some()
    {
        Err(ServerErrorKind::TlsNotSupported)
    } else {
        Ok(server)
    }
}

/// Given a relay config spawns the server together with all actors and lets them run forever.
///
/// Effectively this boots the server.
pub fn start(state: ServiceState) -> Result<Recipient<server::StopServer>, ServerError> {
    let config = state.config();
    let mut server = server::new(move || make_app(state.clone()));
    server = server.shutdown_timeout(SHUTDOWN_TIMEOUT).disable_signals();

    let connector = ClientConnector::default()
        .limit(config.max_concurrent_requests())
        .start();

    System::current().registry().set(connector);

    server = listen(server, &config)?;
    server = listen_ssl(server, &config)?;

    dump_listen_infos(&server);
    Ok(server.start().recipient())
}
