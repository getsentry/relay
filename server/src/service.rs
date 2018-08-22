use std::sync::Arc;

use actix::{Actor, Addr, SyncArbiter, System};
use actix_web::{server, App};
use failure::ResultExt;
use listenfd::ListenFd;
use num_cpus;
use sentry_actix::SentryMiddleware;

use semaphore_aorta::AortaConfig;
use semaphore_config::Config;

use actors::events::EventProcessor;
use actors::keys::KeyManager;
use actors::project::ProjectManager;
use actors::upstream::UpstreamRelay;
use endpoints;
use errors::{ServerError, ServerErrorKind};
use middlewares::{AddCommonHeaders, ErrorHandlers, Metrics};

fn dump_listen_infos<H: server::HttpHandler>(server: &server::HttpServer<H>) {
    info!("spawning http server");
    for (addr, scheme) in server.addrs_with_scheme() {
        info!("  listening on: {}://{}/", scheme, addr);
    }
}

/// Server state.
#[derive(Clone)]
pub struct ServiceState {
    aorta_config: Arc<AortaConfig>,
    key_manager: Addr<KeyManager>,
    project_manager: Addr<ProjectManager>,
    upstream_relay: Addr<UpstreamRelay>,
    event_processor: Addr<EventProcessor>,
    config: Arc<Config>,
}

impl ServiceState {
    /// Returns an atomically counted reference to the aorta config.
    pub fn aorta_config(&self) -> Arc<AortaConfig> {
        self.aorta_config.clone()
    }

    /// Returns an atomically counted reference to the config.
    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
    }

    /// Returns current key manager
    pub fn key_manager(&self) -> Addr<KeyManager> {
        self.key_manager.clone()
    }

    /// Returns actor for upstream relay
    pub fn upstream_relay(&self) -> Addr<UpstreamRelay> {
        self.upstream_relay.clone()
    }

    /// Returns current project manager
    pub fn project_manager(&self) -> Addr<ProjectManager> {
        self.project_manager.clone()
    }

    /// Returns a pool of event processors.
    pub fn event_processor(&self) -> Addr<EventProcessor> {
        self.event_processor.clone()
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

    app = endpoints::forward::configure_app(app);
    app = endpoints::healthcheck::configure_app(app);
    app = endpoints::project_configs::configure_app(app);
    app = endpoints::public_keys::configure_app(app);
    app = endpoints::store::configure_app(app);

    app
}

/// Given a relay config spawns the server together with all actors and lets them run forever.
///
/// Effectively this boots the server.
pub fn run(config: Config) -> Result<(), ServerError> {
    let config = Arc::new(config);
    let aorta_config = config.make_aorta_config();

    let sys = System::new("relay");

    let upstream_relay = UpstreamRelay::new(
        config.credentials().cloned(),
        config.upstream_descriptor().clone().into_owned(),
        config.aorta_auth_retry_interval(),
    ).start();

    // TODO: Make the number configurable via config file
    let event_threads = num_cpus::get();

    let service_state = ServiceState {
        aorta_config,
        upstream_relay: upstream_relay.clone(),
        key_manager: KeyManager::new(upstream_relay.clone()).start(),
        project_manager: ProjectManager::new(upstream_relay.clone()).start(),
        event_processor: SyncArbiter::start(event_threads, move || {
            EventProcessor::new(upstream_relay.clone())
        }),
        config: config.clone(),
    };

    let mut server = server::new(move || make_app(service_state.clone()));
    let mut listenfd = ListenFd::from_env();

    server = if let Some(listener) = listenfd
        .take_tcp_listener(0)
        .context(ServerErrorKind::BindFailed)?
    {
        server.listen(listener)
    } else {
        server
            .bind(config.listen_addr())
            .context(ServerErrorKind::BindFailed)?
    };

    #[cfg(feature = "with_ssl")]
    {
        use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

        if let (Some(addr), Some(pk), Some(cert)) = (
            config.tls_listen_addr(),
            config.tls_private_key_path(),
            config.tls_certificate_path(),
        ) {
            let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())
                .context(ServerErrorKind::TlsInitFailed)?;
            builder
                .set_private_key_file(&pk, SslFiletype::PEM)
                .context(ServerErrorKind::TlsInitFailed)?;
            builder
                .set_certificate_chain_file(&cert)
                .context(ServerErrorKind::TlsInitFailed)?;
            server = if let Some(listener) = listenfd
                .take_tcp_listener(1)
                .context(ServerErrorKind::BindFailed)?
            {
                server.listen_ssl(listener)?
            } else {
                server
                    .bind_ssl(config.listen_addr(), builder)
                    .context(ServerErrorKind::BindFailed)?
            };
        }
    }

    dump_listen_infos(&server);
    info!("spawning relay server");

    server.system_exit().start();
    let _ = sys.run();

    info!("relay shutdown complete");

    Ok(())
}
