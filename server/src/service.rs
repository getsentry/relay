use std::sync::Arc;

use actix;
use actix::Actor;
use actix::Addr;
use actix_web::{server, App};
use failure::ResultExt;
use listenfd::ListenFd;

use semaphore_aorta::AortaConfig;
use semaphore_config::Config;
use semaphore_trove::{Trove, TroveState};

use actors::keys::KeyManager;
use actors::upstream::UpstreamRelay;
use endpoints;
use errors::{ServerError, ServerErrorKind};
use middlewares::{AddCommonHeaders, CaptureSentryError, ErrorHandlers, Metrics};

fn dump_listen_infos<H: server::HttpHandler>(server: &server::HttpServer<H>) {
    info!("spawning http server");
    for (addr, scheme) in server.addrs_with_scheme() {
        info!("  listening on: {}://{}/", scheme, addr);
    }
}

/// Server state.
#[derive(Clone)]
pub struct ServiceState {
    trove_state: Arc<TroveState>,
    key_manager: Addr<KeyManager>,
    config: Arc<Config>,
}

impl ServiceState {
    /// Returns an atomically counted reference to the trove state.
    pub fn trove_state(&self) -> Arc<TroveState> {
        self.trove_state.clone()
    }

    /// Returns an atomically counted reference to the aorta config.
    pub fn aorta_config(&self) -> Arc<AortaConfig> {
        self.trove_state.config().clone()
    }

    /// Returns an atomically counted reference to the config.
    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
    }

    /// Checks if the service is healthy.
    pub fn is_healthy(&self) -> bool {
        self.trove_state.is_healthy()
    }

    /// Returns current key manager
    pub fn key_manager(&self) -> Addr<KeyManager> {
        self.key_manager.clone()
    }
}

/// The actix app type for the relay web service.
pub type ServiceApp = App<ServiceState>;

fn make_app(state: ServiceState) -> ServiceApp {
    let mut app = App::with_state(state)
        .middleware(Metrics)
        .middleware(CaptureSentryError)
        .middleware(AddCommonHeaders)
        .middleware(ErrorHandlers);

    app = endpoints::healthcheck::configure_app(app);
    app = endpoints::store::configure_app(app);
    app = endpoints::forward::configure_app(app);
    app = endpoints::queries::configure_app(app);

    app
}

/// Given a relay config spawns the server and lets it run until it stops.
///
/// This not only spawning the server but also a governed trove in the
/// background.  Effectively this boots the server.
pub fn run(config: Config) -> Result<(), ServerError> {
    let config = Arc::new(config);
    let trove = Arc::new(Trove::new(config.make_aorta_config()));
    trove
        .govern()
        .context(ServerErrorKind::TroveGovernSpawnFailed)?;

    let sys = actix::System::new("relay");

    let upstream_request_manager = UpstreamRelay::new(
        config.credentials().cloned(),
        config.upstream_descriptor().clone().into_owned(),
        trove.state(),
    );

    let upstream_request_manager_addr = upstream_request_manager.start();
    let key_manager = KeyManager::new(upstream_request_manager_addr);

    let service_state = ServiceState {
        trove_state: trove.state(),
        key_manager: key_manager.start(),
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

    trove
        .abdicate()
        .context(ServerErrorKind::TroveGovernSpawnFailed)?;
    info!("relay shutdown complete");

    Ok(())
}
