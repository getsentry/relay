use std::sync::Arc;

use actix::prelude::*;
use actix_web::{server, App};
use failure::ResultExt;
use listenfd::ListenFd;
use sentry_actix::SentryMiddleware;

use semaphore_common::Config;

use actors::events::EventManager;
use actors::keys::KeyCache;
use actors::project::ProjectCache;
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
    config: Arc<Config>,
    key_cache: Addr<KeyCache>,
    project_cache: Addr<ProjectCache>,
    upstream_relay: Addr<UpstreamRelay>,
    event_manager: Addr<EventManager>,
}

impl ServiceState {
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
    let sys = System::new("relay");

    let upstream_relay = UpstreamRelay::new(config.clone()).start();

    let service_state = ServiceState {
        config: config.clone(),
        upstream_relay: upstream_relay.clone(),
        key_cache: KeyCache::new(config.clone(), upstream_relay.clone()).start(),
        project_cache: ProjectCache::new(config.clone(), upstream_relay.clone()).start(),
        event_manager: EventManager::new(upstream_relay.clone()).start(),
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

    server.keep_alive(None).system_exit().start();
    let _ = sys.run();

    info!("relay shutdown complete");

    Ok(())
}
