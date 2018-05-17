use std::sync::Arc;

use actix;
use actix_web::{server, App};
use failure::ResultExt;
use listenfd::ListenFd;

use semaphore_config::Config;
use semaphore_trove::{Trove, TroveState};

use endpoints;
use errors::{ServerError, ServerErrorKind};
use middlewares::{AddCommonHeaders, CaptureSentryError, ErrorHandlers, Metrics};

fn dump_listen_infos<H: server::HttpHandler>(server: &server::HttpServer<H>) {
    info!("spawning http server");
    for (addr, scheme) in server.addrs_with_scheme() {
        info!("  listening on: {}://{}/", scheme, addr);
    }
}

/// The actix app type for the relay web service.
pub type ServiceApp = App<Arc<TroveState>>;

fn make_app(state: Arc<TroveState>) -> ServiceApp {
    let mut app = App::with_state(state)
        .middleware(Metrics)
        .middleware(CaptureSentryError)
        .middleware(AddCommonHeaders)
        .middleware(ErrorHandlers);

    macro_rules! register_endpoint {
        ($name:ident) => {
            app = endpoints::$name::configure_app(app);
        };
    }

    register_endpoint!(healthcheck);
    register_endpoint!(store);

    app
}

/// Given a relay config spawns the server and lets it run until it stops.
///
/// This not only spawning the server but also a governed trove in the
/// background.  Effectively this boots the server.
pub fn run(config: Config) -> Result<(), ServerError> {
    let trove = Arc::new(Trove::new(config.make_aorta_config()));
    let state = trove.state();
    trove
        .govern()
        .context(ServerErrorKind::TroveGovernSpawnFailed)?;

    let server_state = state.clone();
    let mut server = server::new(move || make_app(server_state.clone()));

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

    let sys = actix::System::new("relay");
    server.system_exit().start();
    let _ = sys.run();

    trove
        .abdicate()
        .context(ServerErrorKind::TroveGovernSpawnFailed)?;
    info!("relay shutdown complete");

    Ok(())
}
