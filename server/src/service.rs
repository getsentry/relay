use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

use actix;
use actix_web::{server, App};
use failure::ResultExt;

use semaphore_config::Config;
use semaphore_trove::{Trove, TroveState};

use endpoints;
use errors::{ServerError, ServerErrorKind};
use middlewares::{AddCommonHeaders, CaptureSentryError, ErrorHandlers, Metrics};
use utils::get_external_tcp_listeners;

fn dump_listen_infos<H: server::HttpHandler>(
    server: &server::HttpServer<H>,
    tls_server: Option<&server::HttpServer<H>>,
) {
    info!("spawning http server");
    for addr in server.addrs() {
        info!("  listening on: http://{}/", addr);
    }
    if let Some(tls_server) = tls_server {
        for addr in tls_server.addrs() {
            info!("  listening on: https://{}/", addr);
        }
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

fn bind_server<H: server::HttpHandler>(
    listeners: &mut Vec<TcpListener>,
    server: server::HttpServer<H>,
    listen_addr: SocketAddr,
) -> Result<server::HttpServer<H>, ServerError> {
    Ok(if !listeners.is_empty() {
        server.listen(listeners.remove(0))
    } else {
        server
            .bind(listen_addr)
            .context(ServerErrorKind::BindFailed)?
    })
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

    let mut listeners = get_external_tcp_listeners();
    server = bind_server(&mut listeners, server, config.listen_addr())?;

    let tls = {
        #[cfg(feature = "with_ssl")]
        {
            use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
            if let (Some(addr), Some(pk), Some(cert)) = (
                config.tls_listen_addr(),
                config.tls_private_key_path(),
                config.tls_certificate_path(),
            ) {
                Some((
                    bind_server(
                        &mut listeners,
                        server::new(move || make_app(state.clone())),
                        addr,
                    ),
                    {
                        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())
                            .context(ServerErrorKind::TlsInitFailed)?;
                        builder
                            .set_private_key_file(&pk, SslFiletype::PEM)
                            .context(ServerErrorKind::TlsInitFailed)?;
                        builder
                            .set_certificate_chain_file(&cert)
                            .context(ServerErrorKind::TlsInitFailed)?;
                        builder
                    },
                ))
            } else {
                None
            }
        }
        #[cfg(not(feature = "with_ssl"))]
        {
            None::<(server::HttpServer<_>, ())>
        }
    };

    dump_listen_infos(&server, tls.as_ref().map(|x| &x.0));
    info!("spawning relay server");

    let sys = actix::System::new("relay");
    server.system_exit().start();
    #[cfg(feature = "with_ssl")]
    {
        if let Some((tls_server, ssl_builder)) = tls {
            tls_server
                .system_exit()
                .start_ssl(ssl_builder)
                .context(ServerErrorKind::TlsInitFailed)?;
        }
    }
    let _ = sys.run();

    trove
        .abdicate()
        .context(ServerErrorKind::TroveGovernSpawnFailed)?;
    info!("relay shutdown complete");

    Ok(())
}
