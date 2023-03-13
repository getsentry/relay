use actix_web::{server, App};
use anyhow::{Context, Result};
use listenfd::ListenFd;
use relay_config::Config;
use relay_statsd::metric;
use relay_system::{Addr, Controller, Service, Shutdown};

use crate::middlewares::{
    AddCommonHeaders, ErrorHandlers, Metrics, ReadRequestMiddleware, SentryMiddleware,
};
use crate::service::ServiceState;
use crate::statsd::RelayCounters;

/// Indicates the type of failure of the server.
#[allow(clippy::enum_variant_names)]
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
}

fn make_app(state: ServiceState) -> App<ServiceState> {
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
        use std::fs::File;
        use std::io::Read;

        use native_tls::{Identity, TlsAcceptor};

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

/// HTTP server service.
///
/// This is the main HTTP server of Relay which hosts all [services](ServiceState) and dispatches
/// incoming traffic to them. The server stops when a [`Shutdown`] is triggered.
pub struct HttpServer {
    http_server: actix::Recipient<server::StopServer>,
}

impl HttpServer {
    pub fn start(config: &Config, service: ServiceState) -> anyhow::Result<Addr<()>> {
        metric!(counter(RelayCounters::ServerStarting) += 1);

        let mut server = server::new(move || make_app(service.clone()));
        server = server
            .workers(config.cpu_concurrency())
            .shutdown_timeout(config.shutdown_timeout().as_secs() as u16)
            .keep_alive(config.keepalive_timeout().as_secs() as usize)
            .maxconn(config.max_connections())
            .maxconnrate(config.max_connection_rate())
            .backlog(config.max_pending_connections())
            .disable_signals();

        server = listen(server, config)?;
        server = listen_ssl(server, config)?;
        dump_listen_infos(&server);

        let http_server = server.start().recipient();
        Ok(Self { http_server }.start())
    }
}

impl Service for HttpServer {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let Shutdown { timeout } = Controller::shutdown_handle().notified().await;
            relay_log::info!("Shutting down HTTP server");

            // We assume graceful shutdown if we're given a timeout. The actix-web http server
            // is configured with the same timeout, so it will match. Unfortunately, we have to
            // drop all errors.
            let graceful = timeout.is_some();
            self.http_server
                .do_send(server::StopServer { graceful })
                .ok();
        });
    }
}
