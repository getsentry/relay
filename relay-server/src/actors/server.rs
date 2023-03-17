use std::sync::Arc;

use axum::http::{header, HeaderValue};
use axum::{middleware, Router, ServiceExt};
use axum_server::{Handle, Server};
use relay_config::Config;
use relay_log::_sentry::integrations::tower::NewSentryLayer;
use relay_system::{Controller, Service, Shutdown};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::constants;
use crate::middlewares::{self, NormalizePath};
use crate::service::ServiceState;
use crate::statsd::RelayCounters;

/// Indicates the type of failure of the server.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ServerError {
    /// TLS support was not compiled in.
    #[error("SSL is no longer supported by Relay, please use a proxy in front")]
    TlsNotSupported,
}

/// HTTP server service.
///
/// This is the main HTTP server of Relay which hosts all [services](ServiceState) and dispatches
/// incoming traffic to them. The server stops when a [`Shutdown`] is triggered.
pub struct HttpServer {
    app: NormalizePath<Router>,
    server: Server,
    handle: Handle,
}

impl HttpServer {
    pub fn new(config: Arc<Config>, service: ServiceState) -> Result<Self, ServerError> {
        relay_statsd::metric!(counter(RelayCounters::ServerStarting) += 1);

        // Inform the user about a removed feature.
        if config.tls_listen_addr().is_some()
            || config.tls_identity_password().is_some()
            || config.tls_identity_path().is_some()
        {
            return Err(ServerError::TlsNotSupported);
        }

        let app = crate::endpoints::routes()
            .layer(NewSentryLayer::new_from_top())
            .layer(middleware::from_fn(middlewares::metrics))
            .layer(SetResponseHeaderLayer::overriding(
                header::SERVER,
                HeaderValue::from_static(constants::SERVER),
            ))
            .with_state(service);

        let app = NormalizePath::new(app);

        let handle = Handle::new();
        let server = axum_server::bind(config.listen_addr())
            // todo http_config
            // todo addr_incoming_config
            .handle(handle.clone());

        // let mut server = server::new(move || make_app(service.clone()));
        // server = server
        //     .workers(config.cpu_concurrency())
        //     .shutdown_timeout(config.shutdown_timeout().as_secs() as u16)
        //     .keep_alive(config.keepalive_timeout().as_secs() as usize)
        //     .maxconn(config.max_connections())
        //     .maxconnrate(config.max_connection_rate())
        //     .backlog(config.max_pending_connections())
        //     .disable_signals();

        // TODO(ja): dump_listen_infos(&server);
        Ok(Self {
            app,
            server,
            handle,
        })
    }
}

impl Service for HttpServer {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            app,
            server,
            handle,
        } = self;

        tokio::spawn(server.serve(app.into_make_service()));

        tokio::spawn(async move {
            let Shutdown { timeout } = Controller::shutdown_handle().notified().await;
            relay_log::info!("Shutting down HTTP server");

            match timeout {
                Some(timeout) => handle.graceful_shutdown(Some(timeout)),
                None => handle.shutdown(),
            }
        });
    }
}
