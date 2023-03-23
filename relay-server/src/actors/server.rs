use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::{header, HeaderValue};
use axum::{middleware, ServiceExt};
use axum_server::{Handle, Server};
use relay_config::Config;
use relay_log::_sentry::integrations::tower::NewSentryLayer;
use relay_system::{Controller, Service, Shutdown};
use tower::ServiceBuilder;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::constants;
use crate::middlewares::{
    self, CatchPanicLayer, HandleErrorLayer, NormalizePathLayer, RequestDecompressionLayer,
};
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
    service: ServiceState,
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

        relay_log::info!("spawning http server");
        relay_log::info!("  listening on http://{}/", config.listen_addr());

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

        Ok(Self {
            service,
            server,
            handle,
        })
    }
}

impl Service for HttpServer {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            service,
            server,
            handle,
        } = self;

        // Build the router middleware into a single service which runs _after_ routing. Service
        // builder order defines layers added first will be called first. This means:
        //  - Requests go from top to bottom
        //  - Responses go from bottom to top
        let middleware = ServiceBuilder::new()
            .layer(middleware::from_fn(middlewares::metrics))
            .layer(CatchPanicLayer::custom(middlewares::handle_panic))
            .layer(SetResponseHeaderLayer::overriding(
                header::SERVER,
                HeaderValue::from_static(constants::SERVER),
            ))
            .layer(NewSentryLayer::new_from_top())
            .layer(HandleErrorLayer::new(middlewares::decompression_error))
            .layer(RequestDecompressionLayer::new());

        let router = crate::endpoints::routes(service.config())
            .layer(middleware)
            .with_state(service);

        // Bundle middlewares that need to run _before_ routing, which need to wrap the router.
        // ConnectInfo is special as it needs to last.
        let app = ServiceBuilder::new()
            .layer(NormalizePathLayer::new())
            .service(router)
            .into_make_service_with_connect_info::<SocketAddr>();

        tokio::spawn(server.serve(app));

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
