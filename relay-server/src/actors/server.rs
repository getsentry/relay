use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderValue};
use axum::{middleware, ServiceExt};
use axum_server::{AddrIncomingConfig, Handle, HttpConfig, Server};
use relay_config::Config;
use relay_log::tower::NewSentryLayer;
use relay_system::{Controller, Service, Shutdown};
use tower::ServiceBuilder;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;

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

/// Set the number of keep-alive retransmissions to be carried out before declaring that remote end
/// is not available.
const KEEPALIVE_RETRIES: u32 = 5;

/// Set a timeout for reading client request headers. If a client does not transmit the entire
/// header within this time, the connection is closed.
const CLIENT_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

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

        let http_config = HttpConfig::new()
            .http1_half_close(false) // TODO(ja): Benchmark with `true`
            .http1_header_read_timeout(CLIENT_HEADER_TIMEOUT)
            .http1_writev(true)
            .http2_keep_alive_timeout(config.keepalive_timeout())
            .build();

        let addr_config = AddrIncomingConfig::new()
            .tcp_keepalive(Some(config.keepalive_timeout()).filter(|d| !d.is_zero()))
            .tcp_keepalive_interval(Some(config.keepalive_timeout()).filter(|d| !d.is_zero()))
            .tcp_keepalive_retries(Some(KEEPALIVE_RETRIES))
            .build();

        // TODO(ja): These are unsupported for now:
        //     .maxconn(config.max_connections())
        //     .maxconnrate(config.max_connection_rate())
        //     .backlog(config.max_pending_connections())

        let handle = Handle::new();
        let server = axum_server::bind(config.listen_addr())
            .http_config(http_config)
            .addr_incoming_config(addr_config)
            .handle(handle.clone());

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
            .layer(TraceLayer::new_for_http())
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
