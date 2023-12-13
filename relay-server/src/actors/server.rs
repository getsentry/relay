use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::http::{header, HeaderName, HeaderValue};
use axum::ServiceExt;
use axum_server::Handle;
use relay_config::Config;
use relay_log::tower::{NewSentryLayer, SentryHttpLayer};
use relay_system::{Controller, Service, Shutdown};
use tower::ServiceBuilder;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::constants;
use crate::middlewares::{self, CatchPanicLayer, NormalizePath, RequestDecompressionLayer};
use crate::service::ServiceState;
use crate::statsd::RelayCounters;

/// Indicates the type of failure of the server.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Binding failed.
    #[error("bind to interface failed")]
    BindFailed(#[source] io::Error),

    /// TLS support was not compiled in.
    #[error("SSL is no longer supported by Relay, please use a proxy in front")]
    TlsNotSupported,
}

/// HTTP server service.
///
/// This is the main HTTP server of Relay which hosts all [services](ServiceState) and dispatches
/// incoming traffic to them. The server stops when a [`Shutdown`] is triggered.
pub struct HttpServer {
    config: Arc<Config>,
    service: ServiceState,
    server: axum_server::Server,
}

/// Set a timeout for reading client request headers. If a client does not transmit the entire
/// header within this time, the connection is closed.
const CLIENT_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

impl HttpServer {
    pub fn new(config: Arc<Config>, service: ServiceState) -> Result<Self, ServerError> {
        // Inform the user about a removed feature.
        if config.tls_listen_addr().is_some()
            || config.tls_identity_password().is_some()
            || config.tls_identity_path().is_some()
        {
            return Err(ServerError::TlsNotSupported);
        }

        let listener = TcpListener::bind(config.listen_addr()).map_err(ServerError::BindFailed)?;

        let mut server = axum_server::from_tcp(listener);
        server
            .http_builder()
            .http1()
            .half_close(true)
            .header_read_timeout(CLIENT_HEADER_TIMEOUT)
            .writev(true);
        server
            .http_builder()
            .http2()
            .keep_alive_timeout(config.keepalive_timeout());

        Ok(Self {
            config,
            service,
            server,
        })
    }
}

impl Service for HttpServer {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            config,
            service,
            server,
        } = self;

        // Build the router middleware into a single service which runs _after_ routing. Service
        // builder order defines layers added first will be called first. This means:
        //  - Requests go from top to bottom
        //  - Responses go from bottom to top
        let middleware = ServiceBuilder::new()
            .layer(axum::middleware::from_fn(middlewares::metrics))
            .layer(CatchPanicLayer::custom(middlewares::handle_panic))
            .layer(SetResponseHeaderLayer::overriding(
                header::SERVER,
                HeaderValue::from_static(constants::SERVER),
            ))
            .layer(SetResponseHeaderLayer::overriding(
                HeaderName::from_static("cross-origin-resource-policy"),
                HeaderValue::from_static("cross-origin"),
            ))
            .layer(NewSentryLayer::new_from_top())
            .layer(SentryHttpLayer::with_transaction())
            .layer(middlewares::trace_http_layer())
            .map_request(middlewares::remove_empty_encoding)
            .layer(RequestDecompressionLayer::new());

        let router = crate::endpoints::routes(service.config())
            .layer(middleware)
            .with_state(service);

        // Bundle middlewares that need to run _before_ routing, which need to wrap the router.
        // ConnectInfo is special as it needs to last.
        let app = NormalizePath::new(router);
        let app = ServiceExt::<Request>::into_make_service_with_connect_info::<SocketAddr>(app);

        relay_log::info!("spawning http server");
        relay_log::info!("  listening on http://{}/", config.listen_addr());
        relay_statsd::metric!(counter(RelayCounters::ServerStarting) += 1);

        let handle = Handle::new();
        tokio::spawn(server.handle(handle.clone()).serve(app));

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
