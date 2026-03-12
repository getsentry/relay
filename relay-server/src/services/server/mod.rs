use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use axum::ServiceExt;
use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, header};
use axum_server::Handle;
use hyper_util::rt::TokioTimer;
use relay_config::Config;
use relay_system::{Controller, Service, Shutdown};
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use tokio::net::TcpSocket;
use tower::ServiceBuilder;
use tower_http::compression::predicate::SizeAbove;
use tower_http::compression::{CompressionLayer, DefaultPredicate, Predicate};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::constants;
use crate::middlewares::{self, CatchPanicLayer, NormalizePath, RequestDecompressionLayer};
use crate::service::ServiceState;
use crate::statsd::{RelayCounters, RelayGauges};

mod acceptor;
mod concurrency_limit;
mod io;

/// Set the number of keep-alive retransmissions to be carried out before declaring that remote end
/// is not available.
const KEEPALIVE_RETRIES: u32 = 5;

/// Set a timeout for reading client request headers. If a client does not transmit the entire
/// header within this time, the connection is closed.
const CLIENT_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// Only compress responses above this configured size, in bytes.
///
/// Small responses don't benefit from compression,
/// additionally the envelope endpoint which returns the event id
/// should not be compressed due to a bug in the Unity SDK.
const COMPRESSION_MIN_SIZE: u16 = 128;

/// Indicates the type of failure of the server.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Binding failed.
    #[error("bind to interface failed")]
    BindFailed(#[from] std::io::Error),

    /// TLS support was not compiled in.
    #[error("SSL is no longer supported by Relay, please use a proxy in front")]
    TlsNotSupported,
}

type App = NormalizePath<axum::Router>;

/// Build the axum application with all routes and middleware.
fn make_app(service: ServiceState, f: impl FnOnce(&Config) -> axum::Router<ServiceState>) -> App {
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
        .layer(SentryHttpLayer::new().enable_transaction())
        .layer(middlewares::trace_http_layer())
        .map_request(middlewares::remove_empty_encoding)
        .layer(RequestDecompressionLayer::new())
        .layer(
            CompressionLayer::new()
                .compress_when(SizeAbove::new(COMPRESSION_MIN_SIZE).and(DefaultPredicate::new())),
        );

    let router = f(service.config()).layer(middleware).with_state(service);

    // Add middlewares that need to run _before_ routing, which need to wrap the router. This are
    // especially middlewares that modify the request path for the router:
    NormalizePath::new(router)
}

fn listen(addr: SocketAddr, config: &Config) -> Result<TcpListener, ServerError> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }?;

    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    socket.set_reuseport(true)?;
    socket.bind(addr)?;
    Ok(socket.listen(config.tcp_listen_backlog())?.into_std()?)
}

async fn serve(listener: TcpListener, app: App, config: &Config) -> std::io::Result<()> {
    let handle = Handle::new();

    let acceptor = self::acceptor::RelayAcceptor::new()
        .tcp_keepalive(config.keepalive_timeout(), KEEPALIVE_RETRIES)
        .idle_timeout(config.idle_timeout());

    let mut server = axum_server::from_tcp(listener)?
        .acceptor(acceptor)
        .handle(handle.clone());

    server
        .http_builder()
        .http1()
        .timer(TokioTimer::new())
        .half_close(true)
        .keep_alive(true)
        .header_read_timeout(CLIENT_HEADER_TIMEOUT)
        .writev(true);

    server
        .http_builder()
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_timeout(config.keepalive_timeout());

    let service = ServiceBuilder::new()
        .layer(concurrency_limit::ConcurrencyLimitLayer::new(
            config.max_connections(),
        ))
        .service(ServiceExt::<Request>::into_make_service_with_connect_info::<SocketAddr>(app));

    relay_system::spawn!(emit_active_connections_metric(
        config.metrics_periodic_interval(),
        handle.clone(),
    ));

    relay_system::spawn!(async move {
        let Shutdown { timeout } = Controller::shutdown_handle().notified().await;
        relay_log::info!("Shutting down HTTP server");

        match timeout {
            Some(timeout) => handle.graceful_shutdown(Some(timeout)),
            None => handle.shutdown(),
        }
    });

    server.serve(service).await
}

/// HTTP server service.
///
/// This is the main HTTP server of Relay which hosts all [services](ServiceState) and dispatches
/// incoming traffic to them. The server stops when a [`Shutdown`] is triggered.
pub struct HttpServer {
    config: Arc<Config>,
    service: ServiceState,
    listener: TcpListener,
    internal_listener: Option<TcpListener>,
}

impl HttpServer {
    pub fn new(config: Arc<Config>, service: ServiceState) -> Result<Self, ServerError> {
        // Inform the user about a removed feature.
        if config.tls_listen_addr().is_some()
            || config.tls_identity_password().is_some()
            || config.tls_identity_path().is_some()
        {
            return Err(ServerError::TlsNotSupported);
        }

        let listener = listen(config.listen_addr(), &config)?;
        let internal_listener = match config.listen_addr_internal() {
            Some(addr) => Some(listen(addr, &config)?),
            None => None,
        };

        Ok(Self {
            config,
            service,
            listener,
            internal_listener,
        })
    }
}

impl Service for HttpServer {
    type Interface = ();

    async fn run(self, _rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            config,
            service,
            listener,
            internal_listener,
        } = self;

        let listen_addr = config.listen_addr();

        relay_log::info!("spawning http server");
        relay_log::info!("  listening on http://{listen_addr}/");
        if let Some(internal_addr) = config.listen_addr_internal() {
            relay_log::info!("  listening on http://{internal_addr}/ [internal]");
        }
        relay_statsd::metric!(counter(RelayCounters::ServerStarting) += 1);

        if let Some(internal_listener) = internal_listener {
            let public = make_app(service.clone(), crate::endpoints::public_routes);
            let internal = make_app(service, crate::endpoints::internal_routes);

            tokio::try_join!(
                serve(listener, public, &config),
                serve(internal_listener, internal, &config),
            )
            .map(drop)
        } else {
            let app = make_app(service, crate::endpoints::all_routes);
            serve(listener, app, &config).await
        }
        .expect("axum listener to not fail")
    }
}

async fn emit_active_connections_metric(interval: Option<Duration>, handle: Handle<SocketAddr>) {
    let Some(mut ticker) = interval.map(tokio::time::interval) else {
        return;
    };

    let addr = handle.listening().await.map(|addr| addr.to_string());

    loop {
        ticker.tick().await;
        relay_statsd::metric!(
            gauge(RelayGauges::ServerActiveConnections) = handle.connection_count() as u64,
            addr = addr.as_deref().unwrap_or("unknown"),
        );
    }
}
