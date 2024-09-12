use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::http::{header, HeaderName, HeaderValue};
use axum::ServiceExt;
use axum_server::accept::Accept;
use axum_server::Handle;
use hyper_util::rt::TokioTimer;
use relay_config::Config;
use relay_system::{Controller, Service, Shutdown};
use socket2::TcpKeepalive;
use tokio::net::{TcpSocket, TcpStream};
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tower_http::compression::predicate::SizeAbove;
use tower_http::compression::{CompressionLayer, DefaultPredicate, Predicate};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::constants;
use crate::middlewares::{
    self, CatchPanicLayer, NewSentryLayer, NormalizePath, RequestDecompressionLayer,
    SentryHttpLayer,
};
use crate::service::ServiceState;
use crate::statsd::RelayCounters;

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
    BindFailed(#[from] io::Error),

    /// TLS support was not compiled in.
    #[error("SSL is no longer supported by Relay, please use a proxy in front")]
    TlsNotSupported,
}

type App = NormalizePath<axum::Router>;

/// Build the axum application with all routes and middleware.
fn make_app(service: ServiceState) -> App {
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
        .layer(RequestDecompressionLayer::new())
        .layer(
            CompressionLayer::new()
                .compress_when(SizeAbove::new(COMPRESSION_MIN_SIZE).and(DefaultPredicate::new())),
        );

    let router = crate::endpoints::routes(service.config())
        .layer(middleware)
        .with_state(service);

    // Add middlewares that need to run _before_ routing, which need to wrap the router. This are
    // especially middlewares that modify the request path for the router:
    NormalizePath::new(router)
}

fn listen(config: &Config) -> Result<TcpListener, ServerError> {
    // Inform the user about a removed feature.
    if config.tls_listen_addr().is_some()
        || config.tls_identity_password().is_some()
        || config.tls_identity_path().is_some()
    {
        return Err(ServerError::TlsNotSupported);
    }

    let addr = config.listen_addr();
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }?;

    socket.bind(addr)?;
    Ok(socket.listen(config.tcp_listen_backlog())?.into_std()?)
}

fn build_keepalive(config: &Config) -> Option<TcpKeepalive> {
    let ka_timeout = config.keepalive_timeout();
    if ka_timeout.is_zero() {
        return None;
    }

    let mut ka = TcpKeepalive::new().with_time(ka_timeout);
    #[cfg(not(any(target_os = "openbsd", target_os = "redox", target_os = "solaris")))]
    {
        ka = ka.with_interval(ka_timeout);
    }

    #[cfg(not(any(
        target_os = "openbsd",
        target_os = "redox",
        target_os = "solaris",
        target_os = "windows"
    )))]
    {
        ka = ka.with_retries(KEEPALIVE_RETRIES);
    }

    Some(ka)
}

#[derive(Clone, Debug, Default)]
pub struct KeepAliveAcceptor(Option<TcpKeepalive>);

impl KeepAliveAcceptor {
    /// Create a new acceptor that sets TCP_NODELAY and keep-alive.
    pub fn new(config: &Config) -> Self {
        Self(build_keepalive(config))
    }
}

impl<S> Accept<TcpStream, S> for KeepAliveAcceptor {
    type Stream = TcpStream;
    type Service = S;
    type Future = std::future::Ready<io::Result<(Self::Stream, Self::Service)>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        if let Self(Some(ref tcp_keepalive)) = self {
            let sock_ref = socket2::SockRef::from(&stream);
            if let Err(e) = sock_ref.set_tcp_keepalive(tcp_keepalive) {
                relay_log::trace!("error trying to set TCP keepalive: {e}");
            }
        }

        if let Err(e) = stream.set_nodelay(true) {
            relay_log::trace!("failed to set TCP_NODELAY: {e}");
        }

        std::future::ready(Ok((stream, service)))
    }
}

fn serve(listener: TcpListener, app: App, config: Arc<Config>) -> JoinHandle<()> {
    let handle = Handle::new();

    let mut server = axum_server::from_tcp(listener)
        .acceptor(KeepAliveAcceptor::new(&config))
        .handle(handle.clone());

    server
        .http_builder()
        .http1()
        .timer(TokioTimer::new())
        .half_close(true)
        .header_read_timeout(CLIENT_HEADER_TIMEOUT)
        .writev(true);

    server
        .http_builder()
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_timeout(config.keepalive_timeout());

    let service = ServiceExt::<Request>::into_make_service_with_connect_info::<SocketAddr>(app);
    let _server_handle = tokio::spawn(server.serve(service));

    tokio::spawn(async move {
        let Shutdown { timeout } = Controller::shutdown_handle().notified().await;
        relay_log::info!("Shutting down HTTP server");

        match timeout {
            Some(timeout) => handle.graceful_shutdown(Some(timeout)),
            None => handle.shutdown(),
        }
    }) // TODO: return both
}

/// HTTP server service.
///
/// This is the main HTTP server of Relay which hosts all [services](ServiceState) and dispatches
/// incoming traffic to them. The server stops when a [`Shutdown`] is triggered.
pub struct HttpServer {
    config: Arc<Config>,
    service: ServiceState,
    listener: TcpListener,
}

impl HttpServer {
    pub fn new(config: Arc<Config>, service: ServiceState) -> Result<Self, ServerError> {
        let listener = listen(&config)?;

        Ok(Self {
            config,
            service,
            listener,
        })
    }
}

impl Service for HttpServer {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) -> JoinHandle<()> {
        let Self {
            config,
            service,
            listener,
        } = self;

        relay_log::info!("spawning http server");
        relay_log::info!("  listening on http://{}/", config.listen_addr());
        relay_statsd::metric!(counter(RelayCounters::ServerStarting) += 1);

        let app = make_app(service);
        serve(listener, app, config)
    }
}
