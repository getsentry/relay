use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ConnectInfo;
use axum::http::{header, HeaderName, HeaderValue};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use relay_config::Config;
use relay_log::tower::{NewSentryLayer, SentryHttpLayer};
use relay_system::{Controller, Service};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Notify};
use tower::{Layer, ServiceBuilder};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::constants;
use crate::middlewares::{self, CatchPanicLayer, NormalizePath, RequestDecompressionLayer};
use crate::service::ServiceState;
use crate::statsd::RelayCounters;

/// Set a timeout for reading client request headers. If a client does not transmit the entire
/// header within this time, the connection is closed.
const CLIENT_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

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

fn listen(config: &Config) -> Result<TcpListener, ServerError> {
    // Inform the user about a removed feature.
    if config.tls_listen_addr().is_some()
        || config.tls_identity_password().is_some()
        || config.tls_identity_path().is_some()
    {
        return Err(ServerError::TlsNotSupported);
    }

    let listener = StdTcpListener::bind(config.listen_addr())?;
    listener.set_nonblocking(true)?;
    Ok(listener.try_into()?)
}

type App = NormalizePath<axum::Router>;

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
        .layer(RequestDecompressionLayer::new());

    let router = crate::endpoints::routes(service.config())
        .layer(middleware)
        .with_state(service);

    NormalizePath::new(router)
}

/// HTTP server service.
///
/// This is the main HTTP server of Relay which hosts all [services](ServiceState) and dispatches
/// incoming traffic to them. The server stops when a shutdown is triggered.
pub struct HttpServer {
    config: Arc<Config>,
    service: ServiceState,
    listener: TcpListener,
}

impl HttpServer {
    pub fn new(config: Arc<Config>, service: ServiceState) -> Result<Self, ServerError> {
        let listener = listen(&config)?;
        // let server = build_server(listener, &config);

        Ok(Self {
            config,
            service,
            listener,
        })
    }
}

impl Service for HttpServer {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            config,
            service,
            listener,
        } = self;

        relay_log::info!("spawning http server");
        relay_log::info!("  listening on http://{}/", config.listen_addr());
        relay_statsd::metric!(counter(RelayCounters::ServerStarting) += 1);

        let app = make_app(service);

        tokio::spawn(serve(listener, app, config.clone()));
    }
}

async fn serve(mut listener: TcpListener, app: App, config: Arc<Config>) {
    // Create channels to track shutdown and open connections.
    let hard_shutdown = monitor_hard_shutdown();
    let mut graceful_shutdown = Controller::shutdown_handle();
    let (connections_tx, connections_rx) = watch::channel(());

    // Create a connection builder to reuse across connections
    let builder = Arc::new(connection_builder(&config));

    loop {
        let (stream, addr) = tokio::select! {
            biased;
            result = accept(&mut listener) => result,
            _ = graceful_shutdown.notified() => break, // stop connecting on shutdown signal
        };

        // We don't need to call `poll_ready` because `Router` is always ready.
        let tower_service = axum::Extension(ConnectInfo(addr)).layer(app.clone());
        let hyper_service = TowerToHyperService::new(tower_service);

        let connection_rx = connections_rx.clone();
        let hard_shutdown = hard_shutdown.clone();
        let connection_builder = builder.clone();

        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = connection_builder.serve_connection_with_upgrades(stream, hyper_service) => (),
                _ = hard_shutdown.notified() => () // keep polling until we reach hard shutdown
            }

            drop(connection_rx);
        });
    }

    // Close the listener to stop accepting new connections.
    drop(listener);

    // Drop the last rx and wait for all open connections to close (or hard shutdown).
    drop(connections_rx);
    connections_tx.closed().await;
}

fn connection_builder(config: &Config) -> Builder<TokioExecutor> {
    let mut builder = Builder::new(TokioExecutor::new());

    builder
        .http1()
        .timer(TokioTimer::new())
        .half_close(true)
        .header_read_timeout(CLIENT_HEADER_TIMEOUT)
        .writev(true);

    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_timeout(config.keepalive_timeout());

    builder
}

fn monitor_hard_shutdown() -> Arc<Notify> {
    let shutdown_tx = Arc::new(Notify::new());
    let shutdown_rx = shutdown_tx.clone();

    // `finished()` is not thread-safe, so we need a dedicated task and a notify.
    tokio::spawn(async move {
        Controller::shutdown_handle().finished().await;
        shutdown_tx.notify_waiters();
    });

    shutdown_rx
}

async fn accept(listener: &mut TcpListener) -> (TokioIo<TcpStream>, SocketAddr) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => return (TokioIo::new(stream), addr),
            Err(e) => {
                // Connection errors can be ignored directly, continue
                // by accepting the next request.
                if is_connection_error(&e) {
                    continue;
                }

                // A possible scenario is that the process has hit the max open files allowed, and
                // so trying to accept a new connection will fail with `EMFILE`. In some cases, it's
                // preferable to just wait for some time, if the application will likely close some
                // files (or connections), and try to accept the connection again.
                tokio::time::sleep(Duration::from_millis(50)).await
            }
        }
    }
}

fn is_connection_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}
