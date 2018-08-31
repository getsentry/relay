use std::fmt;
use std::sync::Arc;

use actix::prelude::*;
use actix_web::{server, App};
use failure::ResultExt;
use failure::{Backtrace, Context, Fail};
use listenfd::ListenFd;
use sentry_actix::SentryMiddleware;

use semaphore_common::Config;

use endpoints;
use middlewares::{AddCommonHeaders, ErrorHandlers, Metrics};

/// Common error type for the relay server.
#[derive(Debug)]
pub struct ServerError {
    inner: Context<ServerErrorKind>,
}

/// Indicates the type of failure of the server.
#[derive(Debug, Fail, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ServerErrorKind {
    /// Binding failed.
    #[fail(display = "bind to interface failed")]
    BindFailed,

    /// Listening on the HTTP socket failed.
    #[fail(display = "listening failed")]
    ListenFailed,

    /// A TLS error ocurred
    #[fail(display = "could not initialize the TLS server")]
    TlsInitFailed,

    /// TLS support was not compiled in
    #[fail(display = "compile with the `with_ssl` feature to enable SSL support.")]
    TlsNotSupported,
}

impl Fail for ServerError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl ServerError {
    /// Returns the error kind of the error.
    pub fn kind(&self) -> ServerErrorKind {
        *self.inner.get_context()
    }
}

impl From<ServerErrorKind> for ServerError {
    fn from(kind: ServerErrorKind) -> ServerError {
        ServerError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ServerErrorKind>> for ServerError {
    fn from(inner: Context<ServerErrorKind>) -> ServerError {
        ServerError { inner }
    }
}

/// Server state.
#[derive(Clone)]
pub struct ServiceState {
    config: Arc<Config>,
}

impl ServiceState {
    /// Returns an atomically counted reference to the config.
    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
    }
}

/// The actix app type for the relay web service.
pub type ServiceApp = App<ServiceState>;

fn make_app(state: ServiceState) -> ServiceApp {
    let mut app = App::with_state(state)
        .middleware(SentryMiddleware::new())
        .middleware(Metrics)
        .middleware(AddCommonHeaders)
        .middleware(ErrorHandlers);

    app = endpoints::forward::configure_app(app);
    app
}

fn dump_listen_infos<H: server::HttpHandler>(server: &server::HttpServer<H>) {
    info!("spawning http server");
    for (addr, scheme) in server.addrs_with_scheme() {
        info!("  listening on: {}://{}/", scheme, addr);
    }
}

/// Given a relay config spawns the server together with all actors and lets them run forever.
///
/// Effectively this boots the server.
pub fn run(config: Config) -> Result<(), ServerError> {
    let config = Arc::new(config);
    let sys = System::new("relay");

    let service_state = ServiceState {
        config: config.clone(),
    };

    let mut server = server::new(move || make_app(service_state.clone()));
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

    dump_listen_infos(&server);
    info!("spawning relay server");

    server.keep_alive(None).system_exit().start();
    let _ = sys.run();

    info!("relay shutdown complete");

    Ok(())
}
