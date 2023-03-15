use std::sync::Arc;

use axum::Router;
use axum_server::{Handle, Server};
use relay_config::Config;
use relay_system::{Addr, Controller, Service, Shutdown};

// use crate::middlewares::{
//     AddCommonHeaders, ErrorHandlers, Metrics, ReadRequestMiddleware, SentryMiddleware,
// };
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

// fn dump_listen_infos<H, F>(server: &server::HttpServer<H, F>)
// where
//     H: server::IntoHttpHandler + 'static,
//     F: Fn() -> H + Send + Clone + 'static,
// {
//     relay_log::info!("spawning http server");
//     for (addr, scheme) in server.addrs_with_scheme() {
//         relay_log::info!("  listening on: {}://{}/", scheme, addr);
//     }
// }

// TODO(ja): SSL support
// #[cfg(feature = "ssl")]
// fn listen_ssl<H, F>(
//     mut server: server::HttpServer<H, F>,
//     config: &Config,
// ) -> Result<server::HttpServer<H, F>>
// where
//     H: server::IntoHttpHandler + 'static,
//     F: Fn() -> H + Send + Clone + 'static,
// {
//     if let (Some(addr), Some(path), Some(password)) = (
//         config.tls_listen_addr(),
//         config.tls_identity_path(),
//         config.tls_identity_password(),
//     ) {
//         use std::fs::File;
//         use std::io::Read;

//         use native_tls::{Identity, TlsAcceptor};

//         let mut file = File::open(path).unwrap();
//         let mut data = vec![];
//         file.read_to_end(&mut data).unwrap();
//         let identity =
//             Identity::from_pkcs12(&data, password).context(ServerError::TlsInitFailed)?;

//         let acceptor = TlsAcceptor::builder(identity)
//             .build()
//             .context(ServerError::TlsInitFailed)?;

//         server = server
//             .bind_tls(addr, acceptor)
//             .context(ServerError::BindFailed)?;
//     }

//     Ok(server)
// }

/* TODO(ja): SSL support somehow
#[cfg(feature = "ssl")]
mod acceptor {
    use std::future::{Future, Ready};
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::task::Poll;

    use anyhow::{Context, Result};
    use axum_server::accept::Accept;
    use hyper::server::conn::AddrStream;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio_native_tls::native_tls::{self, Identity};
    use tokio_native_tls::{TlsAcceptor, TlsStream};

    use super::*;

    #[derive(Debug)]
    enum NativeTlsStream<S> {
        Tls(TlsStream<S>),
        Tcp(S),
    }

    impl<S> AsyncRead for NativeTlsStream<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match *self {
                Self::Tls(ref mut inner) => Pin::new(inner).poll_read(cx, buf),
                Self::Tcp(ref mut inner) => Pin::new(inner).poll_read(cx, buf),
            }
        }
    }

    impl<S> AsyncWrite for NativeTlsStream<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            match *self {
                Self::Tls(ref mut inner) => Pin::new(inner).poll_write(cx, buf),
                Self::Tcp(ref mut inner) => Pin::new(inner).poll_write(cx, buf),
            }
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            match *self {
                Self::Tls(ref mut inner) => Pin::new(inner).poll_flush(cx),
                Self::Tcp(ref mut inner) => Pin::new(inner).poll_flush(cx),
            }
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            match *self {
                Self::Tls(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
                Self::Tcp(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
            }
        }
    }

    enum NativeTlsFuture<I, S> {
        Tls(Pin<Box<dyn Future<Output = io::Result<(I, S)>> + Send + 'static>>),
        Tcp(Ready<io::Result<(I, S)>>),
    }

    impl<I, S> NativeTlsFuture<I, S> {
        pub fn tls<F>(f: F) -> Self
        where
            F: Future<Output = io::Result<(I, S)>> + Send + 'static,
        {
            Self::Tls(Box::pin(f))
        }

        pub fn tcp(stream: I, service: S) -> Self {
            Self::Tcp(std::future::ready(Ok((stream, service))))
        }
    }

    impl<I, S> Future for NativeTlsFuture<I, S> {
        type Output = io::Result<(I, S)>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
            match *self {
                Self::Tls(ref mut inner) => Pin::new(inner).poll(cx),
                Self::Tcp(ref mut inner) => Pin::new(inner).poll(cx),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct NativeTlsAcceptor {
        inner: Option<(SocketAddr, Arc<TlsAcceptor>)>,
    }

    impl NativeTlsAcceptor {
        pub fn tls(tls_addr: SocketAddr, acceptor: TlsAcceptor) -> Self {
            Self {
                inner: Some((tls_addr, Arc::new(acceptor))),
            }
        }

        pub fn tcp() -> Self {
            Self { inner: None }
        }
    }

    impl<S> Accept<AddrStream, S> for NativeTlsAcceptor
    where
        S: Send + 'static,
    {
        type Stream = NativeTlsStream<AddrStream>;
        type Service = S;
        type Future = NativeTlsFuture<Self::Stream, Self::Service>;

        fn accept(&self, stream: AddrStream, service: S) -> Self::Future {
            if let Some((tls_addr, ref acceptor)) = self.inner {
                if stream.local_addr() == tls_addr {
                    let acceptor = Arc::clone(acceptor);

                    return NativeTlsFuture::tls(async move {
                        let result = acceptor.accept(stream).await;
                        let tls_stream =
                            result.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                        Ok((NativeTlsStream::Tls(tls_stream), service))
                    });
                }
            }

            NativeTlsFuture::tcp(NativeTlsStream::Tcp(stream), service)
        }
    }

    pub fn create(config: &Config) -> Result<NativeTlsAcceptor> {
        match (
            config.tls_listen_addr(),
            config.tls_identity_path(),
            config.tls_identity_password(),
        ) {
            (Some(tls_addr), Some(path), Some(password)) => {
                let data = std::fs::read(path)?;
                let identity = Identity::from_pkcs12(&data, password)?;
                let native_acceptor = native_tls::TlsAcceptor::builder(identity).build()?;
                let acceptor = TlsAcceptor::from(native_acceptor);
                Ok(NativeTlsAcceptor::tls(tls_addr, acceptor))
            }
            _ => Ok(NativeTlsAcceptor::tcp()),
        }
    }

    pub fn listen_addrs(config: &Config) -> Result<[SocketAddr]> {
        Ok(match config.tls_listen_addr() {
            Some(addr) => [addr, config.listen_addr()],
            None => [config.listen_addr()],
        })
    }
}

#[cfg(not(feature = "ssl"))]
mod acceptor {
    use axum_server::accept::DefaultAcceptor;

    pub fn create(config: &Config) -> Result<DefaultAcceptor> {
        if config.tls_identity_path().is_some() || config.tls_identity_password().is_some() {
            Err(ServerError::TlsNotSupported.into())
        } else {
            Ok(DefaultAcceptor::new())
        }
    }

    pub fn listen_addrs(config: &Config) -> Result<&'static [SocketAddr]> {
        if config.tls_listen_addr().is_some() {
            Err(ServerError::TlsNotSupported.into())
        } else {
            Ok(&[config.listen_addr()])
        }
    }
}
*/

/// HTTP server service.
///
/// This is the main HTTP server of Relay which hosts all [services](ServiceState) and dispatches
/// incoming traffic to them. The server stops when a [`Shutdown`] is triggered.
pub struct HttpServer {
    app: Router,
    server: Server,
    handle: Handle,
}

impl HttpServer {
    pub fn start(config: Arc<Config>, service: ServiceState) -> anyhow::Result<Addr<()>> {
        relay_statsd::metric!(counter(RelayCounters::ServerStarting) += 1);

        // TODO(ja): This does not do what we want. It just binds to the first address.
        // let socket_addrs = acceptor::listen_addrs(&config)?;
        // let listener = TcpListener::bind(socket_addrs).context(ServerError::BindFailed)?;

        let app = crate::endpoints::routes().with_state(service);

        // App::with_state(state)
        //     .middleware(SentryMiddleware::new())
        //     .middleware(Metrics)
        //     .middleware(AddCommonHeaders)
        //     .middleware(ErrorHandlers)
        //     .middleware(ReadRequestMiddleware)
        //     .configure(crate::endpoints::routes)

        let handle = Handle::new();
        let server = axum_server::bind(config.listen_addr())
            // todo http_config
            // todo addr_incoming_config
            // .acceptor(acceptor::create(&config)?)
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

        // dump_listen_infos(&server);
        let service = Self {
            app,
            server,
            handle,
        };

        Ok(service.start())
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
