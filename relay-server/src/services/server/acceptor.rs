use std::io;

use axum_server::accept::Accept;
use relay_config::Config;
use socket2::TcpKeepalive;
use tokio::net::TcpStream;

use crate::services::server::io::IdleTimeout;
use crate::statsd::RelayCounters;

#[derive(Clone, Debug, Default)]
pub struct RelayAcceptor(Option<TcpKeepalive>);

impl RelayAcceptor {
    /// Create a new acceptor that sets `TCP_NODELAY` and keep-alive.
    pub fn new(config: &Config, keepalive_retries: u32) -> Self {
        Self(build_keepalive(config, keepalive_retries))
    }
}

impl<S> Accept<TcpStream, S> for RelayAcceptor {
    type Stream = std::pin::Pin<Box<IdleTimeout<TcpStream>>>;
    type Service = S;
    type Future = std::future::Ready<io::Result<(Self::Stream, Self::Service)>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let mut keepalive = "ok";
        let mut nodelay = "ok";

        if let Self(Some(ref tcp_keepalive)) = self {
            let sock_ref = socket2::SockRef::from(&stream);
            if let Err(e) = sock_ref.set_tcp_keepalive(tcp_keepalive) {
                relay_log::trace!("error trying to set TCP keepalive: {e}");
                keepalive = "error";
            }
        }

        if let Err(e) = stream.set_nodelay(true) {
            relay_log::trace!("failed to set TCP_NODELAY: {e}");
            nodelay = "error";
        }

        relay_statsd::metric!(
            counter(RelayCounters::ServerSocketAccept) += 1,
            keepalive = keepalive,
            nodelay = nodelay
        );

        let stream = Box::pin(IdleTimeout::new(stream, std::time::Duration::from_secs(5)));
        std::future::ready(Ok((stream, service)))
    }
}

fn build_keepalive(config: &Config, keepalive_retries: u32) -> Option<TcpKeepalive> {
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
        ka = ka.with_retries(keepalive_retries);
    }

    Some(ka)
}
