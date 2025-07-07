use std::io;
use std::time::Duration;

use axum_server::accept::Accept;
use socket2::TcpKeepalive;
use tokio::net::TcpStream;

use crate::services::server::io::IdleTimeout;
use crate::statsd::RelayCounters;

#[derive(Clone, Debug, Default)]
pub struct RelayAcceptor {
    tcp_keepalive: Option<TcpKeepalive>,
    idle_timeout: Option<Duration>,
}

impl RelayAcceptor {
    /// Creates a new [`RelayAcceptor`] which only configures `TCP_NODELAY`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Configures the acceptor to enable TCP keep-alive.
    ///
    /// The `timeout` is used to configure the keep-alive time as well as interval.
    /// A zero duration timeout disables TCP keep-alive.
    ///
    /// `retries` configures the amount of keep-alive probes.
    pub fn tcp_keepalive(mut self, timeout: Duration, retries: u32) -> Self {
        if timeout.is_zero() {
            self.tcp_keepalive = None;
            return self;
        }

        let mut keepalive = socket2::TcpKeepalive::new().with_time(timeout);
        #[cfg(not(any(target_os = "openbsd", target_os = "redox", target_os = "solaris")))]
        {
            keepalive = keepalive.with_interval(timeout);
        }

        let _retries = retries;
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "cygwin",
            target_os = "windows",
        ))]
        {
            keepalive = keepalive.with_retries(_retries);
        }
        self.tcp_keepalive = Some(keepalive);

        self
    }

    /// Configures an idle timeout for the connection.
    ///
    /// Whenever there is no activity on a connection for the specified timeout,
    /// the connection is closed.
    ///
    /// Note: This limits the total idle time of a duration and unlike read and write timeouts
    /// also limits the time a connection is kept alive without requests.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }
}

impl<S> Accept<TcpStream, S> for RelayAcceptor {
    type Stream = IdleTimeout<TcpStream>;
    type Service = S;
    type Future = std::future::Ready<io::Result<(Self::Stream, Self::Service)>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let mut keepalive = "ok";
        let mut nodelay = "ok";

        if let Some(tcp_keepalive) = &self.tcp_keepalive {
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

        let stream = IdleTimeout::new(stream, self.idle_timeout);
        std::future::ready(Ok((stream, service)))
    }
}
