use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, io};

use relay_common::{Dsn, Scheme};
use url::Url;

/// Indicates failures in the upstream error api.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// Raised if the DNS lookup for an upstream host failed.
    #[error("dns lookup failed")]
    LookupFailed(#[source] io::Error),
    /// Raised if the DNS lookup succeeded but an empty result was
    /// returned.
    #[error("dns lookup returned no results")]
    EmptyLookupResult,
}

/// Raised if a URL cannot be parsed into an upstream descriptor.
#[derive(Debug, Eq, Hash, PartialEq, thiserror::Error)]
pub enum UpstreamParseError {
    /// Raised if an upstream could not be parsed as URL.
    #[error("invalid upstream URL: bad URL format")]
    BadUrl,
    /// Raised if a path was added to a URL.
    #[error("invalid upstream URL: non root URL given")]
    NonOriginUrl,
    /// Raised if an unknown or unsupported scheme is encountered.
    #[error("invalid upstream URL: unknown or unsupported URL scheme")]
    UnknownScheme,
    /// Raised if no host was provided.
    #[error("invalid upstream URL: no host")]
    NoHost,
}

/// The upstream target is a type that holds all the information
/// to uniquely identify an upstream target.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct UpstreamDescriptor {
    host: Arc<str>,
    port: u16,
    scheme: Scheme,
}

impl UpstreamDescriptor {
    /// Manually constructs an upstream descriptor.
    pub fn new<T>(host: T, port: u16, scheme: Scheme) -> Self
    where
        T: Into<Arc<str>>,
    {
        UpstreamDescriptor {
            host: host.into(),
            port,
            scheme,
        }
    }

    /// Given a DSN this returns an upstream descriptor that
    /// describes it.
    pub fn from_dsn(dsn: &Dsn) -> Self {
        Self::new(dsn.host(), dsn.port(), dsn.scheme())
    }

    /// Returns the host as a string.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the upstream port
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns a URL relative to the upstream.
    pub fn get_url(&self, path: &str) -> Url {
        format!("{self}{}", path.trim_start_matches(&['/'][..]))
            .parse()
            .unwrap()
    }

    /// Returns the socket address of the upstream.
    ///
    /// This might perform a DSN lookup and could fail.  Callers are
    /// encouraged this call this regularly as DNS might be used for
    /// load balancing purposes and results might expire.
    pub fn socket_addr(self) -> Result<SocketAddr, UpstreamError> {
        (self.host(), self.port())
            .to_socket_addrs()
            .map_err(UpstreamError::LookupFailed)?
            .next()
            .ok_or(UpstreamError::EmptyLookupResult)
    }

    /// Returns the upstream's connection scheme.
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }
}

impl Default for UpstreamDescriptor {
    fn default() -> Self {
        Self::new("sentry.io", 443, Scheme::Https)
    }
}

impl fmt::Display for UpstreamDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", &self.scheme, &self.host)?;
        if self.port() != self.scheme.default_port() {
            write!(f, ":{}", self.port())?;
        }
        write!(f, "/")
    }
}

impl FromStr for UpstreamDescriptor {
    type Err = UpstreamParseError;

    fn from_str(s: &str) -> Result<Self, UpstreamParseError> {
        let url = Url::parse(s).map_err(|_| UpstreamParseError::BadUrl)?;
        if url.path() != "/" || !(url.query().is_none() || url.query() == Some("")) {
            return Err(UpstreamParseError::NonOriginUrl);
        }

        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            _ => return Err(UpstreamParseError::UnknownScheme),
        };

        Ok(UpstreamDescriptor {
            host: match url.host_str() {
                Some(host) => host.into(),
                None => return Err(UpstreamParseError::NoHost),
            },
            port: url.port().unwrap_or_else(|| scheme.default_port()),
            scheme,
        })
    }
}

relay_common::impl_str_serde!(UpstreamDescriptor, "a sentry upstream URL");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_parsing() {
        let desc: UpstreamDescriptor = "https://sentry.io/".parse().unwrap();
        assert_eq!(desc.host(), "sentry.io");
        assert_eq!(desc.port(), 443);
        assert_eq!(desc.scheme(), Scheme::Https);
    }

    #[test]
    fn test_from_dsn() {
        let dsn: Dsn = "https://username:password@domain:8888/42".parse().unwrap();
        let desc = UpstreamDescriptor::from_dsn(&dsn);
        assert_eq!(desc.host(), "domain");
        assert_eq!(desc.port(), 8888);
        assert_eq!(desc.scheme(), Scheme::Https);
    }
}
