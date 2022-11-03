use std::borrow::Cow;
use std::fmt;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;

use failure::Fail;
use url::Url;

use relay_common::{Dsn, Scheme};

/// Indicates failures in the upstream error api.
#[derive(Fail, Debug)]
pub enum UpstreamError {
    /// Raised if the DNS lookup for an upstream host failed.
    #[fail(display = "dns lookup failed")]
    LookupFailed(#[cause] io::Error),
    /// Raised if the DNS lookup succeeded but an empty result was
    /// returned.
    #[fail(display = "dns lookup returned no results")]
    EmptyLookupResult,
}

/// Raised if a URL cannot be parsed into an upstream descriptor.
#[derive(Fail, Debug, PartialEq, Eq, Hash)]
pub enum UpstreamParseError {
    /// Raised if an upstream could not be parsed as URL.
    #[fail(display = "invalid upstream URL: bad URL format")]
    BadUrl,
    /// Raised if a path was added to a URL.
    #[fail(display = "invalid upstream URL: non root URL given")]
    NonOriginUrl,
    /// Raised if an unknown or unsupported scheme is encountered.
    #[fail(display = "invalid upstream URL: unknown or unsupported URL scheme")]
    UnknownScheme,
    /// Raised if no host was provided.
    #[fail(display = "invalid upstream URL: no host")]
    NoHost,
}

/// The upstream target is a type that holds all the information
/// to uniquely identify an upstream target.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct UpstreamDescriptor<'a> {
    host: Cow<'a, str>,
    port: u16,
    scheme: Scheme,
}

impl<'a> UpstreamDescriptor<'a> {
    /// Manually constructs an upstream descriptor.
    pub fn new(host: &'a str, port: u16, scheme: Scheme) -> UpstreamDescriptor<'a> {
        UpstreamDescriptor {
            host: Cow::Borrowed(host),
            port,
            scheme,
        }
    }

    /// Given a DSN this returns an upstream descriptor that
    /// describes it.
    pub fn from_dsn(dsn: &'a Dsn) -> UpstreamDescriptor<'a> {
        UpstreamDescriptor {
            host: Cow::Borrowed(dsn.host()),
            port: dsn.port(),
            scheme: dsn.scheme(),
        }
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
        format!("{}{}", self, path.trim_start_matches(&['/'][..]))
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

    /// Returns a version of the upstream descriptor that is static.
    pub fn into_owned(self) -> UpstreamDescriptor<'static> {
        UpstreamDescriptor {
            host: Cow::Owned(self.host.into_owned()),
            port: self.port,
            scheme: self.scheme,
        }
    }
}

impl Default for UpstreamDescriptor<'static> {
    fn default() -> UpstreamDescriptor<'static> {
        UpstreamDescriptor {
            host: Cow::Borrowed("sentry.io"),
            port: 443,
            scheme: Scheme::Https,
        }
    }
}

impl<'a> fmt::Display for UpstreamDescriptor<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", &self.scheme, &self.host)?;
        if self.port() != self.scheme.default_port() {
            write!(f, ":{}", self.port())?;
        }
        write!(f, "/")
    }
}

impl FromStr for UpstreamDescriptor<'static> {
    type Err = UpstreamParseError;

    fn from_str(s: &str) -> Result<UpstreamDescriptor<'static>, UpstreamParseError> {
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
                Some(host) => Cow::Owned(host.to_string()),
                None => return Err(UpstreamParseError::NoHost),
            },
            port: url.port().unwrap_or_else(|| scheme.default_port()),
            scheme,
        })
    }
}

relay_common::impl_str_serde!(UpstreamDescriptor<'static>, "a sentry upstream URL");

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic_parsing() {
        let desc: UpstreamDescriptor<'_> = "https://sentry.io/".parse().unwrap();
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
