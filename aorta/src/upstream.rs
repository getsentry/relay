use std::net::{SocketAddr, ToSocketAddrs};
use std::io;
use std::fmt;
use std::str::FromStr;
use std::borrow::Cow;

use url::Url;
use serde::ser::{Serialize, Serializer};
use serde::de::{self, Deserialize, Deserializer, Visitor};

use smith_common::{Dsn, Scheme};

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
    port: Option<u16>,
    scheme: Scheme,
}

impl<'a> UpstreamDescriptor<'a> {
    /// Manually constructs an upstream descriptor.
    pub fn new(host: &'a str, port: u16, scheme: Scheme) -> UpstreamDescriptor<'a> {
        UpstreamDescriptor {
            host: Cow::Borrowed(host),
            port: Some(port),
            scheme: scheme,
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
        self.port.unwrap_or_else(|| self.scheme().default_port())
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

impl<'a> fmt::Display for UpstreamDescriptor<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
        if url.path() != "/" || !(url.query() == None || url.query() == Some("")) {
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
            port: url.port(),
            scheme: scheme,
        })
    }
}

impl Serialize for UpstreamDescriptor<'static> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for UpstreamDescriptor<'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = UpstreamDescriptor<'static>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sentry upstream url")
            }

            fn visit_str<E>(self, value: &str) -> Result<UpstreamDescriptor<'static>, E>
            where
                E: de::Error,
            {
                value
                    .parse()
                    .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(value), &self))
            }
        }

        deserializer.deserialize_str(V)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use smith_common::Dsn;

    #[test]
    fn test_basic_parsing() {
        let desc: UpstreamDescriptor = "https://ingest.sentry.io/".parse().unwrap();
        assert_eq!(desc.host(), "ingest.sentry.io");
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
