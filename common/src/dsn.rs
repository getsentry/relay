use std::fmt;
use std::str::FromStr;

use serde::ser::{Serialize, Serializer};
use serde::de::{self, Deserialize, Deserializer, Visitor};
use url::Url;

/// Represents a dsn url parsing error.
#[derive(Debug, Fail)]
pub enum DsnParseError {
    #[fail(display = "no valid url provided")] InvalidUrl,
    #[fail(display = "username is empty")] NoUsername,
    #[fail(display = "empty path")] NoProjectId,
    #[fail(display = "no valid scheme")] InvalidScheme,
}

/// Represents the scheme of an url http/https.
///
/// This holds schemes that are supported by sentry and agents.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Scheme {
    Http,
    Https,
}

impl Scheme {
    /// Returns the default port for this scheme.
    pub fn default_port(&self) -> u16 {
        match *self {
            Scheme::Http => 80,
            Scheme::Https => 443,
        }
    }
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Scheme::Https => "https",
                Scheme::Http => "http",
            }
        )
    }
}

/// Represents a Sentry dsn.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Dsn {
    scheme: Scheme,
    public_key: String,
    secret_key: Option<String>,
    host: String,
    port: Option<u16>,
    project_id: String,
}

impl Dsn {
    /// Returns the scheme
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Returns the public_key
    pub fn public_key(&self) -> &str {
        &self.public_key
    }

    /// Returns secret_key
    pub fn secret_key(&self) -> Option<&str> {
        self.secret_key.as_ref().map(|x| x.as_str())
    }

    /// Returns the host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the port
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Returns the project_id
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
}

impl fmt::Display for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.public_key)?;
        if let Some(ref secret_key) = self.secret_key {
            write!(f, ":{}", secret_key)?;
        }
        write!(f, "@{}", self.host)?;
        if let Some(ref port) = self.port {
            write!(f, ":{}", port)?;
        }
        write!(f, "/{}", self.project_id)?;
        Ok(())
    }
}

impl FromStr for Dsn {
    type Err = DsnParseError;

    fn from_str(s: &str) -> Result<Dsn, DsnParseError> {
        let url = Url::parse(s).map_err(|_| DsnParseError::InvalidUrl)?;

        if url.path() == "/" {
            return Err(DsnParseError::NoProjectId);
        }

        let path_segments = url.path_segments()
            .ok_or_else(|| DsnParseError::NoProjectId)?;
        if path_segments.count() > 1 {
            return Err(DsnParseError::InvalidUrl);
        }

        let public_key = match url.username() {
            "" => return Err(DsnParseError::NoUsername),
            username => username.to_string(),
        };

        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            _ => return Err(DsnParseError::InvalidScheme),
        };

        let secret_key = url.password().map(|s| s.into());
        let port = url.port().map(|s| s.into());
        let host = match url.host_str() {
            Some(host) => host.into(),
            None => return Err(DsnParseError::InvalidUrl),
        };
        let project_id = url.path().trim_matches('/').into();

        Ok(Dsn {
            scheme,
            public_key,
            secret_key,
            port,
            host,
            project_id,
        })
    }
}

impl Serialize for Dsn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Dsn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Dsn;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sentry dsn")
            }

            fn visit_str<E>(self, value: &str) -> Result<Dsn, E>
            where
                E: de::Error,
            {
                match value.parse() {
                    Ok(value) => Ok(value),
                    Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_str(V)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use serde_json;

    #[test]
    fn test_dsn_serialize_deserialize() {
        let dsn = Dsn::from_str("https://username@domain/path").unwrap();
        let serialized = serde_json::to_string(&dsn).unwrap();
        assert_eq!(serialized, "\"https://username@domain/path\"");
        let deserialized: Dsn = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.to_string(), "https://username@domain/path");
    }

    #[test]
    fn test_dsn_parsing() {
        let url = "https://username:password@domain:8888/path";
        let dsn = url.parse::<Dsn>().unwrap();
        assert_eq!(dsn.scheme(), Scheme::Https);
        assert_eq!(dsn.public_key(), "username");
        assert_eq!(dsn.secret_key(), Some("password"));
        assert_eq!(dsn.host(), "domain");
        assert_eq!(dsn.port(), Some(8888));
        assert_eq!(dsn.project_id(), "path");
        assert_eq!(url, dsn.to_string());
    }

    #[test]
    fn test_dsn_no_port() {
        let url = "https://username@domain/path";
        let dsn = Dsn::from_str(url).unwrap();
        assert_eq!(url, dsn.to_string());
    }

    #[test]
    fn test_dsn_no_password() {
        let url = "https://username@domain:8888/path";
        let dsn = Dsn::from_str(url).unwrap();
        assert_eq!(url, dsn.to_string());
    }

    #[test]
    fn test_dsn_http_url() {
        let url = "http://username@domain:8888/path";
        let dsn = Dsn::from_str(url).unwrap();
        assert_eq!(url, dsn.to_string());
    }

    #[test]
    #[should_panic(expected = "InvalidUrl")]
    fn test_dsn_more_than_one_path() {
        Dsn::from_str("http://username@domain:8888/path/path2").unwrap();
    }

    #[test]
    #[should_panic(expected = "NoUsername")]
    fn test_dsn_no_username() {
        Dsn::from_str("https://:password@domain:8888/path").unwrap();
    }

    #[test]
    #[should_panic(expected = "InvalidUrl")]
    fn test_dsn_invalid_url() {
        Dsn::from_str("random string").unwrap();
    }

    #[test]
    #[should_panic(expected = "InvalidUrl")]
    fn test_dsn_no_host() {
        Dsn::from_str("https://username:password@:8888/path").unwrap();
    }

    #[test]
    #[should_panic(expected = "NoProjectId")]
    fn test_dsn_no_project_id() {
        Dsn::from_str("https://username:password@domain:8888/").unwrap();
    }

    #[test]
    #[should_panic(expected = "InvalidScheme")]
    fn test_dsn_invalid_scheme() {
        Dsn::from_str("ftp://username:password@domain:8888/1").unwrap();
    }
}
