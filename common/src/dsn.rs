use std::fmt;
use std::str::FromStr;

use url::Url;

/// Represents a dsn url parsing error.
#[derive(Debug, Fail)]
pub enum DsnParseError {
    #[fail(display = "no valid url provided")] InvalidUrl,
    #[fail(display = "username is empty")] NoUsername,
    #[fail(display = "empty path")] NoPath,
}

/// Represents a Sentry dsn.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Dsn {
    scheme: String,
    username: String,
    password: Option<String>,
    host: String,
    port: Option<u16>,
    path: String,
}

impl Dsn {
    /// Returns the scheme
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Returns the username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns password
    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(|x| x.as_str())
    }

    /// Returns the host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the port
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Returns the path
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl fmt::Display for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.username)?;
        if let Some(ref password) = self.password {
            write!(f, ":{}", password)?;
        }
        write!(f, "@{}", self.host)?;
        if let Some(ref port) = self.port {
            write!(f, ":{}", port)?;
        }
        write!(f, "{}", self.path)?;
        Ok(())
    }
}

impl FromStr for Dsn {
    type Err = DsnParseError;

    fn from_str(s: &str) -> Result<Dsn, DsnParseError> {
        let url = Url::parse(s).map_err(|_| DsnParseError::InvalidUrl)?;

        if url.path() == "/" {
            return Err(DsnParseError::NoPath);
        }

        let username = match url.username() {
            "" => return Err(DsnParseError::NoUsername),
            username => username.to_string(),
        };

        let scheme = url.scheme().to_string();
        let password = url.password().map(|s| s.into());
        let port = url.port().map(|s| s.into());
        let host = match url.host_str() {
            Some(host) => host.into(),
            None => return Err(DsnParseError::InvalidUrl),
        };
        let path = url.path().into();

        Ok(Dsn {
            scheme,
            username,
            password,
            port,
            host,
            path,
        })
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_dsn_parsing() {
        let url = "https://username:password@domain:8888/path";
        let dsn = url.parse::<Dsn>().unwrap();
        assert_eq!(dsn.scheme(), "https");
        assert_eq!(dsn.username(), "username");
        assert_eq!(dsn.password(), Some("password"));
        assert_eq!(dsn.host(), "domain");
        assert_eq!(dsn.port(), Some(8888));
        assert_eq!(dsn.path(), "/path");
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
    #[should_panic(expected = "NoPath")]
    fn test_dsn_no_path() {
        Dsn::from_str("https://username:password@domain:8888/").unwrap();
    }
}
