use std::fmt;

use failure::{Backtrace, Context, Fail};

/// Common error type for the relay server.
#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

/// Indicates the type of failure of the server.
#[derive(Debug, Fail, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Binding failed.
    #[fail(display = "bind to interface failed")]
    BindFailed,

    /// Listening on the HTTP socket failed.
    #[fail(display = "listening failed")]
    ListenFailed,

    /// Spawning the trove governor failed.
    #[fail(display = "aorta trove governor spawn failed")]
    TroveGovernSpawnFailed,

    /// Abdicating the trove governor failed.
    #[fail(display = "aorta trove governor shutdown failed")]
    TroveGovernShutdownFailed,
}

impl Fail for Error {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Error {
    /// Returns the error kind of the error.
    pub fn kind(&self) -> ErrorKind {
        *self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Error {
        Error { inner: inner }
    }
}
