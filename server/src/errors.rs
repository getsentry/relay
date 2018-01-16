use hyper;

#[derive(Debug, Fail)]
pub enum Error {
    /// An HTTP error.
    #[fail(display="http error: {}", _0)]
    #[cause]
    Http(hyper::Error),
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Error {
        Error::Http(err)
    }
}
