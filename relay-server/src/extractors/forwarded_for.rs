use actix_web::http::header;
use actix_web::{Error, FromRequest, HttpMessage, HttpRequest};

#[derive(Debug)]
pub struct ForwardedFor(String);

impl ForwardedFor {
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for ForwardedFor {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<ForwardedFor> for String {
    fn from(forwarded: ForwardedFor) -> Self {
        forwarded.into_inner()
    }
}

impl<'a, S> From<&'a HttpRequest<S>> for ForwardedFor {
    fn from(request: &'a HttpRequest<S>) -> Self {
        let peer_addr = request
            .peer_addr()
            .map(|v| v.ip().to_string())
            .unwrap_or_else(String::new);

        let forwarded = request
            .headers()
            .get("X-Forwarded-For")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        ForwardedFor(if forwarded.is_empty() {
            peer_addr
        } else if peer_addr.is_empty() {
            forwarded.to_string()
        } else {
            format!("{}, {}", forwarded, peer_addr)
        })
    }
}

impl<'a> header::IntoHeaderValue for &'a ForwardedFor {
    type Error = header::InvalidHeaderValue;

    fn try_into(self) -> Result<header::HeaderValue, Self::Error> {
        (self.0.as_str()).try_into()
    }
}

impl header::IntoHeaderValue for ForwardedFor {
    type Error = header::InvalidHeaderValueBytes;

    fn try_into(self) -> Result<header::HeaderValue, Self::Error> {
        self.0.try_into()
    }
}

impl<S> FromRequest<S> for ForwardedFor {
    type Config = ();
    type Result = Result<Self, Error>;

    fn from_request(request: &HttpRequest<S>, _cfg: &Self::Config) -> Self::Result {
        Ok(Self::from(request))
    }
}
