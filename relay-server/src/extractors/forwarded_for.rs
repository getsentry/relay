use std::convert::Infallible;
use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;

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

#[axum::async_trait]
impl<S> FromRequestParts<S> for ForwardedFor
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let peer_addr = ConnectInfo::<SocketAddr>::from_request_parts(parts, state)
            .await
            .map(|v| v.ip().to_string())
            .unwrap_or_else(String::new);

        let forwarded = parts
            .headers
            .get("X-Forwarded-For")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        Ok(ForwardedFor(if forwarded.is_empty() {
            peer_addr
        } else if peer_addr.is_empty() {
            forwarded.to_string()
        } else {
            format!("{forwarded}, {peer_addr}")
        }))
    }
}

// impl<'a, S> From<&'a HttpRequest<S>> for ForwardedFor {
//     fn from(request: &'a HttpRequest<S>) -> Self {
//         let peer_addr = request
//             .peer_addr()
//             .map(|v| v.ip().to_string())
//             .unwrap_or_else(String::new);

//         let forwarded = request
//             .headers()
//             .get("X-Forwarded-For")
//             .and_then(|v| v.to_str().ok())
//             .unwrap_or("");

//         ForwardedFor(if forwarded.is_empty() {
//             peer_addr
//         } else if peer_addr.is_empty() {
//             forwarded.to_string()
//         } else {
//             format!("{forwarded}, {peer_addr}")
//         })
//     }
// }

// impl<'a> header::IntoHeaderValue for &'a ForwardedFor {
//     type Error = header::InvalidHeaderValue;

//     fn try_into(self) -> Result<header::HeaderValue, Self::Error> {
//         IntoHeaderValue::try_into(self.0.as_str())
//     }
// }

// impl header::IntoHeaderValue for ForwardedFor {
//     type Error = header::InvalidHeaderValueBytes;

//     fn try_into(self) -> Result<header::HeaderValue, Self::Error> {
//         IntoHeaderValue::try_into(self.0)
//     }
// }

// impl<S> FromRequest<S> for ForwardedFor {
//     type Config = ();
//     type Result = Result<Self, Error>;

//     fn from_request(request: &HttpRequest<S>, _cfg: &Self::Config) -> Self::Result {
//         Ok(Self::from(request))
//     }
// }
