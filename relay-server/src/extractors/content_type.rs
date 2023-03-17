use std::convert::Infallible;
use std::fmt;

use axum::extract::FromRequestParts;
use axum::http::header;
use axum::http::request::Parts;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawContentType(String);

impl RawContentType {
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for RawContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for RawContentType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for RawContentType {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let mime = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();

        Ok(Self(mime))
    }
}
