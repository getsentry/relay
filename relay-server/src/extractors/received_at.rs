use std::convert::Infallible;

use axum::Extension;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use chrono::{DateTime, Utc};

/// The time at which the request started.
#[derive(Clone, Copy, Debug)]
pub struct ReceivedAt(pub DateTime<Utc>);

impl ReceivedAt {
    pub fn now() -> Self {
        Self(Utc::now())
    }
}

impl<S> FromRequestParts<S> for ReceivedAt
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Extension(start_time) = Extension::from_request_parts(parts, state)
            .await
            .expect("ReceivedAt middleware is not configured");

        Ok(start_time)
    }
}
