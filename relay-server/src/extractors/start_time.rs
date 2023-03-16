use std::convert::Infallible;
use std::time::Instant;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::Extension;

/// The time at which the request started.
#[derive(Clone, Copy, Debug)]
pub struct StartTime(Instant);

impl StartTime {
    pub fn now() -> Self {
        Self(Instant::now())
    }

    /// Returns the `Instant` of this start time.
    #[inline]
    pub fn into_inner(self) -> Instant {
        self.0
    }
}

// TODO(ja): Derive instead, but that requires to handle errors.
#[axum::async_trait]
impl<S> FromRequestParts<S> for StartTime
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Extension(start_time) = Extension::from_request_parts(parts, state)
            .await
            .expect("StartTime middleware is not configured");

        Ok(start_time)
    }
}
