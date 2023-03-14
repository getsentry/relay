use std::time::Instant;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::Extension;

/// The time at which the request started.
#[derive(Clone, Copy, Debug)]
pub struct StartTime(Instant);

impl StartTime {
    /// Returns the `Instant` of this start time.
    #[inline]
    pub fn into_inner(self) -> Instant {
        self.0
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for StartTime
where
    S: Send + Sync,
{
    type Rejection = <Extension<Self> as FromRequestParts<S>>::Rejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Extension(start_time) = Extension::from_request_parts(parts, state).await?;
        Ok(start_time)
    }
}
