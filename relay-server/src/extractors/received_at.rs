use std::convert::Infallible;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::Extension;
use chrono::{DateTime, Utc};

/// The time at which the request started.
#[derive(Clone, Copy, Debug)]
pub struct ReceivedAt(DateTime<Utc>);

impl ReceivedAt {
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Returns the `Instant` of this start time.
    #[inline]
    pub fn into_inner(self) -> DateTime<Utc> {
        self.0
    }

    /// Returns the [`ReceivedAt`] corresponding to provided timestamp.
    pub fn from_timestamp_millis(timestamp: i64) -> Self {
        let datetime =
            DateTime::<Utc>::from_timestamp_millis(timestamp).unwrap_or_else(|| Utc::now());

        Self(datetime)
    }
}

#[axum::async_trait]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn start_time_from_timestamp() {
        let elapsed = Duration::from_secs(10);
        let now = Utc::now();
        let past = now - chrono::Duration::from_std(elapsed).unwrap();
        let start_time = ReceivedAt::from_timestamp_millis(past.timestamp_millis()).into_inner();

        // Check that the difference between the now and generated start_time is about 10s
        let diff = now - start_time;
        let diff_duration = diff.to_std().unwrap();
        assert!(diff_duration < elapsed + Duration::from_millis(50));
        assert!(diff_duration > elapsed - Duration::from_millis(50));
    }
}
