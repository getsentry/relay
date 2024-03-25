use std::convert::Infallible;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

    /// Returns the [`StartTime`] corresponding to provided timestamp.
    pub fn from_timestamp_millis(timestamp: u64) -> Self {
        let ts = Duration::from_millis(timestamp);

        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .checked_sub(ts)
            .unwrap_or_default();

        let start_time = Instant::now()
            // Subtract the elapsed time from the current `Instant` to get the timestamp
            // of the start time.
            .checked_sub(elapsed)
            // If we fail to get the `Instant` from the timestamp, we fallback to `now()`.
            .unwrap_or_else(Instant::now);

        Self(start_time)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_time_from_timestamp() {
        let elapsed = Duration::from_secs(10);
        let now = Instant::now();
        let system_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - elapsed;
        let start_time =
            StartTime::from_timestamp_millis(system_time.as_millis() as u64).into_inner();

        // Check that the difference between the now and generated start_time is about 10s.
        assert!((now - start_time) < elapsed + Duration::from_millis(50));
        assert!((now - start_time) > elapsed - Duration::from_millis(50));
    }
}
