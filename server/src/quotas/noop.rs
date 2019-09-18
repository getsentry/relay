use std::fmt;

use semaphore_common::Config;

use crate::actors::project::{Quota, RetryAfter};

#[derive(Clone)]
pub struct RateLimiter {}

#[derive(Debug)]
pub enum QuotasError {}

impl fmt::Display for QuotasError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl failure::Fail for QuotasError {}

impl RateLimiter {
    pub fn new(_relay_config: &Config) -> Result<Self, QuotasError> {
        Ok(RateLimiter {})
    }

    pub fn is_rate_limited(
        &self,
        _quotas: &[Quota],
        _organization_id: u64,
    ) -> Result<Option<RetryAfter>, QuotasError> {
        Ok(None)
    }
}
