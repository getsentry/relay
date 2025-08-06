use relay_quotas::{ItemScoping, Quota, RateLimits};

use crate::managed::OutcomeError;
use crate::processing::{Context, Counted, Managed, Rejected};

#[cfg(feature = "processing")]
use crate::services::{
    global_rate_limits::GlobalRateLimitsServiceHandle, projects::cache::ProjectCacheHandle,
};

#[cfg(feature = "processing")]
type Redis = relay_quotas::RedisRateLimiter<GlobalRateLimitsServiceHandle>;

/// A quota based rate limiter for Relay's new processing pipeline.
///
/// The rate limiter can enforce in memory/cached quotas as well as enforce quotas consistently
/// with Redis.
pub struct QuotaRateLimiter {
    #[cfg(feature = "processing")]
    project_cache: ProjectCacheHandle,
    #[cfg(feature = "processing")]
    redis: Option<Redis>,
}

impl QuotaRateLimiter {
    /// Creates a new [`Self`].
    #[cfg(feature = "processing")]
    pub fn new(project_cache: ProjectCacheHandle, redis: Option<Redis>) -> Self {
        Self {
            project_cache,
            redis,
        }
    }

    /// Creates a new [`Self`].
    #[cfg(not(feature = "processing"))]
    pub fn new() -> Self {
        Self {}
    }

    /// Enforces quotas for the passed item.
    pub async fn enforce_quotas<T>(
        &self,
        data: &mut Managed<T>,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<<Managed<T> as RateLimited>::Error>>
    where
        T: Counted,
        Managed<T>: RateLimited,
    {
        let quotas = CombinedQuotas {
            global_quotas: &ctx.global_config.quotas,
            project_quotas: ctx.project_info.get_quotas(),
        };

        let limiter = CachedRateLimiter {
            cached: ctx.rate_limits,
            quotas,
        };

        #[cfg(feature = "processing")]
        let limiter = {
            let redis = self.redis.as_ref().map(|redis| redis::RedisRateLimiter {
                redis,
                quotas,
                limits: RateLimits::new(),
                project: self.project_cache.get(data.scoping().project_key),
            });
            redis::CombinedRateLimiter(limiter, redis)
        };

        data.enforce(limiter, ctx).await
    }
}

/// A rate limiter which can attempt to consume quota.
pub trait RateLimiter {
    /// Attempts to consume the specified quota for the specified scope.
    ///
    /// Returns encountered rate limits. The returned rate limits must always be current
    /// and are not allowed to be stale.
    /// Rate limits must always apply the passed `scope`.
    ///
    /// If there are no (empty) rate limits returned, the item shall not be rate limited.
    async fn try_consume(&mut self, scope: ItemScoping, quantity: usize) -> RateLimits;
}

/// An item which can be rate limited with a [`RateLimiter`].
///
/// A [`RateLimiter`] is usually created by the [`QuotaRateLimiter`].
pub trait RateLimited {
    /// Error returned when rejecting the entire item.
    type Error;

    /// Enforce quotas and rate limits using the passed [`RateLimiter`].
    ///
    /// The implementation must check the quotas and already discard the necessary items
    /// as well as emit the correct outcomes.
    async fn enforce<T>(
        &mut self,
        rate_limiter: T,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        T: RateLimiter;
}

impl<T> RateLimiter for Option<T>
where
    T: RateLimiter,
{
    async fn try_consume(&mut self, scope: ItemScoping, quantity: usize) -> RateLimits {
        match self.as_mut() {
            Some(limiter) => limiter.try_consume(scope, quantity).await,
            None => RateLimits::default(),
        }
    }
}

/// A convenience trait for types which need to be rate limited by their [`Counted::quantities`].
///
/// For rate limiting, each category and quantity is rate limited individually,
/// if any category has rate limits enforced the implementation will reject the entire item.
pub trait CountRateLimited {
    type Error: From<RateLimits> + OutcomeError;
}

impl<T> RateLimited for Managed<T>
where
    Managed<T>: CountRateLimited,
    T: Counted,
{
    type Error = <<Managed<T> as CountRateLimited>::Error as OutcomeError>::Error;

    async fn enforce<R>(
        &mut self,
        mut rate_limiter: R,
        _ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        R: RateLimiter,
    {
        let scoping = self.scoping();

        for (category, quantity) in self.quantities() {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                let error = <Managed<T> as CountRateLimited>::Error::from(limits);
                return Err(self.reject_err(error));
            }
        }

        Ok(())
    }
}

/// A [`RateLimiter`] implementation which enforces cached [`RateLimits`].
struct CachedRateLimiter<'a> {
    cached: &'a RateLimits,
    quotas: CombinedQuotas<'a>,
}

impl RateLimiter for CachedRateLimiter<'_> {
    async fn try_consume(&mut self, scope: ItemScoping, _quantity: usize) -> RateLimits {
        self.cached.check_with_quotas(self.quotas, scope)
    }
}

#[cfg(feature = "processing")]
mod redis {
    use crate::services::projects::cache::Project;

    use super::*;
    use relay_quotas::GlobalLimiter;

    /// A [`RateLimiter`] implementation which enforces quotas with Redis.
    pub struct RedisRateLimiter<'a, T> {
        pub redis: &'a relay_quotas::RedisRateLimiter<T>,
        pub quotas: CombinedQuotas<'a>,
        pub limits: RateLimits,
        pub project: Project<'a>,
    }

    impl<T> RateLimiter for RedisRateLimiter<'_, T>
    where
        T: GlobalLimiter,
    {
        async fn try_consume(&mut self, scope: ItemScoping, quantity: usize) -> RateLimits {
            let limits = self
                .redis
                .is_rate_limited(self.quotas, scope, quantity, false)
                .await
                .inspect_err(|err| {
                    relay_log::error!(
                        error = err as &dyn std::error::Error,
                        "rate limiting failed"
                    );
                })
                // Return empty (no) rate limits when the rate limiter fails,
                // this means even with a failing Redis instance no items will be dropped.
                .unwrap_or_default();

            self.limits.merge(limits.clone());
            limits
        }
    }

    impl<T> Drop for RedisRateLimiter<'_, T> {
        fn drop(&mut self) {
            let limits = std::mem::take(&mut self.limits);
            self.project.rate_limits().merge(limits);
        }
    }

    pub struct CombinedRateLimiter<T, S>(pub T, pub S);

    impl<T, S> RateLimiter for CombinedRateLimiter<T, S>
    where
        T: RateLimiter,
        S: RateLimiter,
    {
        async fn try_consume(&mut self, scope: ItemScoping, quantity: usize) -> RateLimits {
            let limits = self.0.try_consume(scope, quantity).await;
            if !limits.is_empty() {
                return limits;
            }

            self.1.try_consume(scope, quantity).await
        }
    }
}

/// Container for global and project level [`Quota`].
#[derive(Copy, Clone, Debug)]
struct CombinedQuotas<'a> {
    global_quotas: &'a [Quota],
    project_quotas: &'a [Quota],
}

impl<'a> IntoIterator for CombinedQuotas<'a> {
    type Item = &'a Quota;
    type IntoIter = std::iter::Chain<std::slice::Iter<'a, Quota>, std::slice::Iter<'a, Quota>>;

    fn into_iter(self) -> Self::IntoIter {
        self.global_quotas.iter().chain(self.project_quotas.iter())
    }
}
