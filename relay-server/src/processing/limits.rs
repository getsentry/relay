use relay_quotas::{ItemScoping, Quota, RateLimits};

use crate::processing::{Context, Counted, Managed, Rejected};

use crate::services::global_rate_limits::GlobalRateLimitsServiceHandle;
use crate::services::projects::cache::ProjectCacheHandle;

type Redis = relay_quotas::RedisRateLimiter<GlobalRateLimitsServiceHandle>;

// TODO: this may need a better name
pub struct QuotaRateLimiter {
    #[cfg_attr(
        not(feature = "processing"),
        expect(unused, reason = "only needed for processing")
    )]
    project_cache: ProjectCacheHandle,
    #[cfg_attr(
        not(feature = "processing"),
        expect(unused, reason = "only needed for processing")
    )]
    redis: Option<Redis>,
}

impl QuotaRateLimiter {
    pub fn new(project_cache: ProjectCacheHandle, redis: Option<Redis>) -> Self {
        Self {
            project_cache,
            redis,
        }
    }

    // TODO: maybe this should take a `&mut Managed<T>` or some variant of this
    // TODO: maybe this should return a result which can be applied to items, instead
    // of modifying in place?
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

        // TODO: indexed/non-indexed special casing thing, cached rate limits
        // can only be enforced on indexed in the processor.
        // Which means this will always be correct, but there is the chance someone will re-use
        // this code outside of the processor. Do we guard against this already to prevent misuse?
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

pub trait RateLimiter {
    // TODO: over-accept
    //  - Maybe should be a parameter, the old impl always used `false` here
    //  - Maybe should be conditional on the impl
    // TODO: maybe the checker should return Option<RateLimits> or something
    // and only return active rate limits. As `RateLimits` can contain expired limits.
    async fn try_consume(&mut self, scope: ItemScoping, quantity: usize) -> RateLimits;
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

// TODO: better name, something that can be rate limited
pub trait RateLimited {
    type Error;

    async fn enforce<T>(
        &mut self,
        rate_limiter: T,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        T: RateLimiter;
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
