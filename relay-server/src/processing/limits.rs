use relay_quotas::{GlobalLimiter, ItemScoping, Quota, RateLimits};

use crate::processing::Context;
use crate::services::global_rate_limits::GlobalRateLimitsServiceHandle;

// TODO: this may need a better name
#[derive(Default)]
pub struct QuotaRateLimiter {
    redis: Option<relay_quotas::RedisRateLimiter<GlobalRateLimitsServiceHandle>>,
}

impl QuotaRateLimiter {
    // TODO: maybe this should take a `&mut Managed<T>` or some variant of this
    // TODO: maybe this should return a result which can be applied to items, instead
    // of modifying in place?
    pub async fn enforce_quotas<T>(
        &self,
        data: &mut T,
        ctx: Context<'_>,
    ) -> Result<RateLimits, T::Error>
    where
        T: RateLimited,
    {
        let quotas = CombinedQuotas {
            global_quotas: &ctx.global_config.quotas,
            project_quotas: ctx.project_info.get_quotas(),
        };

        // TODO: indexed/non-indexed special casing thing
        let cached = CachedRateLimiter {
            cached: ctx.rate_limits,
            quotas,
        };
        let redis = self.redis.as_ref().map(|redis| RedisRateLimiter {
            redis: &redis,
            quotas,
        });
        let limiter = CombinedRateLimiter(cached, redis);

        data.enforce(limiter, ctx).await
    }
}

struct CachedRateLimiter<'a> {
    cached: &'a RateLimits,
    quotas: CombinedQuotas<'a>,
}

impl RateLimiter for CachedRateLimiter<'_> {
    async fn try_consume(&self, scope: ItemScoping, _quantity: usize) -> RateLimits {
        self.cached.check_with_quotas(self.quotas, scope)
    }
}

struct RedisRateLimiter<'a, T> {
    redis: &'a relay_quotas::RedisRateLimiter<T>,
    quotas: CombinedQuotas<'a>,
}

impl<T> RateLimiter for RedisRateLimiter<'_, T>
where
    T: GlobalLimiter,
{
    async fn try_consume(&self, scope: ItemScoping, quantity: usize) -> RateLimits {
        // TODO: error case
        self.redis
            .is_rate_limited(self.quotas, scope, quantity, false)
            .await
            .unwrap()
    }
}

struct CombinedRateLimiter<T, S>(T, S);

impl<T, S> RateLimiter for CombinedRateLimiter<T, S>
where
    T: RateLimiter,
    S: RateLimiter,
{
    async fn try_consume(&self, scope: ItemScoping, quantity: usize) -> RateLimits {
        let limits = self.0.try_consume(scope, quantity).await;
        if !limits.is_empty() {
            return limits;
        }

        self.1.try_consume(scope, quantity).await
    }
}

pub trait RateLimiter {
    // TODO: over-accept
    //  - Maybe should be a parameter, the old impl always used `false` here
    //  - Maybe should be conditional on the impl
    // TODO: maybe the checker should return Option<RateLimits> or something
    // and only return active rate limits. As `RateLimits` can contain expired limits.
    async fn try_consume(&self, scope: ItemScoping, quantity: usize) -> RateLimits;
}

impl<T> RateLimiter for Option<T>
where
    T: RateLimiter,
{
    async fn try_consume(&self, scope: ItemScoping, quantity: usize) -> RateLimits {
        match self.as_ref() {
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
    ) -> Result<RateLimits, Self::Error>
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
