use relay_quotas::{
    GlobalLimiter, GlobalRateLimiter, OwnedRedisQuota, RateLimitingError, RedisQuota,
};
use relay_redis::AsyncRedisPool;
use relay_system::{
    Addr, AsyncResponse, FromMessage, Interface, MessageResponse, Receiver, Sender, Service,
};

/// A request to check which quotas are rate limited by the global rate limiter.
///
/// This is typically sent to a [`GlobalRateLimitsService`] to determine which quotas
/// should be rate limited based on the current usage.
pub struct CheckRateLimited {
    pub global_quotas: Vec<OwnedRedisQuota>,
    pub quantity: usize,
}

/// Interface for global rate limiting operations.
///
/// This enum defines the messages that can be sent to a service implementing
/// global rate limiting functionality.
pub enum GlobalRateLimits {
    /// Checks which quotas are rate limited by the global rate limiter.
    CheckRateLimited(
        CheckRateLimited,
        Sender<Result<Vec<OwnedRedisQuota>, RateLimitingError>>,
    ),
}

impl Interface for GlobalRateLimits {}

impl FromMessage<CheckRateLimited> for GlobalRateLimits {
    type Response = AsyncResponse<Result<Vec<OwnedRedisQuota>, RateLimitingError>>;

    fn from_message(
        message: CheckRateLimited,
        sender: <Self::Response as MessageResponse>::Sender,
    ) -> Self {
        Self::CheckRateLimited(message, sender)
    }
}

/// A handle to the [`GlobalRateLimitsServiceHandle`].
///
/// This handle implements [`GlobalLimiter`] to expose the global rate limiting feature from the
/// [`GlobalRateLimitsServiceHandle`].
pub struct GlobalRateLimitsServiceHandle {
    tx: Addr<GlobalRateLimits>,
}

impl GlobalLimiter for GlobalRateLimitsServiceHandle {
    async fn check_global_rate_limits<'a>(
        &self,
        global_quotas: &'a [RedisQuota<'a>],
        quantity: usize,
    ) -> Result<Vec<&'a RedisQuota<'a>>, RateLimitingError> {
        // We build the owned quotas to send over to the service.
        let owned_global_quotas = global_quotas
            .iter()
            .map(|q| q.build_owned())
            .collect::<Vec<_>>();

        let rate_limited_owned_global_quotas = self
            .tx
            .send(CheckRateLimited {
                global_quotas: owned_global_quotas,
                quantity,
            })
            .await
            .map_err(|_| RateLimitingError::UnreachableGlobalRateLimits)?;

        // Perform a reverse lookup to match each owned quota with its original reference.
        // If multiple identical quotas exist, the first match will be reused. Equality is determined
        // by the `Eq` and `PartialEq` implementations, meaning duplicate quotas will return
        // the same reference multiple times. This does not impact correctness since the returned
        // reference is interchangeable with any other identical quota.
        //
        // This design ensures that `check_global_rate_limits` operates exclusively on references,
        // abstracting away the fact that owned data is transferred across threads.
        //
        // The operation has a time complexity of O(n^2), but the number of quotas is assumed
        // to be small, as they are currently used only for metric bucket limiting.
        let rate_limited_global_quotas =
            rate_limited_owned_global_quotas.map(|owned_global_quotas| {
                owned_global_quotas
                    .iter()
                    .filter_map(|owned_global_quota| {
                        let global_quota = owned_global_quota.build_ref();
                        global_quotas.iter().find(|x| **x == global_quota)
                    })
                    .collect::<Vec<_>>()
            });

        rate_limited_global_quotas
    }
}

impl From<Addr<GlobalRateLimits>> for GlobalRateLimitsServiceHandle {
    fn from(tx: Addr<GlobalRateLimits>) -> Self {
        Self { tx }
    }
}

/// Service implementing the [`GlobalRateLimits`] interface.
///
/// This service provides global rate limiting functionality that is synchronized
/// across multiple instances using a [`AsyncRedisPool`].
#[derive(Debug)]
pub struct GlobalRateLimitsService {
    pool: AsyncRedisPool,
    limiter: GlobalRateLimiter,
}

impl GlobalRateLimitsService {
    /// Creates a new [`GlobalRateLimitsService`] with the provided Redis pool.
    ///
    /// The service will use the pool to communicate with Redis for synchronizing
    /// rate limits across multiple instances.
    pub fn new(pool: AsyncRedisPool) -> Self {
        Self {
            client,
            limiter: GlobalRateLimiter::default(),
        }
    }

    /// Handles a [`GlobalRateLimits`] message.
    async fn handle_message(
        pool: &AsyncRedisPool,
        limiter: &mut GlobalRateLimiter,
        message: GlobalRateLimits,
    ) {
        match message {
            GlobalRateLimits::CheckRateLimited(check_rate_limited, sender) => {
                let result =
                    Self::handle_check_rate_limited(client, limiter, check_rate_limited).await;
                sender.send(result);
            }
        }
    }

    /// Handles the [`GlobalRateLimits::CheckRateLimited`] message.
    ///
    /// This function uses `spawn_blocking` to suspend on synchronous work that is offloaded to
    /// a specialized thread pool.
    async fn handle_check_rate_limited(
        pool: &AsyncRedisPool,
        limiter: &mut GlobalRateLimiter,
        check_rate_limited: CheckRateLimited,
    ) -> Result<Vec<OwnedRedisQuota>, RateLimitingError> {
        let quotas = check_rate_limited
            .global_quotas
            .iter()
            .map(|q| q.build_ref())
            .collect::<Vec<_>>();

        limiter
            .filter_rate_limited(client, &quotas, check_rate_limited.quantity)
            .await
            .map(|q| q.into_iter().map(|q| q.build_owned()).collect::<Vec<_>>())
    }
}

impl Service for GlobalRateLimitsService {
    type Interface = GlobalRateLimits;

    async fn run(mut self, mut rx: Receiver<Self::Interface>) {
        loop {
            let Some(message) = rx.recv().await else {
                break;
            };

            Self::handle_message(&self.client, &mut self.limiter, message).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use relay_base_schema::data_category::DataCategory;
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_common::time::UnixTimestamp;
    use relay_quotas::{DataCategories, Quota, QuotaScope, RedisQuota, Scoping};
    use relay_redis::{AsyncRedisPool, RedisConfigOptions};
    use relay_system::Service;

    use crate::services::global_rate_limits::{CheckRateLimited, GlobalRateLimitsService};

    fn build_redis_pool() -> AsyncRedisPool {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        AsyncRedisPool::single(&url, &RedisConfigOptions::default()).unwrap()
    }

    fn build_quota(window: u64, limit: impl Into<Option<u64>>) -> Quota {
        Quota {
            id: Some(uuid::Uuid::new_v4().to_string()),
            categories: DataCategories::new(),
            scope: QuotaScope::Global,
            scope_id: None,
            window: Some(window),
            limit: limit.into(),
            reason_code: None,
            namespace: None,
        }
    }

    fn build_redis_quota<'a>(quota: &'a Quota, scoping: &'a Scoping) -> RedisQuota<'a> {
        let scoping = scoping.item(DataCategory::MetricBucket);
        RedisQuota::new(quota, scoping, UnixTimestamp::now()).unwrap()
    }

    #[tokio::test]
    async fn test_global_rate_limits_service() {
        let client = build_redis_pool();
        let service = GlobalRateLimitsService::new(client);
        let tx = service.start_detached();

        let scoping = Scoping {
            organization_id: OrganizationId::new(69420),
            project_id: ProjectId::new(42),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(4711),
        };

        let quota1 = build_quota(10, 100);
        let quota2 = build_quota(10, 150);
        let quota3 = build_quota(10, 200);
        let quantity = 175;

        let redis_quota_2 = build_redis_quota(&quota2, &scoping);
        let redis_quotas = [
            build_redis_quota(&quota1, &scoping),
            // We add a duplicated quota, to make sure the reverse mapping works.
            redis_quota_2.clone(),
            redis_quota_2,
            build_redis_quota(&quota3, &scoping),
        ]
        .iter()
        .map(|q| q.build_owned())
        .collect();

        let check_rate_limited = CheckRateLimited {
            global_quotas: redis_quotas,
            quantity,
        };

        let rate_limited_quotas = tx.send(check_rate_limited).await.unwrap().unwrap();

        // Only the quotas that are less than the quantity gets rate_limited.
        assert_eq!(
            BTreeSet::from([100, 150, 150]),
            rate_limited_quotas
                .iter()
                .map(|quota| quota.build_ref().limit())
                .collect()
        );
    }
}
