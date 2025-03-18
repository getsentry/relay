use futures::StreamExt;
use relay_quotas::{
    GlobalLimiter, GlobalRateLimiter, OwnedRedisQuota, RateLimitingError, RedisQuota,
};
use relay_redis::RedisPool;
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
            .into_iter()
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

        // For each owned quota, we lookup the original quota and match it. In case of multiple
        // duplicate quotas, the first one will be matched.
        //
        // This is done to make the `check_global_rate_limits` function depend only on references
        // and hide the fact that we send owned data across threads.
        //
        // The operation is O(n^2) but we are assuming the number of quotas is bounded by a low number
        // since now they are only used for metric buckets limiting.
        let rate_limited_global_quotas = rate_limited_owned_global_quotas.map(|o| {
            o.iter()
                .filter_map(|owned_global_quota| {
                    let global_quota = owned_global_quota.build_ref();
                    global_quotas.iter().find(|x| **x == global_quota)
                })
                .collect::<Vec<_>>()
        });

        rate_limited_global_quotas
    }
}

/// Service implementing the [`GlobalRateLimits`] interface.
///
/// This service provides global rate limiting functionality that is synchronized
/// across multiple instances using a [`RedisPool`].
#[derive(Debug)]
pub struct GlobalRateLimitsService {
    pool: RedisPool,
    limiter: GlobalRateLimiter,
}

impl GlobalRateLimitsService {
    /// Creates a new [`GlobalRateLimitsService`] with the provided Redis pool.
    ///
    /// The service will use the pool to communicate with Redis for synchronizing
    /// rate limits across multiple instances.
    pub fn new(pool: RedisPool) -> Self {
        Self {
            pool,
            limiter: GlobalRateLimiter::default(),
        }
    }
}

impl Service for GlobalRateLimitsService {
    type Interface = GlobalRateLimits;

    async fn run(mut self, mut rx: Receiver<Self::Interface>) {
        let Ok(mut client) = self.pool.client() else {
            relay_log::error!(
                "The redis client could not be created from the global rate limits service"
            );
            return;
        };

        loop {
            let Some(message) = rx.recv().await else {
                break;
            };

            match message {
                GlobalRateLimits::CheckRateLimited(message, sender) => {
                    let quotas = message
                        .global_quotas
                        .iter()
                        .map(|q| q.build_ref())
                        .collect::<Vec<_>>();

                    let result = tokio::task::spawn_blocking(|| {
                        self.limiter
                            .filter_rate_limited(&mut client, &quotas, message.quantity)
                            .map(|q| q.into_iter().map(|q| q.build_owned()).collect())
                    })
                    .await;

                    sender.send(result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::services::global_rate_limits::{CheckRateLimited, GlobalRateLimitsService};
    use relay_base_schema::data_category::DataCategory;
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_common::time::UnixTimestamp;
    use relay_quotas::{DataCategories, Quota, QuotaScope, RedisQuota, Scoping};
    use relay_redis::{RedisConfigOptions, RedisPool};
    use relay_system::Service;
    use std::collections::BTreeSet;

    fn build_redis_pool() -> RedisPool {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        RedisPool::single(&url, RedisConfigOptions::default()).unwrap()
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

    fn build_scoping() -> Scoping {
        Scoping {
            organization_id: OrganizationId::new(69420),
            project_id: ProjectId::new(42),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(4711),
        }
    }

    fn build_redis_quota<'a>(quota: &'a Quota, scoping: &'a Scoping) -> RedisQuota<'a> {
        let scoping = scoping.item(DataCategory::MetricBucket);
        RedisQuota::new(quota, scoping, UnixTimestamp::now()).unwrap()
    }

    #[tokio::test]
    async fn test_global_rate_limits_service() {
        let service = GlobalRateLimitsService::new(build_redis_pool());
        let tx = service.start_detached();

        let scoping = build_scoping();

        let quota1 = build_quota(10, 100);
        let quota2 = build_quota(10, 150);
        let quota3 = build_quota(10, 200);
        let quantity = 175;

        let redis_quotas = [
            build_redis_quota(&quota1, &scoping),
            build_redis_quota(&quota2, &scoping),
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
            BTreeSet::from([100, 150]),
            rate_limited_quotas
                .iter()
                .map(|quota| quota.build_ref().limit())
                .collect()
        );
    }
}
