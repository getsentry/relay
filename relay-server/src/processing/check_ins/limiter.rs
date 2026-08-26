use std::sync::{Arc, OnceLock};

use relay_quotas::{DataCategories, DataCategory, Quota, QuotaScope, ReasonCode};
use uuid::Uuid;

/// Reason code reported on outcomes for check-ins dropped by this limiter.
pub const REASON_CODE: &str = "monitor_rate_limit";

/// Default number of heck-in messages permitted per monitor environment per window
/// Mirrors `crons.per_monitor_rate_limit` in Sentry
pub const DEFAULT_LIMIT: u64 = 6;

/// Length of the window in seconds.
pub const DEFAULT_WINDOW: u64 = 60;

/// Builds a quota that counts a single monitor environment.
///
/// `environment` is expected to already be normalized by [`relay_monitors::process_check_in`],
/// which is also what the Kafka routing key is derived from, so the two cannot disagree.
pub fn monitor_quota(slug: &str, environment: &str, limit: u64, window: u64) -> Quota {
    static NAMESPACE: OnceLock<Uuid> = OnceLock::new();
    let namespace = NAMESPACE
        .get_or_init(|| Uuid::new_v5(&Uuid::NAMESPACE_URL, b"https://sentry.io/crons/#rl"));

    let key = format!("{}:{slug}:{environment}", slug.len());
    let id = format!(
        "monitor:{}",
        Uuid::new_v5(namespace, key.as_bytes()).simple()
    );

    Quota {
        id: Some(Arc::from(id)),
        categories: DataCategories::from_slice(&[DataCategory::Monitor]),
        scope: QuotaScope::Project,
        scope_id: None,
        limit: Some(limit),
        window: Some(window),
        namespace: None,
        reason_code: Some(ReasonCode::new(REASON_CODE)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota(slug: &str, environment: &str) -> Quota {
        monitor_quota(slug, environment, DEFAULT_LIMIT, DEFAULT_WINDOW)
    }

    #[test]
    fn test_id_is_stable() {
        assert_eq!(quota("nightly", "prod").id, quota("nightly", "prod").id);
        assert_eq!(
            quota("nightly", "prod").id.as_deref(),
            Some("monitor:7cdfae8f0da55aecba3b846aa8c31c14")
        );
    }

    #[test]
    fn test_environments_do_not_share_a_counter() {
        assert_ne!(quota("job", "prod").id, quota("job", "stg").id);
    }

    #[test]
    fn test_separator_in_slug_does_not_collide() {
        assert_ne!(quota("job", "a:b").id, quota("job:a", "b").id);
    }

    #[test]
    fn test_applies_to_check_ins_in_any_project() {
        let quota = quota("job", "production");

        assert_eq!(quota.scope, QuotaScope::Project);
        assert_eq!(quota.scope_id, None);
        assert!(quota.categories.contains(&DataCategory::Monitor));
        assert!(quota.id.is_some(), "an id is required to count in redis");
    }
}

/// Tests against a live redis
///
/// Every test derives a unique slug so counters from an earlier run cannot leak into a later one.
#[cfg(test)]
mod redis_tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_quotas::{RedisRateLimiter, Scoping};
    use relay_redis::{AsyncRedisClient, RedisConfigOptions};

    use super::*;

    fn build_limiter() -> RedisRateLimiter {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let client =
            AsyncRedisClient::single("test", &url, &RedisConfigOptions::default()).unwrap();

        RedisRateLimiter::new(client)
    }

    fn scoping(project_id: u64) -> Scoping {
        Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(project_id),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: None,
        }
    }

    fn unique_slug(name: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        format!("{name}-{nanos}")
    }

    async fn check(
        limiter: &RedisRateLimiter,
        slug: &str,
        environment: &str,
        project_id: u64,
    ) -> bool {
        let quota = monitor_quota(slug, environment, DEFAULT_LIMIT, DEFAULT_WINDOW);
        let scoping = scoping(project_id);

        limiter
            .is_rate_limited(&[quota], scoping.item(DataCategory::Monitor), 1, false)
            .await
            .unwrap()
            .is_limited()
    }

    #[tokio::test]
    async fn test_limits_once_the_allowance_is_used_up() {
        let limiter = build_limiter();
        let slug = unique_slug("noisy");

        for i in 0..DEFAULT_LIMIT {
            assert!(
                !check(&limiter, &slug, "production", 1).await,
                "check-in {i} passes"
            );
        }

        assert!(
            check(&limiter, &slug, "production", 1).await,
            "the next is limited"
        );
        assert!(
            check(&limiter, &slug, "production", 1).await,
            "and stays limited"
        );
    }

    #[tokio::test]
    async fn test_reports_the_expected_reason_code() {
        let limiter = build_limiter();
        let slug = unique_slug("reason");
        let quota = monitor_quota(&slug, "production", 0, DEFAULT_WINDOW);
        let scoping = scoping(1);

        let limits = limiter
            .is_rate_limited(&[quota], scoping.item(DataCategory::Monitor), 1, false)
            .await
            .unwrap();

        let reason = limits.longest().and_then(|limit| limit.reason_code.clone());
        assert_eq!(reason.as_ref().map(|r| r.as_str()), Some(REASON_CODE));
    }

    #[tokio::test]
    async fn test_environments_do_not_share_an_allowance() {
        let limiter = build_limiter();
        let slug = unique_slug("shared");

        for _ in 0..DEFAULT_LIMIT {
            assert!(!check(&limiter, &slug, "prod", 1).await);
        }
        assert!(check(&limiter, &slug, "prod", 1).await, "prod limited");

        assert!(
            !check(&limiter, &slug, "staging", 1).await,
            "staging has its own allowance"
        );
    }

    #[tokio::test]
    async fn test_projects_do_not_share_an_allowance() {
        let limiter = build_limiter();
        let slug = unique_slug("cross-project");

        for _ in 0..DEFAULT_LIMIT {
            assert!(!check(&limiter, &slug, "production", 1).await);
        }
        assert!(
            check(&limiter, &slug, "production", 1).await,
            "project 1 limited"
        );

        assert!(
            !check(&limiter, &slug, "production", 2).await,
            "the same slug in another project is unaffected"
        );
    }
}
