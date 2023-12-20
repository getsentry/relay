use std::time::Duration;

use hash32::{FnvHasher, Hasher as _};
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_redis::{RedisError, RedisPool};

use super::{Item, MetricMeta};
use crate::{statsd::MetricCounters, MetricResourceIdentifier};

/// Redis metric meta
pub struct RedisMetricMetaStore {
    redis: RedisPool,
    expiry: Duration,
}

impl RedisMetricMetaStore {
    /// Creates a new Redis metrics meta store.
    pub fn new(redis: RedisPool, expiry: Duration) -> Self {
        Self { redis, expiry }
    }

    /// Stores metric metadata in Redis.
    pub fn store(
        &self,
        organization_id: u64,
        project_id: ProjectId,
        meta: MetricMeta,
    ) -> Result<(), RedisError> {
        let mut client = self.redis.client()?;
        let mut connection = client.connection()?;

        let mut redis_updates = 0;

        let mut pipe = relay_redis::redis::pipe();
        for (mri, items) in meta.mapping {
            let key = self.build_redis_key(organization_id, project_id, *meta.timestamp, &mri);

            // Should be fine if we don't batch here, we expect a very small amount of locations
            // from the aggregator.
            let location_cmd = pipe.cmd("SADD").arg(&key);
            for item in items {
                match item {
                    Item::Location(location) => {
                        let member = serde_json::to_string(&location).unwrap();
                        location_cmd.arg(member);
                    }
                    Item::Unknown => {}
                }
            }
            location_cmd.ignore();

            redis_updates += 1;
            relay_log::trace!("storing metric meta for project {organization_id}:{project_id}");

            // use original timestamp to not bump expiry
            let expire_at = meta.timestamp.as_secs() + self.expiry.as_secs();
            pipe.cmd("EXPIREAT").arg(key).arg(expire_at).ignore();
        }
        pipe.query(&mut connection).map_err(RedisError::Redis)?;

        relay_statsd::metric!(counter(MetricCounters::MetaRedisUpdate) += redis_updates);

        Ok(())
    }

    fn build_redis_key(
        &self,
        organization_id: u64,
        project_id: ProjectId,
        timestamp: UnixTimestamp,
        mri: &MetricResourceIdentifier<'_>,
    ) -> String {
        let mri_hash = mri_to_fnv1a32(mri);

        format!("mm:l:{{{organization_id}}}:{project_id}:{mri_hash}:{timestamp}")
    }
}

/// Converts an MRI to a fnv1a32 hash.
///
/// Sentry also uses the exact same algorithm to hash MRIs.
fn mri_to_fnv1a32(mri: &MetricResourceIdentifier<'_>) -> u32 {
    let mut hasher = FnvHasher::default();

    let s = mri.to_string();
    // hash the bytes directly, `write_str` on the hasher adds a 0xff byte at the end
    std::hash::Hasher::write(&mut hasher, s.as_bytes());
    hasher.finish32()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use relay_redis::RedisConfigOptions;

    use crate::meta::{Location, StartOfDayUnixTimestamp};

    use super::*;

    fn build_store() -> RedisMetricMetaStore {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        let redis = RedisPool::single(&url, RedisConfigOptions::default()).unwrap();

        RedisMetricMetaStore::new(redis, Duration::from_secs(86400))
    }

    #[test]
    fn test_store() {
        let store = build_store();

        let organization_id = 1000;
        let project_id = ProjectId::new(2);
        let mri = MetricResourceIdentifier::parse("c:foo").unwrap();

        let timestamp = StartOfDayUnixTimestamp::new(UnixTimestamp::now()).unwrap();

        let location = Location {
            filename: Some("foobar".to_owned()),
            abs_path: None,
            module: None,
            function: None,
            lineno: Some(42),
            pre_context: Vec::new(),
            context_line: None,
            post_context: Vec::new(),
        };

        store
            .store(
                organization_id,
                project_id,
                MetricMeta {
                    timestamp,
                    mapping: HashMap::from([(mri.clone(), vec![Item::Location(location.clone())])]),
                },
            )
            .unwrap();

        let mut client = store.redis.client().unwrap();
        let mut connection = client.connection().unwrap();
        let key = store.build_redis_key(organization_id, project_id, *timestamp, &mri);
        let locations: Vec<String> = relay_redis::redis::cmd("SMEMBERS")
            .arg(key)
            .query(&mut connection)
            .unwrap();

        assert_eq!(locations, vec![serde_json::to_string(&location).unwrap()]);
    }

    #[test]
    fn test_mri_hash() {
        fn test_mri(s: &str, expected_hash: u32) {
            let mri = MetricResourceIdentifier::parse(s).unwrap();
            assert_eq!(mri_to_fnv1a32(&mri), expected_hash);
        }

        // Sentry has the same tests.
        test_mri("c:transactions/count_per_root_project@none", 2684394786);
        test_mri("d:transactions/duration@millisecond", 1147819254);
        test_mri("s:transactions/user@none", 1739810785);
        test_mri("c:custom/user.click@none", 1248146441);
        test_mri("d:custom/page.load@millisecond", 2103554973);
        test_mri("s:custom/username@none", 670706478);
    }
}
