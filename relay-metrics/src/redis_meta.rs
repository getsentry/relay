use hash32::{FnvHasher, Hasher as _};
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_redis::{RedisError, RedisPool};

use crate::{MetricMeta, MetricMetaItem, MetricResourceIdentifier};

/// Redis metric meta
pub struct RedisMetricMetaStore {
    redis: RedisPool,
    expiry_in_seconds: u64,
}

impl RedisMetricMetaStore {
    /// Creates a new Redis metrics meta store.
    pub fn new(redis: RedisPool) -> Self {
        Self {
            redis,
            // 14 days + 1 to make sure it stays at least the entire day
            expiry_in_seconds: 15 * 24 * 60 * 60, // TODO: dont hardcode this
        }
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

        for (mri, items) in meta.mapping {
            let key = self.build_redis_key(organization_id, project_id, *meta.timestamp, &mri);

            // Should be fine if we don't batch here, we expect a very small amount of locations
            // from the aggregator.
            let mut location_cmd = relay_redis::redis::cmd("SADD");
            location_cmd.arg(&key);

            for item in items {
                match item {
                    MetricMetaItem::Location(location) => {
                        let member = serde_json::to_string(&location).unwrap();
                        location_cmd.arg(member);
                    }
                }
            }

            relay_log::trace!("storing metric meta for project {organization_id}:{project_id}");
            location_cmd
                .query(&mut connection)
                .map_err(RedisError::Redis)?;

            // use original timestamp to not bump expiry
            let expire_at = meta.timestamp.as_secs() + self.expiry_in_seconds;
            relay_redis::redis::Cmd::expire_at(key, expire_at as usize)
                .query(&mut connection)
                .map_err(RedisError::Redis)?;
        }

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
    use super::*;

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
