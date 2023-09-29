use chrono::{DateTime, Utc};

use crate::config::RuleId;

pub struct ReservoirRuleKey(String);

impl ReservoirRuleKey {
    pub fn new(org_id: u64, rule_id: RuleId) -> Self {
        Self(format!("reservoir:{}:{}", org_id, rule_id))
    }

    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Increments the reservoir count for a given rule in redis.
///
/// - INCR docs: [`https://redis.io/commands/incr/`]
/// - If the counter doesn't exist in redis, a new one will be inserted.
pub fn increment_redis_reservoir_count(
    redis_connection: &mut relay_redis::Connection,
    key: &ReservoirRuleKey,
) -> anyhow::Result<i64> {
    let val = relay_redis::redis::cmd("INCR")
        .arg(key.as_str())
        .query(redis_connection)?;

    Ok(val)
}

/// Sets the expiry time for a reservoir rule count.
pub fn set_redis_expiry(
    redis_connection: &mut relay_redis::Connection,
    key: &ReservoirRuleKey,
    rule_expiry: Option<&DateTime<Utc>>,
) -> anyhow::Result<()> {
    let now = Utc::now().timestamp();
    let expiry_time = rule_expiry
        .map(|rule_expiry| rule_expiry.timestamp() + 60)
        .unwrap_or_else(|| now + 86400);

    relay_redis::redis::cmd("EXPIRE")
        .arg(key.as_str())
        .arg(expiry_time - now)
        .query(redis_connection)?;
    Ok(())
}
