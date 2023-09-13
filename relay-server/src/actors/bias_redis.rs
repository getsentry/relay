use relay_base_schema::project::ProjectKey;
use relay_redis::{RedisError, RedisPool};
use relay_sampling::config::RuleId;
use relay_system::Addr;

use crate::actors::upstream::UpstreamRelay;

/// When we reached the reservoir limit, we want to remove the rule from sentry. Sentry should
/// after this function, remove the bias rule from the [`SamplingConfig`] of all affected projects.
pub fn notify_sentry_we_reached_limit(
    _upstream: Addr<UpstreamRelay>,
    _project_key: &ProjectKey,
    _rule_id: RuleId,
) {
    todo!()
}

pub struct BiasRedisKey(String);

impl BiasRedisKey {
    pub fn new(project_key: &ProjectKey, rule_id: RuleId) -> Self {
        Self(format!("bias:{}:{}", project_key, rule_id))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Update the synchronized bias counter in redis.
pub fn update_redis_bias_count(
    redis: RedisPool,
    reservoir_limit: usize,
    upstream: Addr<UpstreamRelay>,
    project_key: ProjectKey,
    rule_id: RuleId,
) -> anyhow::Result<()> {
    let key = BiasRedisKey::new(&project_key, rule_id);

    increment_bias_rule_count(&redis, &key).unwrap();
    let total_count = get_bias_rule_count(&redis, &key)?;
    if total_count as usize >= reservoir_limit {
        delete_bias_rule(&redis, &key)?;
        notify_sentry_we_reached_limit(upstream, &project_key, rule_id);
    };
    Ok(())
}

/// Get the current synchronized bias count.
fn get_bias_rule_count(redis_pool: &RedisPool, key: &BiasRedisKey) -> anyhow::Result<i64> {
    let mut command = relay_redis::redis::cmd("GET");

    command.arg(key.as_str());

    let raw_response_opt: Option<Vec<u8>> = command
        .query(&mut redis_pool.client()?.connection()?)
        .map_err(RedisError::Redis)?;

    let response = match raw_response_opt {
        Some(ref response) => {
            let count = std::str::from_utf8(response)?.parse::<i64>()?;
            Some(count)
        }
        None => None,
    };

    response
        .ok_or_else(|| anyhow::anyhow!("Key not found"))
        .map_err(Into::into)
}

/// Increments the count, it will be initialized automatically if it doesn't exist.
///
/// INCR docs: [`https://redis.io/commands/incr/`]
fn increment_bias_rule_count(redis_pool: &RedisPool, key: &BiasRedisKey) -> anyhow::Result<i64> {
    let mut command = relay_redis::redis::cmd("INCR");
    command.arg(key.as_str());
    let new_count: i64 = command.query(&mut redis_pool.client()?.connection()?)?;
    Ok(new_count)
}

fn delete_bias_rule(redis_pool: &RedisPool, key: &BiasRedisKey) -> anyhow::Result<()> {
    let mut command = relay_redis::redis::cmd("DEL");
    command.arg(key.as_str());

    let _: i64 = command
        .query(&mut redis_pool.client()?.connection()?)
        .unwrap();

    Ok(())
}
