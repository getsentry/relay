use relay_base_schema::project::ProjectKey;
use relay_redis::{RedisError, RedisPool};
use relay_sampling::config::RuleId;
use relay_system::Addr;

use crate::actors::upstream::UpstreamRelay;

/// Sentry gotta the know the limit is up, then it will change the sampling config
pub fn thisfunctionshouldnotifysentrythatwehavereachedthereservoirlimit(
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

pub fn do_the_stuff(
    redis: RedisPool,
    reservoir_limit: usize,
    upstream: Addr<UpstreamRelay>,
    project_key: ProjectKey,
    rule_id: RuleId,
) {
    let key = BiasRedisKey::new(&project_key, rule_id);
    // 1. update count in relay
    // 2. ask for total count
    // 3. if total count exceeds limit, signal to sentry to remove the bias

    increment_bias_rule_count(&redis, &key).unwrap();
    let total_count = get_bias_rule_count(&redis, &key).unwrap().unwrap();
    if total_count as usize >= reservoir_limit {
        delete_bias_rule(&redis, &key).unwrap();
        thisfunctionshouldnotifysentrythatwehavereachedthereservoirlimit(
            upstream,
            &project_key,
            rule_id,
        );
    }
}

fn get_bias_rule_count(redis_pool: &RedisPool, key: &BiasRedisKey) -> anyhow::Result<Option<i64>> {
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

    Ok(response)
}

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
