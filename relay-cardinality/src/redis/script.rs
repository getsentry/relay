use relay_redis::{
    redis::{self, FromRedisValue, Script},
    Connection,
};

use crate::Result;

pub struct CardinalityScript(Script);

/// Status wether an entry/bucket is accepted or rejected by the cardinality limiter.
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// Item is rejected.
    Rejected,
    /// Item is accepted.
    Accepted,
}

impl Status {
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }
}

impl FromRedisValue for Status {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let accepted = bool::from_redis_value(v)?;
        Ok(if accepted {
            Self::Accepted
        } else {
            Self::Rejected
        })
    }
}

#[derive(Debug)]
pub struct CardinalityScriptResult {
    pub cardinality: u64,
    pub statuses: Vec<Status>,
}

impl FromRedisValue for CardinalityScriptResult {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let Some(seq) = v.as_sequence() else {
            return Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Expected a sequence from the cardinality script",
            )));
        };

        let mut iter = seq.iter();

        let cardinality = iter
            .next()
            .ok_or_else(|| {
                redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected cardinality as the first result from the cardinality script",
                ))
            })
            .and_then(FromRedisValue::from_redis_value)?;

        let mut statuses = Vec::with_capacity(iter.len());
        for value in iter {
            statuses.push(Status::from_redis_value(value)?);
        }

        Ok(Self {
            cardinality,
            statuses,
        })
    }
}

impl CardinalityScript {
    pub fn load() -> Self {
        Self(Script::new(include_str!("cardinality.lua")))
    }

    pub fn invoke(
        &self,
        con: &mut Connection,
        limit: u64,
        expire: u64,
        hashes: impl Iterator<Item = u32>,
        keys: impl Iterator<Item = String>,
    ) -> Result<CardinalityScriptResult> {
        let mut invocation = self.0.prepare_invoke();

        for key in keys {
            invocation.key(key);
        }

        invocation.arg(limit);
        invocation.arg(expire);

        let mut num_hashes = 0;
        for hash in hashes {
            invocation.arg(&hash.to_le_bytes());
            num_hashes += 1;
        }

        let result: CardinalityScriptResult = invocation
            .invoke(con)
            .map_err(relay_redis::RedisError::Redis)?;

        if num_hashes != result.statuses.len() {
            return Err(relay_redis::RedisError::Redis(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Script returned an invalid number of elements",
                format!(
                    "Expected {num_hashes} results, got {}",
                    result.statuses.len()
                ),
            )))
            .into());
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use relay_redis::{RedisConfigOptions, RedisPool};
    use uuid::Uuid;

    use super::*;

    fn build_redis() -> RedisPool {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        let opts = RedisConfigOptions {
            max_connections: 1,
            ..Default::default()
        };
        RedisPool::single(&url, opts).unwrap()
    }

    fn keys(prefix: Uuid, keys: &[&str]) -> impl Iterator<Item = String> {
        keys.iter()
            .map(move |key| format!("{prefix}-{key}"))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn assert_ttls(connection: &mut Connection, prefix: Uuid) {
        let keys = redis::cmd("KEYS")
            .arg(format!("{prefix}-*"))
            .query::<Vec<String>>(connection)
            .unwrap();

        for key in keys {
            let ttl = redis::cmd("TTL")
                .arg(&key)
                .query::<i64>(connection)
                .unwrap();

            assert!(ttl >= 0, "Key {key} has no TTL");
        }
    }

    #[test]
    fn test_below_limit_perfect_cardinality_ttl() {
        let redis = build_redis();
        let mut client = redis.client().unwrap();
        let mut connection = client.connection().unwrap();

        let script = CardinalityScript::load();

        let prefix = Uuid::new_v4();
        let k1 = &["a", "b", "c"];
        let k2 = &["b", "c", "d"];

        script
            .invoke(&mut connection, 50, 3600, 0..30, keys(prefix, k1))
            .unwrap();

        script
            .invoke(&mut connection, 50, 3600, 0..30, keys(prefix, k2))
            .unwrap();

        assert_ttls(&mut connection, prefix);
    }
}
