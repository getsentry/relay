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

impl CardinalityScriptResult {
    pub fn validate(&self, num_hashes: usize) -> Result<()> {
        if num_hashes == self.statuses.len() {
            return Ok(());
        }

        Err(relay_redis::RedisError::Redis(redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            "Script returned an invalid number of elements",
            format!("Expected {num_hashes} results, got {}", self.statuses.len()),
        )))
        .into())
    }
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

    pub fn pipe(&self) -> CardinalityScriptPipeline<'_> {
        CardinalityScriptPipeline {
            script: self,
            pipe: redis::pipe(),
        }
    }

    fn load_redis(&self, con: &mut Connection) -> Result<()> {
        self.0
            .prepare_invoke()
            .load(con)
            .map_err(relay_redis::RedisError::Redis)?;

        Ok(())
    }

    fn prepare_invocation(
        &self,
        limit: u64,
        expire: u64,
        hashes: impl Iterator<Item = u32>,
        keys: impl Iterator<Item = String>,
    ) -> redis::ScriptInvocation<'_> {
        let mut invocation = self.0.prepare_invoke();

        for key in keys {
            invocation.key(key);
        }

        invocation.arg(limit);
        invocation.arg(expire);

        for hash in hashes {
            invocation.arg(&hash.to_le_bytes());
        }

        invocation
    }
}

pub struct CardinalityScriptPipeline<'a> {
    script: &'a CardinalityScript,
    pipe: redis::Pipeline,
}

impl<'a> CardinalityScriptPipeline<'a> {
    pub fn add_invocation(
        &mut self,
        limit: u64,
        expire: u64,
        hashes: impl Iterator<Item = u32>,
        keys: impl Iterator<Item = String>,
    ) -> &mut Self {
        let invocation = self.script.prepare_invocation(limit, expire, hashes, keys);
        self.pipe.script(invocation);
        self
    }

    pub fn invoke(&self, con: &mut Connection<'_>) -> Result<Vec<CardinalityScriptResult>> {
        match self.pipe.query(con) {
            Ok(result) => Ok(result),
            Err(err) if err.kind() == redis::ErrorKind::NoScriptError => {
                relay_log::trace!("Redis script no loaded, loading it now");
                self.script.load_redis(con)?;
                self.pipe
                    .query(con)
                    .map_err(relay_redis::RedisError::Redis)
                    .map_err(Into::into)
            }
            Err(err) => Err(relay_redis::RedisError::Redis(err).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_redis::{RedisConfigOptions, RedisPool};
    use uuid::Uuid;

    use super::*;

    impl CardinalityScript {
        fn invoke_one(
            &self,
            con: &mut Connection,
            limit: u64,
            expire: u64,
            hashes: impl Iterator<Item = u32>,
            keys: impl Iterator<Item = String>,
        ) -> Result<CardinalityScriptResult> {
            let mut results = self
                .pipe()
                .add_invocation(limit, expire, hashes, keys)
                .invoke(con)?;

            assert_eq!(results.len(), 1);
            Ok(results.pop().unwrap())
        }
    }

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
            .invoke_one(&mut connection, 50, 3600, 0..30, keys(prefix, k1))
            .unwrap();

        script
            .invoke_one(&mut connection, 50, 3600, 0..30, keys(prefix, k2))
            .unwrap();

        assert_ttls(&mut connection, prefix);
    }

    #[test]
    fn test_load_script() {
        let redis = build_redis();
        let mut client = redis.client().unwrap();
        let mut connection = client.connection().unwrap();

        let script = CardinalityScript::load();
        let keys = keys(Uuid::new_v4(), &["a", "b", "c"]);

        redis::cmd("SCRIPT").arg("FLUSH").execute(&mut connection);
        script
            .invoke_one(&mut connection, 50, 3600, 0..30, keys)
            .unwrap();
    }

    #[test]
    fn test_multiple_calls_in_pipeline() {
        let redis = build_redis();
        let mut client = redis.client().unwrap();
        let mut connection = client.connection().unwrap();

        let script = CardinalityScript::load();
        let k2 = keys(Uuid::new_v4(), &["a", "b", "c"]);
        let k1 = keys(Uuid::new_v4(), &["a", "b", "c"]);

        let mut pipeline = script.pipe();
        let results = pipeline
            .add_invocation(50, 3600, 0..30, k1)
            .add_invocation(50, 3600, 0..30, k2)
            .invoke(&mut connection)
            .unwrap();

        assert_eq!(results.len(), 2);
    }
}
