use relay_redis::{
    AsyncRedisConnection, RedisScripts,
    redis::{self, FromRedisValue, Script},
};

use crate::Result;

/// Status wether an entry/bucket is accepted or rejected by the cardinality limiter.
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// Item is rejected.
    Rejected,
    /// Item is accepted.
    Accepted,
}

impl Status {
    /// Returns `true` if the status is [`Status::Rejected`].
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

/// Result returned from [`CardinalityScript`].
#[derive(Debug)]
pub struct CardinalityScriptResult {
    /// Cardinality of the limit.
    pub cardinality: u32,
    /// Status for each hash passed to the script.
    pub statuses: Vec<Status>,
}

impl CardinalityScriptResult {
    /// Validates the result against the amount of hashes originally supplied.
    ///
    /// This is not necessarily required but recommended.
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
                format!("{v:?}"),
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

/// Abstraction over the `cardinality.lua` lua Redis script.
pub struct CardinalityScript(&'static Script);

impl CardinalityScript {
    /// Loads the script.
    ///
    /// This is somewhat costly and shouldn't be done often.
    pub fn load() -> Self {
        Self(RedisScripts::load_cardinality())
    }

    /// Creates a new pipeline to batch multiple script invocations.
    pub fn pipe(&self) -> CardinalityScriptPipeline<'_> {
        CardinalityScriptPipeline {
            script: self,
            pipe: redis::pipe(),
        }
    }

    /// Makes sure the script is loaded in Redis.
    async fn load_redis(&self, con: &mut AsyncRedisConnection) -> Result<()> {
        self.0
            .prepare_invoke()
            .load_async(con)
            .await
            .map_err(relay_redis::RedisError::Redis)?;

        Ok(())
    }

    /// Returns a [`redis::ScriptInvocation`] with all keys and arguments prepared.
    fn prepare_invocation(
        &self,
        limit: u32,
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

/// Pipeline to batch multiple [`CardinalityScript`] invocations.
pub struct CardinalityScriptPipeline<'a> {
    script: &'a CardinalityScript,
    pipe: redis::Pipeline,
}

impl CardinalityScriptPipeline<'_> {
    /// Adds another invocation of the script to the pipeline.
    pub fn add_invocation(
        &mut self,
        limit: u32,
        expire: u64,
        hashes: impl Iterator<Item = u32>,
        keys: impl Iterator<Item = String>,
    ) -> &mut Self {
        let invocation = self.script.prepare_invocation(limit, expire, hashes, keys);
        self.pipe.invoke_script(&invocation);
        self
    }

    /// Invokes the entire pipeline and returns the results.
    ///
    /// Returns one result for each script invocation.
    pub async fn invoke(
        &self,
        con: &mut AsyncRedisConnection,
    ) -> Result<Vec<CardinalityScriptResult>> {
        match self.pipe.query_async(con).await {
            Ok(result) => Ok(result),
            Err(err) if err.kind() == redis::ErrorKind::NoScriptError => {
                relay_log::trace!("Redis script no loaded, loading it now");
                self.script.load_redis(con).await?;
                self.pipe
                    .query_async(con)
                    .await
                    .map_err(relay_redis::RedisError::Redis)
                    .map_err(Into::into)
            }
            Err(err) => Err(relay_redis::RedisError::Redis(err).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_redis::{AsyncRedisClient, RedisConfigOptions};
    use uuid::Uuid;

    use super::*;

    impl CardinalityScript {
        async fn invoke_one(
            &self,
            con: &mut AsyncRedisConnection,
            limit: u32,
            expire: u64,
            hashes: impl Iterator<Item = u32>,
            keys: impl Iterator<Item = String>,
        ) -> Result<CardinalityScriptResult> {
            let mut results = self
                .pipe()
                .add_invocation(limit, expire, hashes, keys)
                .invoke(con)
                .await?;

            assert_eq!(results.len(), 1);
            Ok(results.pop().unwrap())
        }
    }

    fn build_redis_client() -> AsyncRedisClient {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        let opts = RedisConfigOptions {
            max_connections: 1,
            ..Default::default()
        };
        AsyncRedisClient::single(&url, &opts).unwrap()
    }

    fn keys(prefix: Uuid, keys: &[&str]) -> impl Iterator<Item = String> {
        keys.iter()
            .map(move |key| format!("{prefix}-{key}"))
            .collect::<Vec<_>>()
            .into_iter()
    }

    async fn assert_ttls(connection: &mut AsyncRedisConnection, prefix: Uuid) {
        let keys = redis::cmd("KEYS")
            .arg(format!("{prefix}-*"))
            .query_async::<Vec<String>>(connection)
            .await
            .unwrap();

        for key in keys {
            let ttl = redis::cmd("TTL")
                .arg(&key)
                .query_async::<i64>(connection)
                .await
                .unwrap();

            assert!(ttl >= 0, "Key {key} has no TTL");
        }
    }

    #[tokio::test]
    async fn test_below_limit_perfect_cardinality_ttl() {
        let client = build_redis_client();
        let mut connection = client.get_connection().await.unwrap();

        let script = CardinalityScript::load();

        let prefix = Uuid::new_v4();
        let k1 = &["a", "b", "c"];
        let k2 = &["b", "c", "d"];

        script
            .invoke_one(&mut connection, 50, 3600, 0..30, keys(prefix, k1))
            .await
            .unwrap();

        script
            .invoke_one(&mut connection, 50, 3600, 0..30, keys(prefix, k2))
            .await
            .unwrap();

        assert_ttls(&mut connection, prefix).await;
    }

    #[tokio::test]
    async fn test_load_script() {
        let client = build_redis_client();
        let mut connection = client.get_connection().await.unwrap();

        let script = CardinalityScript::load();
        let keys = keys(Uuid::new_v4(), &["a", "b", "c"]);

        redis::cmd("SCRIPT")
            .arg("FLUSH")
            .exec_async(&mut connection)
            .await
            .unwrap();
        script
            .invoke_one(&mut connection, 50, 3600, 0..30, keys)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_multiple_calls_in_pipeline() {
        let client = build_redis_client();
        let mut connection = client.get_connection().await.unwrap();

        let script = CardinalityScript::load();
        let k2 = keys(Uuid::new_v4(), &["a", "b", "c"]);
        let k1 = keys(Uuid::new_v4(), &["a", "b", "c"]);

        let mut pipeline = script.pipe();
        let results = pipeline
            .add_invocation(50, 3600, 0..30, k1)
            .add_invocation(50, 3600, 0..30, k2)
            .invoke(&mut connection)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
    }
}
