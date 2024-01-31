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
