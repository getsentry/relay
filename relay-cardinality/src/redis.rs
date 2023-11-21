use std::collections::HashSet;

use itertools::Itertools;
use relay_redis::{
    redis::{self, ToRedisArgs},
    Connection, RedisPool,
};

use crate::{limiter::Entry, Limiter, Result, Scope};

// TODO: this should be in the config
const REDIS_SET_PREFIX: &str = "relay:cardinality";
const CARDINALITY_LIMIT: usize = 10000;

/// Implementation uses Redis sets to keep track of cardinality.
pub struct RedisSetLimiter {
    redis: RedisPool,
}

/// A Redis based limiter using Redis sets to track cardinality and membership.
impl RedisSetLimiter {
    /// Creates a new [`RedisSetLimiter`].
    pub fn new(redis: RedisPool) -> Self {
        Self { redis }
    }
}

impl RedisSetLimiter {
    /// Checks the limits for a specific scope.
    ///
    /// Returns an iterator over all entries which have been accepted.
    fn check_limits(
        &self,
        client: &mut RedisHelper,
        scope: Scope,
        mut entries: Vec<Entry>,
    ) -> Result<impl Iterator<Item = Entry>> {
        // TODO: benchmark ahash for these sets

        let set_key = self.to_redis_key(&scope);
        let set = client.read_set(&set_key)?;
        let mut new_hashes = HashSet::new();

        let mut new_cardinality = set.len();

        for entry in &mut entries {
            if set.contains(&entry.hash) {
                entry.accept();
            } else if new_cardinality < CARDINALITY_LIMIT {
                entry.accept();
                new_hashes.insert(entry.hash);
                new_cardinality += 1;
            } else {
                entry.reject();
                // Cardinality limit hit
                // TODO: metrics, errors, logs, whatever
            }
        }

        // TODO: metrics etc.
        client.add_to_set(&set_key, new_hashes);

        Ok(entries.into_iter().filter(|entry| entry.is_accepted()))
    }

    fn to_redis_key(&self, scope: &Scope) -> String {
        // Pattern match here, to not forget to update the key, when modifying the scope.
        let Scope {
            organization_id,
            namespace,
        } = scope;

        let prefix = REDIS_SET_PREFIX; // TODO: make this configurable
        format!("{prefix}:scope-{organization_id}-{namespace}",)
    }
}

impl Limiter for RedisSetLimiter {
    fn check_cardinality_limits<I>(
        &self,
        organization_id: u64,
        entries: I,
    ) -> Result<Box<dyn Iterator<Item = Entry>>>
    where
        I: IntoIterator<Item = Entry>,
    {
        let entries = entries.into_iter().into_group_map_by(|f| Scope {
            organization_id,
            namespace: f.namespace,
        });

        let mut client = self.redis.client()?;
        let mut client = RedisHelper(client.connection()?);

        let mut accepted = Vec::new();
        for (scope, entries) in entries {
            // TODO: retry on redis error?
            accepted.push(self.check_limits(&mut client, scope, entries)?);
        }

        // Check if metric/hash is already in set -> it can pass
        // Check how much cardinality
        Ok(Box::new(accepted.into_iter().flatten()))
    }
}

/// Internal Redis client wrapper to abstract the issuing of commands.
struct RedisHelper<'a>(Connection<'a>);

impl<'a> RedisHelper<'a> {
    fn read_set(&mut self, name: &str) -> Result<HashSet<u32>> {
        let result = redis::cmd("SMEMBERS")
            .arg(name)
            .query(&mut self.0)
            .map_err(relay_redis::RedisError::Redis)?;

        Ok(result)
    }

    fn add_to_set<I: ToRedisArgs>(&mut self, name: &str, values: impl IntoIterator<Item = I>) {
        let mut cmd = redis::cmd("SADD");
        cmd.arg(name);

        // TODO: sliding window and expiry
        // TODO: batch values
        for arg in values {
            cmd.arg(arg);
        }

        cmd.execute(&mut self.0)
    }
}
