use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_redis::{RedisError, RedisPool};
use relay_statsd::metric;

use crate::services::project::{ParsedProjectState, ProjectState};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};

#[derive(Debug, Clone)]
pub struct RedisProjectSource {
    config: Arc<Config>,
    redis: RedisPool,
}

#[derive(Debug, thiserror::Error)]
pub enum RedisProjectError {
    #[error("failed to parse projectconfig from redis")]
    Parsing(#[from] serde_json::Error),

    #[error("failed to talk to redis")]
    Redis(#[from] RedisError),
}

fn parse_redis_response(raw_response: &[u8]) -> Result<ParsedProjectState, RedisProjectError> {
    let decompression_result = metric!(timer(RelayTimers::ProjectStateDecompression), {
        zstd::decode_all(raw_response)
    });

    let decoded_response = match &decompression_result {
        Ok(decoded) => {
            metric!(
                histogram(RelayHistograms::ProjectStateSizeBytesCompressed) =
                    raw_response.len() as f64
            );
            metric!(
                histogram(RelayHistograms::ProjectStateSizeBytesDecompressed) =
                    decoded.len() as f64
            );
            decoded.as_slice()
        }
        // If decoding fails, assume uncompressed payload and try again
        Err(_) => raw_response,
    };

    Ok(serde_json::from_slice(decoded_response)?)
}

impl RedisProjectSource {
    pub fn new(config: Arc<Config>, redis: RedisPool) -> Self {
        RedisProjectSource { config, redis }
    }

    /// Fetches a project config from Redis.
    ///
    /// Returns `None` if the the project config stored in Redis has the same `revision`.
    /// Always returns a project state if the passed `revision` is `None`.
    ///
    /// The returned project state is [`ProjectState::Pending`] if the requested project config is not
    /// stored in Redis.
    pub fn get_config_if_changed(
        &self,
        key: ProjectKey,
        revision: Option<&str>,
    ) -> Result<Option<ProjectState>, RedisProjectError> {
        let mut client = self.redis.client()?;
        let mut connection = client.connection()?;

        // Only check for the revision if we were passed a revision.
        if let Some(revision) = revision {
            let current_revision: Option<String> = relay_redis::redis::cmd("GET")
                .arg(self.get_redis_rev_key(key))
                .query(&mut connection)
                .map_err(RedisError::Redis)?;

            relay_log::trace!(
                "Redis revision {current_revision:?}, requested revision {revision:?}"
            );
            if current_revision.as_deref() == Some(revision) {
                metric!(
                    counter(RelayCounters::ProjectStateRedis) += 1,
                    hit = "revision",
                );
                return Ok(None);
            }
        }

        let raw_response_opt: Option<Vec<u8>> = relay_redis::redis::cmd("GET")
            .arg(self.get_redis_project_config_key(key))
            .query(&mut connection)
            .map_err(RedisError::Redis)?;

        let Some(response) = raw_response_opt else {
            metric!(
                counter(RelayCounters::ProjectStateRedis) += 1,
                hit = "false"
            );
            return Ok(Some(ProjectState::Pending));
        };

        let response = ProjectState::from(parse_redis_response(response.as_slice())?);

        // If we were passed a revision, check if we just loaded the same revision from Redis.
        //
        // We always want to keep the old revision alive if possible, since the already loaded
        // version has already initialized caches.
        //
        // While this is theoretically possible this should always been handled using the above revision
        // check using the additional Redis key.
        if revision.is_some() && response.revision() == revision {
            metric!(
                counter(RelayCounters::ProjectStateRedis) += 1,
                hit = "project_config_revision"
            );
            Ok(None)
        } else {
            metric!(
                counter(RelayCounters::ProjectStateRedis) += 1,
                hit = "project_config"
            );
            Ok(Some(response))
        }
    }

    fn get_redis_project_config_key(&self, key: ProjectKey) -> String {
        let prefix = self.config.projectconfig_cache_prefix();
        format!("{prefix}:{key}")
    }

    fn get_redis_rev_key(&self, key: ProjectKey) -> String {
        let prefix = self.config.projectconfig_cache_prefix();
        format!("{prefix}:{key}.rev")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_redis_response() {
        let raw_response = b"{}";
        let result = parse_redis_response(raw_response);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_redis_response_compressed() {
        let raw_response = b"(\xb5/\xfd \x02\x11\x00\x00{}"; // As dumped by python zstandard library
        let result = parse_redis_response(raw_response);
        assert!(result.is_ok(), "{result:?}");
    }
}
