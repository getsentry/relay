use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_redis::{AsyncRedisPool, RedisError};
use relay_statsd::metric;
use std::fmt::Debug;
use std::sync::Arc;

use crate::services::projects::project::{ParsedProjectState, ProjectState, Revision};
use crate::services::projects::source::SourceProjectState;
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use relay_redis::redis::cmd;

#[derive(Clone, Debug)]
pub struct RedisProjectSource {
    config: Arc<Config>,
    redis: AsyncRedisPool,
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
    pub fn new(config: Arc<Config>, redis: AsyncRedisPool) -> Self {
        RedisProjectSource { config, redis }
    }

    /// Fetches a project config from Redis.
    ///
    /// The returned project state is [`ProjectState::Pending`] if the requested project config is not
    /// stored in Redis.
    pub async fn get_config_if_changed(
        &self,
        key: ProjectKey,
        revision: Revision,
    ) -> Result<SourceProjectState, RedisProjectError> {
        let mut connection = self.redis.get_connection().await?;
        // Only check for the revision if we were passed a revision.
        if let Some(revision) = revision.as_str() {
            let current_revision: Option<String> = cmd("GET")
                .arg(self.get_redis_rev_key(key))
                .query_async(&mut connection)
                .await
                .map_err(RedisError::Redis)?;

            relay_log::trace!(
                "Redis revision {current_revision:?}, requested revision {revision:?}"
            );
            if current_revision.as_deref() == Some(revision) {
                metric!(
                    counter(RelayCounters::ProjectStateRedis) += 1,
                    hit = "revision",
                );
                return Ok(SourceProjectState::NotModified);
            }
        }

        let raw_response_opt: Option<Vec<u8>> = cmd("GET")
            .arg(self.get_redis_project_config_key(key))
            .query_async(&mut connection)
            .await
            .map_err(RedisError::Redis)?;

        let Some(response) = raw_response_opt else {
            metric!(
                counter(RelayCounters::ProjectStateRedis) += 1,
                hit = "false"
            );
            return Ok(SourceProjectState::New(ProjectState::Pending));
        };

        let response = ProjectState::from(parse_redis_response(response.as_slice())?);

        // If we were passed a revision, check if we just loaded the same revision from Redis.
        //
        // We always want to keep the old revision alive if possible, since the already loaded
        // version has already initialized caches.
        //
        // While this is theoretically possible this should always been handled using the above revision
        // check using the additional Redis key.
        if response.revision() == revision {
            metric!(
                counter(RelayCounters::ProjectStateRedis) += 1,
                hit = "project_config_revision"
            );
            Ok(SourceProjectState::NotModified)
        } else {
            metric!(
                counter(RelayCounters::ProjectStateRedis) += 1,
                hit = "project_config"
            );
            Ok(SourceProjectState::New(response))
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
