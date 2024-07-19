use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_redis::{RedisError, RedisPool};
use relay_statsd::metric;

use crate::services::project::ProjectState;
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

fn parse_redis_response(raw_response: &[u8]) -> Result<ProjectState, RedisProjectError> {
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

    pub fn get_config(&self, key: ProjectKey) -> Result<Option<ProjectState>, RedisProjectError> {
        let mut command = relay_redis::redis::cmd("GET");

        let prefix = self.config.projectconfig_cache_prefix();
        command.arg(format!("{prefix}:{key}"));

        let raw_response_opt: Option<Vec<u8>> = command
            .query(&mut self.redis.client()?.connection()?)
            .map_err(RedisError::Redis)?;

        let response = match raw_response_opt {
            Some(response) => {
                metric!(counter(RelayCounters::ProjectStateRedis) += 1, hit = "true");
                Some(parse_redis_response(response.as_slice())?)
            }
            None => {
                metric!(
                    counter(RelayCounters::ProjectStateRedis) += 1,
                    hit = "false"
                );
                None
            }
        };

        Ok(response)
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
