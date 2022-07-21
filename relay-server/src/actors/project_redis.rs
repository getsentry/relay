use std::borrow::Cow;
use std::sync::Arc;

use actix::prelude::*;
use failure::Fail;
use relay_common::ProjectKey;
use relay_config::Config;
use relay_log::LogError;
use relay_redis::{RedisError, RedisPool};
use relay_statsd::metric;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;
use crate::statsd::RelayCounters;

pub struct RedisProjectSource {
    config: Arc<Config>,
    redis: RedisPool,
}

#[derive(Debug, Fail)]
enum RedisProjectError {
    #[fail(display = "failed to parse projectconfig from redis")]
    Parsing(#[cause] serde_json::Error),

    #[fail(display = "failed to talk to redis")]
    Redis(#[cause] RedisError),
}

impl From<RedisError> for RedisProjectError {
    fn from(e: RedisError) -> RedisProjectError {
        RedisProjectError::Redis(e)
    }
}

impl From<serde_json::Error> for RedisProjectError {
    fn from(e: serde_json::Error) -> RedisProjectError {
        RedisProjectError::Parsing(e)
    }
}

fn parse_redis_response(raw_response: &[u8]) -> Result<ProjectState, RedisProjectError> {
    let decoded = zstd::decode_all(raw_response);

    let raw_response: Cow<[u8]> = match decoded {
        Ok(decoded) => decoded.into(),
        // If decoding fails, assume uncompressed payload and try again
        Err(e) => {
            dbg!(e);
            raw_response.into()
        }
    };

    Ok(serde_json::from_slice(raw_response.as_ref())?)
}

impl RedisProjectSource {
    pub fn new(config: Arc<Config>, redis: RedisPool) -> Self {
        RedisProjectSource { config, redis }
    }

    fn get_config(&self, key: ProjectKey) -> Result<Option<ProjectState>, RedisProjectError> {
        let mut command = relay_redis::redis::cmd("GET");

        let prefix = self.config.projectconfig_cache_prefix();
        command.arg(format!("{}:{}", prefix, key));

        let raw_response_opt: Option<Vec<u8>> = command
            .query(&mut self.redis.client()?.connection())
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

impl Actor for RedisProjectSource {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("redis project cache started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("redis project cache stopped");
    }
}

impl Handler<FetchOptionalProjectState> for RedisProjectSource {
    type Result = Option<Arc<ProjectState>>;

    fn handle(
        &mut self,
        message: FetchOptionalProjectState,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        match self.get_config(message.project_key()) {
            Ok(x) => x.map(ProjectState::sanitize).map(Arc::new),
            Err(e) => {
                relay_log::error!("Failed to fetch project from Redis: {}", LogError(&e));
                None
            }
        }
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
        assert!(result.is_ok(), "{:?}", result);
    }
}
