use std::sync::Arc;

use actix::prelude::*;
use failure::Fail;
use relay_common::ProjectKey;
use relay_config::Config;
use relay_log::LogError;
use relay_redis::{RedisError, RedisPool};

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;

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

impl RedisProjectSource {
    pub fn new(config: Arc<Config>, redis: RedisPool) -> Self {
        RedisProjectSource { config, redis }
    }

    fn get_config(&self, key: ProjectKey) -> Result<Option<ProjectState>, RedisProjectError> {
        let mut command = relay_redis::redis::cmd("GET");

        let prefix = self.config.projectconfig_cache_prefix();
        command.arg(format!("{}:{}", prefix, key));

        let raw_response_opt: Option<String> = command
            .query(&mut self.redis.client()?.connection())
            .map_err(RedisError::Redis)?;

        let raw_response = match raw_response_opt {
            Some(response) => response,
            None => return Ok(None),
        };

        Ok(serde_json::from_str(&raw_response)?)
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
        match self.get_config(message.public_key) {
            Ok(x) => x.map(ProjectState::sanitize).map(Arc::new),
            Err(e) => {
                relay_log::error!("Failed to fetch project from Redis: {}", LogError(&e));
                None
            }
        }
    }
}
