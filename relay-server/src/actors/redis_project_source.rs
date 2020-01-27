use std::sync::Arc;

use actix::prelude::*;
use failure::Fail;
use relay_common::{LogError, ProjectId};
use relay_config::Config;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;
use crate::utils::{RedisError, RedisPool};

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

    fn get_config(&self, id: ProjectId) -> Result<Option<ProjectState>, RedisProjectError> {
        let mut command = redis::cmd("GET");
        command.arg(format!(
            "{}:{}",
            self.config.projectconfig_cache_prefix(),
            id
        ));

        let raw_response_opt: Option<String> = match self.redis {
            RedisPool::Cluster(ref pool) => {
                let mut client = pool.get().map_err(RedisError::RedisPool)?;
                command.query(&mut *client).map_err(RedisError::Redis)?
            }
            RedisPool::Single(ref pool) => {
                let mut client = pool.get().map_err(RedisError::RedisPool)?;
                command.query(&mut *client).map_err(RedisError::Redis)?
            }
        };

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
        log::info!("redis project cache started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("redis project cache stopped");
    }
}

impl Handler<FetchOptionalProjectState> for RedisProjectSource {
    type Result = Option<Arc<ProjectState>>;

    fn handle(
        &mut self,
        message: FetchOptionalProjectState,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        match self.get_config(message.id) {
            Ok(x) => x.map(Arc::new),
            Err(e) => {
                log::error!("Failed to fetch project from Redis: {}", LogError(&e));
                None
            }
        }
    }
}
