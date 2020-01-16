use std::sync::Arc;

use actix::prelude::*;

use crate::actors::project::GetProjectStatesResponse;
use crate::redis::{RedisError, RedisPool};
use crate::utils::ErrorBoundary;

use relay_common::ProjectId;
use relay_config::Config;

pub struct RedisProjectCache {
    config: Arc<Config>,
    redis: RedisPool,
}

impl RedisProjectCache {
    pub fn new(config: Arc<Config>, redis: RedisPool) -> Self {
        RedisProjectCache { config, redis }
    }
}

impl Actor for RedisProjectCache {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        log::info!("redis project cache started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("redis project cache stopped");
    }
}

pub struct GetProjectStatesFromRedis {
    pub projects: Vec<ProjectId>,
}

impl Message for GetProjectStatesFromRedis {
    type Result = Result<GetProjectStatesResponse, RedisError>;
}

impl Handler<GetProjectStatesFromRedis> for RedisProjectCache {
    type Result = Result<GetProjectStatesResponse, RedisError>;

    fn handle(
        &mut self,
        request: GetProjectStatesFromRedis,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mut command = redis::cmd("MGET");
        for id in &request.projects {
            command.arg(format!(
                "{}:{}",
                self.config.projectconfig_cache_prefix(),
                id
            ));
        }

        let raw_response: Vec<String> = match self.redis {
            RedisPool::Cluster(ref pool) => {
                let mut client = pool.get().map_err(RedisError::RedisPool)?;
                command.query(&mut *client).map_err(RedisError::Redis)?
            }
            RedisPool::Single(ref pool) => {
                let mut client = pool.get().map_err(RedisError::RedisPool)?;
                command.query(&mut *client).map_err(RedisError::Redis)?
            }
        };

        let parsed_response =
            raw_response
                .into_iter()
                .map(|json| match serde_json::from_str(&json) {
                    Ok(project_state) => ErrorBoundary::Ok(project_state),
                    Err(err) => ErrorBoundary::Err(Box::new(err)),
                });

        Ok(GetProjectStatesResponse {
            configs: request
                .projects
                .iter()
                .cloned()
                .zip(parsed_response)
                .collect(),
        })
    }
}
