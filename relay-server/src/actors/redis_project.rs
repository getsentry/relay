use actix::prelude::*;

use redis::Commands;

use crate::actors::project::GetProjectStatesResponse;
use crate::redis::{RedisError, RedisPool};
use crate::utils::ErrorBoundary;

use relay_common::ProjectId;

pub struct RedisProjectCache {
    redis: RedisPool,
}

impl RedisProjectCache {
    pub fn new(redis: RedisPool) -> Self {
        RedisProjectCache { redis }
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
        let keys: Vec<_> = request
            .projects
            .iter()
            .map(|id| format!("relayconfig:{}", id))
            .collect();
        let raw_response: Vec<String> = match self.redis {
            RedisPool::Cluster(ref pool) => {
                let mut client = pool.get().map_err(RedisError::RedisPool)?;
                client.get(&keys[..]).map_err(RedisError::Redis)?
            }
            RedisPool::Single(ref pool) => {
                let mut client = pool.get().map_err(RedisError::RedisPool)?;
                client.get(&keys[..]).map_err(RedisError::Redis)?
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
