use relay_base_schema::project::ProjectKey;
use relay_config::{Config, RelayMode};
#[cfg(feature = "processing")]
use relay_redis::RedisClients;
use relay_system::{Addr, ServiceSpawn, ServiceSpawnExt as _};
use std::convert::Infallible;
use std::sync::Arc;

pub mod local;
#[cfg(feature = "processing")]
pub mod redis;
pub mod upstream;

use crate::services::projects::project::{ProjectState, Revision};
use crate::services::upstream::UpstreamRelay;

use self::local::{LocalProjectSource, LocalProjectSourceService};
#[cfg(feature = "processing")]
use self::redis::RedisProjectSource;
use self::upstream::{UpstreamProjectSource, UpstreamProjectSourceService};

/// Helper type that contains all configured sources for project cache fetching.
#[derive(Clone, Debug)]
pub struct ProjectSource {
    config: Arc<Config>,
    local_source: Option<Addr<LocalProjectSource>>,
    upstream_source: Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<RedisProjectSource>,
}

impl ProjectSource {
    /// Starts all project source services in the given [`ServiceSpawn`].
    pub async fn start_in(
        services: &dyn ServiceSpawn,
        config: Arc<Config>,
        upstream_relay: Addr<UpstreamRelay>,
        #[cfg(feature = "processing")] _redis: Option<RedisClients>,
    ) -> Self {
        let local_source = if config.relay_mode() == RelayMode::Static {
            Some(services.start(LocalProjectSourceService::new(config.clone())))
        } else {
            None
        };

        let upstream_source = services.start(UpstreamProjectSourceService::new(
            config.clone(),
            upstream_relay,
        ));

        #[cfg(feature = "processing")]
        let redis_source =
            _redis.map(|pool| RedisProjectSource::new(config.clone(), pool.project_configs));

        Self {
            config,
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
        }
    }

    /// Fetches a project with `project_key` from the configured sources.
    ///
    /// Returns a fully sanitized project.
    pub async fn fetch(
        self,
        project_key: ProjectKey,
        no_cache: bool,
        current_revision: Revision,
    ) -> Result<SourceProjectState, ProjectSourceError> {
        match self.config.relay_mode() {
            RelayMode::Proxy => return Ok(ProjectState::new_allowed().into()),
            RelayMode::Managed => (), // Proceed with loading the config from redis or upstream
            RelayMode::Static => {
                return Ok(match self.local_source {
                    Some(local) => local
                        .send(FetchOptionalProjectState { project_key })
                        .await?
                        .map_or(ProjectState::Disabled.into(), Into::into),
                    None => ProjectState::Disabled.into(),
                });
            }
        }

        #[cfg(feature = "processing")]
        if let Some(redis_source) = self.redis_source {
            let current_revision = current_revision.clone();

            let state_fetch_result = redis_source
                .get_config_if_changed(project_key, current_revision)
                .await;

            match state_fetch_result {
                // New state fetched from Redis, possibly pending.
                //
                // If it is pending, we must fallback to fetching from the upstream.
                Ok(SourceProjectState::New(state)) => {
                    let state = state.sanitized();
                    if !state.is_pending() {
                        return Ok(state.into());
                    }
                }
                // Redis reported that we're holding an up-to-date version of the state already,
                // refresh the state and return the old cached state again.
                Ok(SourceProjectState::NotModified) => return Ok(SourceProjectState::NotModified),
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn std::error::Error,
                        "failed to fetch project from Redis",
                    );
                }
            };
        };

        let state = self
            .upstream_source
            .send(FetchProjectState {
                project_key,
                current_revision,
                no_cache,
            })
            .await?;

        Ok(match state {
            SourceProjectState::New(state) => SourceProjectState::New(state.sanitized()),
            SourceProjectState::NotModified => SourceProjectState::NotModified,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProjectSourceError {
    #[error("redis permit error {0}")]
    RedisPermit(#[from] tokio::sync::AcquireError),
    #[error("redis join error {0}")]
    RedisJoin(#[from] tokio::task::JoinError),
    #[error("upstream error {0}")]
    Upstream(#[from] relay_system::SendError),
}

impl From<Infallible> for ProjectSourceError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

#[derive(Clone, Debug)]
pub struct FetchProjectState {
    /// The public key to fetch the project by.
    pub project_key: ProjectKey,

    /// Currently cached revision if available.
    ///
    /// The upstream is allowed to omit full project configs
    /// for requests for which the requester already has the most
    /// recent revision.
    pub current_revision: Revision,

    /// If true, all caches should be skipped and a fresh state should be computed.
    pub no_cache: bool,
}

#[derive(Clone, Debug)]
pub struct FetchOptionalProjectState {
    project_key: ProjectKey,
}

impl FetchOptionalProjectState {
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}

/// Response indicating whether a project state needs to be updated
/// or the upstream does not have a newer version.
#[derive(Debug, Clone)]
pub enum SourceProjectState {
    /// The upstream sent a [`ProjectState`].
    New(ProjectState),
    /// The upstream indicated that there is no newer version of the state available.
    NotModified,
}

impl From<ProjectState> for SourceProjectState {
    fn from(value: ProjectState) -> Self {
        Self::New(value)
    }
}
