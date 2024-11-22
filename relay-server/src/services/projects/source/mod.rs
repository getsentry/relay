use relay_base_schema::project::ProjectKey;
#[cfg(feature = "processing")]
use relay_config::RedisConfigRef;
use relay_config::{Config, RedisPoolConfigs, RelayMode};
#[cfg(feature = "processing")]
use relay_redis::{AsyncRedisPool, RedisPool};
use relay_system::{Addr, ServiceRunner};
use std::convert::Infallible;
use std::sync::Arc;

pub mod local;
#[cfg(feature = "processing")]
pub mod redis;
pub mod upstream;

use crate::services::projects::project::{ProjectState, Revision};
#[cfg(feature = "processing")]
use crate::services::projects::source::redis::RedisProjectError;
use crate::services::upstream::UpstreamRelay;

use self::local::{LocalProjectSource, LocalProjectSourceService};
#[cfg(feature = "processing")]
use self::redis::RedisProjectSource;
use self::upstream::{UpstreamProjectSource, UpstreamProjectSourceService};

/// Helper type that contains all configured sources for project cache fetching.
#[derive(Clone, Debug)]
pub struct ProjectSource {
    config: Arc<Config>,
    local_source: Addr<LocalProjectSource>,
    upstream_source: Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<RedisProjectSource>,
}

impl ProjectSource {
    /// Starts all project source services in the given `ServiceRunner`.
    pub async fn start_in(
        runner: &mut ServiceRunner,
        config: Arc<Config>,
        upstream_relay: Addr<UpstreamRelay>,
        _redis: Option<RedisPool>,
    ) -> Self {
        let local_source = runner.start(LocalProjectSourceService::new(config.clone()));
        let upstream_source = runner.start(UpstreamProjectSourceService::new(
            config.clone(),
            upstream_relay,
        ));

        #[cfg(feature = "processing")]
        let redis_source = if let Some(redis_config) = config.redis() {
            Self::pool_config(redis_config)
                .await
                .ok()
                .map(|client| RedisProjectSource::new(config.clone(), client))
        } else {
            None
        };

        Self {
            config,
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
        }
    }

    #[cfg(feature = "processing")]
    async fn pool_config(
        config: RedisPoolConfigs<'_>,
    ) -> Result<AsyncRedisPool, RedisProjectError> {
        match config {
            RedisPoolConfigs::Unified(config) => Self::create_async_pool(config).await,
            RedisPoolConfigs::Individual {
                project_configs, ..
            } => Self::create_async_pool(project_configs).await,
        }
    }

    #[cfg(feature = "processing")]
    async fn create_async_pool(
        config: RedisConfigRef<'_>,
    ) -> Result<AsyncRedisPool, RedisProjectError> {
        match config {
            RedisConfigRef::Cluster {
                cluster_nodes,
                options,
            } => AsyncRedisPool::cluster(cluster_nodes.iter().map(|s| s.as_str()), &options)
                .await
                .map_err(RedisProjectError::Redis),
            RedisConfigRef::Single { server, options } => {
                AsyncRedisPool::single(server.as_str(), &options)
                    .await
                    .map_err(RedisProjectError::Redis)
            }
            _ => Err(RedisProjectError::MultiWriteNotSupported),
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
        let state_opt = self
            .local_source
            .send(FetchOptionalProjectState { project_key })
            .await?;

        if let Some(state) = state_opt {
            return Ok(state.into());
        }

        match self.config.relay_mode() {
            RelayMode::Proxy => return Ok(ProjectState::new_allowed().into()),
            RelayMode::Static => return Ok(ProjectState::Disabled.into()),
            RelayMode::Capture => return Ok(ProjectState::new_allowed().into()),
            RelayMode::Managed => (), // Proceed with loading the config from redis or upstream
        }

        #[cfg(feature = "processing")]
        if let Some(redis_source) = self.redis_source {
            let current_revision = current_revision.clone();

            // let redis_permit = self.redis_semaphore.acquire().await?;
            // let state_fetch_result = tokio::task::spawn_blocking(move || {
            //     redis_source.get_config_if_changed(project_key, current_revision)
            // }).await?;
            let state_fetch_result = redis_source
                .get_config_if_changed(project_key, current_revision)
                .await;
            // drop(redis_permit);

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
