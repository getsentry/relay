use std::convert::Infallible;
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
#[cfg(feature = "processing")]
use relay_config::RedisConfigRef;
use relay_config::{Config, RelayMode};
use relay_redis::RedisPool;
use relay_system::{Addr, Service as _};
#[cfg(feature = "processing")]
use tokio::sync::Semaphore;

pub mod local;
#[cfg(feature = "processing")]
pub mod redis;
pub mod upstream;

use crate::services::projects::project::state::UpstreamProjectState;
use crate::services::projects::project::ProjectFetchState;
use crate::services::upstream::UpstreamRelay;

use self::local::{LocalProjectSource, LocalProjectSourceService};
#[cfg(feature = "processing")]
use self::redis::RedisProjectSource;
use self::upstream::{UpstreamProjectSource, UpstreamProjectSourceService};

/// Default value of maximum connections to Redis. This value was arbitrarily determined.
#[cfg(feature = "processing")]
const DEFAULT_REDIS_MAX_CONNECTIONS: u32 = 10;

/// Helper type that contains all configured sources for project cache fetching.
#[derive(Clone, Debug)]
pub struct ProjectSource {
    config: Arc<Config>,
    local_source: Addr<LocalProjectSource>,
    upstream_source: Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<RedisProjectSource>,
    #[cfg(feature = "processing")]
    redis_semaphore: Arc<Semaphore>,
}

impl ProjectSource {
    /// Starts all project source services in the current runtime.
    pub fn start(
        config: Arc<Config>,
        upstream_relay: Addr<UpstreamRelay>,
        _redis: Option<RedisPool>,
    ) -> Self {
        let (_, local_source) = LocalProjectSourceService::new(config.clone()).start();
        let (_, upstream_source) =
            UpstreamProjectSourceService::new(config.clone(), upstream_relay).start();

        #[cfg(feature = "processing")]
        let redis_max_connections = config
            .redis()
            .map(|configs| {
                let config = match configs {
                    relay_config::RedisPoolConfigs::Unified(config) => config,
                    relay_config::RedisPoolConfigs::Individual {
                        project_configs: config,
                        ..
                    } => config,
                };
                Self::compute_max_connections(config).unwrap_or(DEFAULT_REDIS_MAX_CONNECTIONS)
            })
            .unwrap_or(DEFAULT_REDIS_MAX_CONNECTIONS);
        #[cfg(feature = "processing")]
        let redis_source = _redis.map(|pool| RedisProjectSource::new(config.clone(), pool));

        Self {
            config,
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
            #[cfg(feature = "processing")]
            redis_semaphore: Arc::new(Semaphore::new(redis_max_connections.try_into().unwrap())),
        }
    }

    #[cfg(feature = "processing")]
    fn compute_max_connections(config: RedisConfigRef) -> Option<u32> {
        match config {
            RedisConfigRef::Cluster { options, .. } => Some(options.max_connections),
            RedisConfigRef::MultiWrite { configs } => configs
                .into_iter()
                .filter_map(|c| Self::compute_max_connections(c))
                .max(),
            RedisConfigRef::Single { options, .. } => Some(options.max_connections),
        }
    }

    pub async fn fetch(
        self,
        project_key: ProjectKey,
        no_cache: bool,
        cached_state: ProjectFetchState,
    ) -> Result<ProjectFetchState, ProjectSourceError> {
        let state_opt = self
            .local_source
            .send(FetchOptionalProjectState { project_key })
            .await?;

        if let Some(state) = state_opt {
            return Ok(ProjectFetchState::new(state));
        }

        match self.config.relay_mode() {
            RelayMode::Proxy => return Ok(ProjectFetchState::allowed()),
            RelayMode::Static => return Ok(ProjectFetchState::disabled()),
            RelayMode::Capture => return Ok(ProjectFetchState::allowed()),
            RelayMode::Managed => (), // Proceed with loading the config from redis or upstream
        }

        let current_revision = cached_state.revision().map(String::from);
        #[cfg(feature = "processing")]
        if let Some(redis_source) = self.redis_source {
            let current_revision = current_revision.clone();

            let redis_permit = self.redis_semaphore.acquire().await?;
            let state_fetch_result = tokio::task::spawn_blocking(move || {
                redis_source.get_config_if_changed(project_key, current_revision.as_deref())
            })
            .await?;
            drop(redis_permit);

            match state_fetch_result {
                // New state fetched from Redis, possibly pending.
                Ok(UpstreamProjectState::New(state)) => {
                    let state = state.sanitized();
                    if !state.is_pending() {
                        return Ok(ProjectFetchState::new(state));
                    }
                }
                // Redis reported that we're holding an up-to-date version of the state already,
                // refresh the state and return the old cached state again.
                Ok(UpstreamProjectState::NotModified) => {
                    return Ok(ProjectFetchState::refresh(cached_state))
                }
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

        match state {
            UpstreamProjectState::New(state) => Ok(ProjectFetchState::new(state.sanitized())),
            UpstreamProjectState::NotModified => Ok(ProjectFetchState::refresh(cached_state)),
        }
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
    ///
    /// Settings this to `None` will essentially always re-fetch
    /// the project config.
    pub current_revision: Option<String>,

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
