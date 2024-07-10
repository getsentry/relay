//! Types that represent the current project state.
use std::sync::Arc;

use relay_config::Config;
use relay_dynamic_config::ProjectConfig;
use tokio::time::Instant;

use crate::services::project::{ParsedProjectState, ProjectInfo};

#[derive(Clone, Debug)]
pub struct ProjectFetchState {
    /// The time at which this project state was last updated.
    last_fetch: Instant,
    state: ProjectState,
}

impl ProjectFetchState {
    /// Takes a [`ProjectState`] and sets it's last fetch to the current time.
    pub fn new(state: ProjectState) -> Self {
        Self {
            last_fetch: Instant::now(),
            state,
        }
    }

    /// Project state for an unknown but allowed project.
    ///
    /// This state is used for forwarding in Proxy mode.
    pub fn allowed() -> Self {
        Self::enabled(ProjectInfo {
            project_id: None,
            last_change: None,
            public_keys: Default::default(),
            slug: None,
            config: ProjectConfig::default(),
            organization_id: None,
        })
    }

    /// An enabled project state created from a project info.
    pub fn enabled(project_info: ProjectInfo) -> Self {
        Self::new(ProjectState::Enabled(Arc::new(project_info)))
    }

    // Returns a disabled state.
    pub fn disabled() -> Self {
        Self::new(ProjectState::Disabled)
    }

    /// Returns a pending or invalid state.
    pub fn pending() -> Self {
        Self {
            last_fetch: Instant::now(),
            state: ProjectState::Pending,
        }
    }

    /// Create a config that immediately counts as expired.
    ///
    /// This is what [`super::Project`] initializes itself with.
    pub fn expired(config: &Config) -> Self {
        Self {
            // Make sure the state immediately qualifies as expired:
            last_fetch: Instant::now() - config.project_cache_expiry(),
            state: ProjectState::Pending,
        }
    }

    /// Returns `true` if the contained state is invalid.
    pub fn is_pending(&self) -> bool {
        matches!(self.state, ProjectState::Pending)
    }

    pub fn sanitize(self) -> Self {
        let Self { last_fetch, state } = self;
        Self {
            last_fetch,
            state: state.sanitize(),
        }
    }

    pub fn expiry_state(&self, config: &Config) -> ExpiryState {
        match self.check_expiry(config) {
            Expiry::Updated => ExpiryState::Updated(&self.state),
            Expiry::Stale => ExpiryState::Stale(&self.state),
            Expiry::Expired => ExpiryState::Expired,
        }
    }

    /// Maps the expiry state to a usable state.
    pub fn current_state(&self, config: &Config) -> ProjectState {
        match self.expiry_state(config) {
            ExpiryState::Updated(state) | ExpiryState::Stale(state) => state.clone(),
            ExpiryState::Expired => ProjectState::Pending,
        }
    }

    /// Returns whether this state is outdated and needs to be refetched.
    /// TODO(jjbayer): can be merged w/
    fn check_expiry(&self, config: &Config) -> Expiry {
        let expiry = match &self.state {
            ProjectState::Enabled(info) if info.project_id.is_some() => {
                config.project_cache_expiry()
            }
            _ => config.cache_miss_expiry(),
        };

        let elapsed = self.last_fetch.elapsed();
        if elapsed >= expiry + config.project_grace_period() {
            Expiry::Expired
        } else if elapsed >= expiry {
            Expiry::Stale
        } else {
            Expiry::Updated
        }
    }
}

/// The expiry status of a project state. Return value of [`ProjectState::check_expiry`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Expiry {
    /// The project state is perfectly up to date.
    Updated,
    /// The project state is outdated but events depending on this project state can still be
    /// processed. The state should be refreshed in the background though.
    Stale,
    /// The project state is completely outdated and events need to be buffered up until the new
    /// state has been fetched.
    Expired,
}

#[derive(Clone, Copy, Debug)]
pub enum ExpiryState<'a> {
    /// An up-to-date project state. See [`Expiry::Updated`].
    Updated(&'a ProjectState),
    /// A stale project state that can still be used. See [`Expiry::Stale`].
    Stale(&'a ProjectState),
    /// An expired project state that should not be used. See [`Expiry::Expired`].
    Expired,
}

/// TODO: docs
#[derive(Clone, Debug)]
pub enum ProjectState {
    Enabled(Arc<ProjectInfo>),
    Disabled,
    Pending,
}

impl ProjectState {
    pub fn sanitize(self) -> Self {
        match self {
            ProjectState::Enabled(state) => {
                ProjectState::Enabled(Arc::new(state.as_ref().clone().sanitize()))
            }
            ProjectState::Disabled => ProjectState::Disabled,
            ProjectState::Pending => ProjectState::Pending,
        }
    }
}

impl From<ParsedProjectState> for ProjectState {
    fn from(value: ParsedProjectState) -> Self {
        let ParsedProjectState { disabled, info } = value;
        match disabled {
            true => ProjectState::Disabled,
            false => ProjectState::Enabled(Arc::new(info)),
        }
    }
}