use std::sync::Arc;

use tokio::time::Instant;

use relay_dynamic_config::ProjectConfig;

use crate::services::projects::project::state::info::ProjectInfo;
use crate::services::projects::project::ProjectState;

/// Hides a cached project state and only exposes it if it has not expired.
#[derive(Clone, Debug)]
pub struct ProjectFetchState {
    /// The time at which this project state was last updated.
    last_fetch: Option<Instant>,
    state: ProjectState,
}

impl ProjectFetchState {
    /// Takes a [`ProjectState`] and sets it's last fetch to the current time.
    pub fn new(state: ProjectState) -> Self {
        Self {
            last_fetch: Some(Instant::now()),
            state,
        }
    }

    /// Refreshes the expiry of the fetch state.
    pub fn refresh(old: ProjectFetchState) -> Self {
        Self {
            last_fetch: Some(Instant::now()),
            state: old.state,
        }
    }

    /// Project state for an unknown but allowed project.
    ///
    /// This state is used for forwarding in Proxy mode.
    pub fn allowed() -> Self {
        Self::enabled(ProjectInfo {
            project_id: None,
            last_change: None,
            rev: None,
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
        Self::new(ProjectState::Pending)
    }

    /// Create a config that immediately counts as expired.
    ///
    /// This is what [`Project`](crate::services::projects::project::Project) initializes itself with.
    pub fn expired() -> Self {
        Self {
            // Make sure the state immediately qualifies as expired:
            last_fetch: None,
            state: ProjectState::Pending,
        }
    }

    /// Sanitizes the contained project state. See [`ProjectState::sanitized`].
    pub fn sanitized(self) -> Self {
        let Self { last_fetch, state } = self;
        Self {
            last_fetch,
            state: state.sanitized(),
        }
    }

    /// Returns `true` if the contained state is pending.
    pub fn is_pending(&self) -> bool {
        matches!(self.state, ProjectState::Pending)
    }

    /// Returns the revision of the contained project state.
    ///
    /// See: [`ProjectState::revision`].
    pub fn revision(&self) -> Option<&str> {
        self.state.revision()
    }
}

impl From<ProjectFetchState> for ProjectState {
    fn from(value: ProjectFetchState) -> Self {
        value.state
    }
}
