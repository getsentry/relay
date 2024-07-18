//! Types that represent the current project state.
use std::sync::Arc;

mod fetch_state;
mod info;

pub use fetch_state::{ExpiryState, ProjectFetchState};
pub use info::{LimitedProjectInfo, ProjectInfo};
use serde::{Deserialize, Serialize};

/// Representation of a project's current state.
#[derive(Clone, Debug)]
pub enum ProjectState {
    /// A valid project that is not disabled.
    Enabled(Arc<ProjectInfo>),
    /// A project that was marked as "gone" by the upstream. This variant does not expose
    /// any other project information.
    Disabled,
    /// A project to which one of the following conditions apply:
    /// - The project has not yet been fetched.
    /// - The upstream returned "pending" for this project (see [`crate::services::project_upstream`]).
    /// - The upstream returned an unparsable project so we have to try again.
    /// - The project has expired and must be treated as "has not been fetched".
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

    pub fn is_pending(&self) -> bool {
        matches!(self, ProjectState::Pending)
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

/// Project state as used in serialization / deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedProjectState {
    /// Whether the project state is disabled.
    #[serde(default)]
    pub disabled: bool,
    /// Project info.
    ///
    /// This contains no information when `disabled` is `true`, except for
    /// public keys in static project configs (see [`crate::services::project_local`]).
    #[serde(flatten)]
    pub info: ProjectInfo,
}

/// Limited project state for external Relays.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", remote = "ParsedProjectState")]
pub struct LimitedParsedProjectState {
    pub disabled: bool,
    #[serde(with = "LimitedProjectInfo")]
    #[serde(flatten)]
    pub info: ProjectInfo,
}
