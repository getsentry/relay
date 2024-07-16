//! Types that represent the current project state.
use std::sync::Arc;

mod fetch_state;
mod info;

pub use fetch_state::{ExpiryState, ProjectFetchState};
pub use info::{LimitedProjectInfo, ProjectInfo};
use serde::{Deserialize, Serialize};

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

    pub fn is_pending(&self) -> bool {
        matches!(self, ProjectState::Pending)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedProjectState {
    #[serde(default)]
    pub disabled: bool,
    #[serde(flatten)]
    pub info: ProjectInfo,
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
