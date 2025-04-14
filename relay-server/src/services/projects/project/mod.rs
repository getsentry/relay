//! Types that represent the current project state.
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_quotas::Scoping;
use serde::{Deserialize, Serialize};

mod info;

pub use self::info::*;

/// Representation of a project's current state.
#[derive(Clone, Debug, Default)]
pub enum ProjectState {
    /// A valid project that is not disabled.
    Enabled(Arc<ProjectInfo>),
    /// A project that was marked as "gone" by the upstream. This variant does not expose
    /// any other project information.
    Disabled,
    /// A project to which one of the following conditions apply:
    /// - The project has not yet been fetched.
    /// - The upstream returned "pending" for this project (see [`crate::services::projects::source::upstream`]).
    /// - The upstream returned an unparsable project so we have to try again.
    /// - The project has expired and must be treated as "has not been fetched".
    #[default]
    Pending,
}

impl ProjectState {
    /// Project state for an unknown but allowed project.
    ///
    /// This state is used for forwarding in Proxy mode.
    pub fn new_allowed() -> Self {
        Self::Enabled(Arc::new(ProjectInfo {
            project_id: None,
            last_change: None,
            rev: Default::default(),
            public_keys: Default::default(),
            slug: None,
            config: Default::default(),
            organization_id: None,
        }))
    }

    /// Runs a post-deserialization step to normalize the project config (e.g. legacy fields).
    pub fn sanitized(self) -> Self {
        match self {
            Self::Enabled(state) => Self::Enabled(Arc::new(state.as_ref().clone().sanitized())),
            Self::Disabled => Self::Disabled,
            Self::Pending => Self::Pending,
        }
    }

    /// Whether or not this state is pending.
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Utility function that returns the project config if enabled.
    pub fn enabled(self) -> Option<Arc<ProjectInfo>> {
        match self {
            Self::Enabled(info) => Some(info),
            Self::Disabled | Self::Pending => None,
        }
    }

    /// Returns the revision of the contained project info.
    ///
    /// `None` if the revision is missing or not available.
    pub fn revision(&self) -> Revision {
        match &self {
            Self::Enabled(info) => info.rev.clone(),
            Self::Disabled | Self::Pending => Revision::default(),
        }
    }

    /// Creates `Scoping` for this project if the state is loaded.
    ///
    /// Returns `Some` if the project state has been fetched and contains a project identifier,
    /// otherwise `None`.
    pub fn scoping(&self, project_key: ProjectKey) -> Option<Scoping> {
        match self {
            Self::Enabled(info) => info.scoping(project_key),
            _ => None,
        }
    }
}

impl From<ParsedProjectState> for ProjectState {
    fn from(value: ParsedProjectState) -> Self {
        let ParsedProjectState { disabled, info } = value;
        match disabled {
            true => Self::Disabled,
            false => Self::Enabled(Arc::new(info)),
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
    /// public keys in static project configs (see [`crate::services::projects::source::local`]).
    #[serde(flatten)]
    pub info: ProjectInfo,
}

/// Limited project state for external Relays.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", remote = "ParsedProjectState")]
pub struct LimitedParsedProjectState {
    /// Whether the project state is disabled.
    pub disabled: bool,
    /// Limited project info for external Relays.
    ///
    /// This contains no information when `disabled` is `true`, except for
    /// public keys in static project configs (see [`crate::services::projects::source::local`]).
    #[serde(with = "LimitedProjectInfo")]
    #[serde(flatten)]
    pub info: ProjectInfo,
}
