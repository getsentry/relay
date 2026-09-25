//! Types that represent the current project state.
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_quotas::Scoping;

mod info;
mod serialize;

pub use self::info::*;
pub use self::serialize::*;

/// Representation of a project's current state.
#[derive(Clone, Debug, Default)]
pub enum ProjectState {
    /// A valid project that is not disabled.
    Enabled(Arc<ProjectInfo>),
    /// A dummy state for Relay instances which do not synchronize project configs with their upstream.
    ///
    /// This is used by proxy Relay instances which still communicate with the project cache,
    /// but never fetch project configs from upstream. In this configuration data must still be
    /// forwarded even without a project config.
    Dummy,
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
    /// Runs a post-deserialization step to normalize the project config (e.g. legacy fields).
    pub fn sanitized(self, is_processing: bool) -> Self {
        match self {
            Self::Enabled(state) => {
                Self::Enabled(Arc::new(state.as_ref().clone().sanitized(is_processing)))
            }
            Self::Dummy => Self::Dummy,
            Self::Disabled => Self::Disabled,
            Self::Pending => Self::Pending,
        }
    }

    /// Whether or not this state is pending.
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Returns the revision of the contained project info.
    ///
    /// `None` if the revision is missing or not available.
    pub fn revision(&self) -> Revision {
        match &self {
            Self::Enabled(info) => info.rev.clone(),
            Self::Dummy | Self::Disabled | Self::Pending => Revision::default(),
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

impl From<IncomingProjectState> for ProjectState {
    fn from(value: IncomingProjectState) -> Self {
        let IncomingProjectState { disabled, info } = value;
        match disabled {
            true => Self::Disabled,
            false => Self::Enabled(info),
        }
    }
}
