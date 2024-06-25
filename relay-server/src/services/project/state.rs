//! Types that represent the current project state.
use std::sync::Arc;

use relay_config::Config;
use relay_dynamic_config::ProjectConfig;
use tokio::time::Instant;

use crate::envelope::Envelope;
use crate::services::outcome::DiscardReason;
use crate::services::project::{ParsedProjectState, ProjectInfo};

#[derive(Clone, Debug)]
pub struct ProjectFetchState {
    /// The time at which this project state was last updated.
    last_fetch: Instant,
    state: ProjectState,
}

impl ProjectFetchState {
    /// Project state for an unknown but allowed project.
    ///
    /// This state is used for forwarding in Proxy mode.
    pub fn allowed() -> Self {
        Self::new(ProjectState::Enabled(Arc::new(ProjectInfo {
            project_id: None,
            last_change: None,
            public_keys: Default::default(),
            slug: None,
            config: ProjectConfig::default(),
            organization_id: None,
        })))
    }

    // Returns an invalid state.
    pub fn err() -> Self {
        // TODO: rename to invalid()
        Self::new(ProjectState::Invalid)
    }

    // Returns a disabled state.
    pub fn disabled() -> Self {
        Self::new(ProjectState::Disabled)
    }

    /// Returns `true` if the contained state is invalid.
    pub fn invalid(&self) -> bool {
        matches!(self.state, ProjectState::Invalid)
    }

    pub fn sanitize(self) -> Self {
        let Self { last_fetch, state } = self;
        Self {
            last_fetch,
            state: state.sanitize(),
        }
    }

    /// Determines whether the given envelope should be accepted or discarded.
    ///
    /// Returns `Ok(())` if the envelope should be accepted. Returns `Err(DiscardReason)` if the
    /// envelope should be discarded, by indicating the reason. The checks preformed for this are:
    ///
    ///  - Allowed origin headers
    ///  - Disabled or unknown projects
    ///  - Disabled project keys (DSN)
    ///  - Feature flags
    pub fn check_envelope(
        &self,
        envelope: &Envelope,
        config: &Config,
    ) -> Result<(), DiscardReason> {
        // Verify that the stated project id in the DSN matches the public key used to retrieve this
        // project state.
        let meta = envelope.meta();
        if !self.state.is_valid_project_id(meta.project_id(), config) {
            return Err(DiscardReason::ProjectId);
        }

        // Try to verify the request origin with the project config.
        if !self.state.is_valid_origin(meta.origin()) {
            return Err(DiscardReason::Cors);
        }

        // sanity-check that the state has a matching public key loaded.
        if !self.state.is_matching_key(meta.public_key()) {
            relay_log::error!("public key mismatch on state {}", meta.public_key());
            return Err(DiscardReason::ProjectId);
        }

        // Check for invalid or disabled projects.
        self.check_disabled(config)?;

        // Check feature.
        if let Some(disabled_feature) = envelope
            .required_features()
            .iter()
            .find(|f| !self.state.has_feature(**f))
        {
            return Err(DiscardReason::FeatureDisabled(*disabled_feature));
        }

        Ok(())
    }

    pub fn never_fetched() -> Self {
        Self {
            last_fetch: Instant::now(),
            state: ProjectState::Invalid,
        }
    }

    fn new(state: ProjectState) -> Self {
        Self {
            last_fetch: Instant::now(),
            state,
        }
    }

    /// Returns `Err` if the project is known to be invalid or disabled.
    ///
    /// If this project state is hard outdated, this returns `Ok(())`, instead, to avoid prematurely
    /// dropping data.
    // TODO(jjbayer): Remove this function.
    pub fn check_disabled(&self, config: &Config) -> Result<(), DiscardReason> {
        // if the state is out of date, we proceed as if it was still up to date. The
        // upstream relay (or sentry) will still filter events.
        if self.check_expiry(config) == Expiry::Expired {
            return Ok(());
        }

        // if we recorded an invalid project state response from the upstream (i.e. parsing
        // failed), discard the event with a state reason.
        if self.invalid() {
            return Err(DiscardReason::ProjectState);
        }

        // only drop events if we know for sure the project or key are disabled.
        if matches!(self.state, ProjectState::Disabled) {
            return Err(DiscardReason::ProjectId);
        }

        Ok(())
    }

    /// Returns the current [`ExpiryState`] for this project.
    /// If the project state's [`Expiry`] is `Expired`, do not return it.
    pub fn current_state(&self, config: &Config) -> CurrentState {
        match self.check_expiry(config) {
            Expiry::Stale | Expiry::Updated => match &self.state {
                ProjectState::Enabled(info) => CurrentState::Enabled(info),
                ProjectState::Disabled => CurrentState::Disabled,
                ProjectState::Invalid => CurrentState::Pending,
            },
            Expiry::Expired => CurrentState::Pending,
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

/// The exposed state of a project.
pub enum CurrentState<'a> {
    /// An enabled project that has not yet expired.
    Enabled(&'a Arc<ProjectInfo>),
    /// A disabled project.
    Disabled,
    /// A project that needs fetching.
    Pending,
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

/// TODO: docs
#[derive(Clone, Debug)]
pub enum ProjectState {
    Enabled(Arc<ProjectInfo>),
    Disabled,
    Invalid,
}

impl ProjectState {
    pub fn sanitize(self) -> Self {
        match self {
            ProjectState::Enabled(state) => ProjectState::Enabled(state.sanitize()),
            ProjectState::Disabled => ProjectState::Disabled,
            ProjectState::Invalid => ProjectState::Invalid,
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
