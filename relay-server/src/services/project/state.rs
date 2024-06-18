//! Manages the internal and the exposed state of a [`super::Project`].
//!
//! The project might still have a cached buffer internally but already be expired,
//! which is why we differentiate between internal and exposed state.
use std::sync::Arc;

use relay_config::Config;
use relay_metrics::aggregator;

use crate::services::project::metrics::{Buckets, Filtered};
use crate::services::project::Expiry;

use super::ProjectState;

#[derive(Debug)]
pub struct State {
    config: Arc<Config>,
    inner: InternalState,
}

impl State {
    /// TODO: docs
    pub fn pending(config: Arc<Config>) -> Self {
        let inner = InternalState::pending(&config);
        Self { config, inner }
    }

    #[cfg(test)]
    pub fn cached(project_state: ProjectState) -> Self {
        Self {
            config: Arc::new(Config::default()),
            inner: InternalState::Cached(Arc::new(project_state)),
        }
    }

    /// TODO: docs
    pub fn as_ref(&self) -> StateRef {
        match &self.inner {
            InternalState::Disabled => StateRef::Disabled,
            InternalState::Cached(project_state) => {
                match project_state.check_expiry(&self.config) {
                    Expiry::Updated | Expiry::Stale => StateRef::Cached(project_state),
                    Expiry::Expired => StateRef::Pending,
                }
            }
            InternalState::Pending(_) => StateRef::Pending,
        }
    }

    pub fn as_mut(&mut self) -> StateRefMut {
        // 1 - Transition state if necessary.
        match &mut self.inner {
            InternalState::Disabled => {}
            InternalState::Cached(project_state) => {
                match project_state.check_expiry(&self.config) {
                    Expiry::Updated | Expiry::Stale => {}
                    Expiry::Expired => {
                        // Modify the internal state to make the buffer available.
                        self.inner = InternalState::pending(&self.config);
                    }
                }
            }
            InternalState::Pending(_) => {}
        };

        // 2 - Return state.
        match &mut self.inner {
            InternalState::Disabled => StateRefMut::Disabled,
            InternalState::Cached(project_state) => StateRefMut::Cached(project_state),
            InternalState::Pending(buffer) => StateRefMut::Pending(buffer.as_mut()),
        }
    }

    /// Sets the cached state using provided `ProjectState`.
    /// If the variant was pending, the buckets will be returned.
    pub fn update(
        &mut self,
        project_state: &Arc<ProjectState>,
        config: &Config,
    ) -> Option<Buckets<Filtered>> {
        if project_state.disabled() {
            self.inner = InternalState::Disabled;
            None
        } else if project_state.invalid()
            || matches!(project_state.check_expiry(config), Expiry::Expired)
        {
            // Invalid or expired means we'll try again, set state to pending.
            // TODO: ensure we'll fetch here (unite this function with get_or_fetch_state).
            match self.inner {
                InternalState::Pending(_) => {}
                InternalState::Cached(_) | InternalState::Disabled => {
                    self.inner = InternalState::pending(&self.config)
                }
            }
            None
        } else {
            // Set state to cached and return buffer.
            let old = std::mem::replace(
                &mut self.inner,
                InternalState::Cached(project_state.clone()),
            );
            match old {
                InternalState::Pending(agg) => Some(Buckets::new(agg.into_buckets())),
                _ => None,
            }
        }
    }
}

/// Externally visible state of a project.
pub enum StateRef<'a> {
    Cached(&'a Arc<ProjectState>),
    Pending,
    Disabled,
}

pub enum StateRefMut<'a> {
    Cached(&'a Arc<ProjectState>),
    Pending(&'a mut aggregator::Aggregator),
    Disabled,
}

#[derive(Debug)]
enum InternalState {
    Cached(Arc<ProjectState>),
    Pending(Box<aggregator::Aggregator>),
    Disabled,
}

impl InternalState {
    fn pending(config: &Config) -> Self {
        Self::Pending(Box::new(aggregator::Aggregator::named(
            "metrics-buffer".to_string(),
            config.permissive_aggregator_config(),
        )))
    }
}
