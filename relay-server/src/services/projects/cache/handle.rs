use std::fmt;
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::Addr;
use tokio::sync::broadcast;

use super::state::Shared;
use crate::services::projects::cache::service::ProjectEvent;
use crate::services::projects::cache::{Project, ProjectCache};

#[derive(Clone)]
pub struct ProjectCacheHandle {
    pub(super) shared: Arc<Shared>,
    pub(super) config: Arc<Config>,
    pub(super) service: Addr<ProjectCache>,
    pub(super) project_events: broadcast::Sender<ProjectEvent>,
}

impl ProjectCacheHandle {
    /// Returns the current project state for the `project_key`.
    pub fn get(&self, project_key: ProjectKey) -> Project<'_> {
        let project = self.shared.get_or_create(project_key);
        // Always trigger a fetch after retrieving the project to make sure the state is up to date.
        self.fetch(project_key);

        Project::new(project, &self.config)
    }

    /// Triggers a fetch/update check in the project cache for the supplied project.
    pub fn fetch(&self, project_key: ProjectKey) {
        self.service.send(ProjectCache::Fetch(project_key));
    }

    /// Returns a subscription to all [`ProjectEvent`]'s.
    ///
    /// This stream notifies the subscriber about project state changes in the project cache.
    /// Events may arrive in arbitrary order and be delivered multiple times.
    pub fn events(&self) -> broadcast::Receiver<ProjectEvent> {
        self.project_events.subscribe()
    }
}

impl fmt::Debug for ProjectCacheHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectCacheHandle")
            .field("shared", &self.shared)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::services::projects::project::ProjectState;

    impl ProjectCacheHandle {
        pub fn for_test() -> Self {
            Self {
                shared: Default::default(),
                config: Default::default(),
                service: Addr::dummy(),
                project_events: broadcast::channel(999_999).0,
            }
        }

        pub fn test_set_project_state(&self, project_key: ProjectKey, state: ProjectState) {
            let is_pending = state.is_pending();
            self.shared.test_set_project_state(project_key, state);
            if is_pending {
                let _ = self.project_events.send(ProjectEvent::Evicted(project_key));
            } else {
                let _ = self.project_events.send(ProjectEvent::Ready(project_key));
            }
        }

        pub fn test_has_project_created(&self, project_key: ProjectKey) -> bool {
            self.shared.test_has_project_created(project_key)
        }

        pub fn test_num_fetches(&self) -> u64 {
            self.service.len()
        }
    }
}
