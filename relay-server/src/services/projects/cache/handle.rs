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
