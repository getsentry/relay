use core::fmt;
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::Addr;
use tokio::sync::broadcast;

use super::state::Shared;
use crate::services::projects::cache2::service::ProjectEvent;
use crate::services::projects::cache2::{Project, ProjectCache};

#[derive(Clone)]
pub struct ProjectCacheHandle {
    shared: Arc<Shared>,
    config: Arc<Config>,
    service: Addr<ProjectCache>,
    project_events: broadcast::Sender<ProjectEvent>,
}

impl ProjectCacheHandle {
    /// Returns the current project state for the `project_key`.
    pub fn get(&self, project_key: ProjectKey) -> Project<'_> {
        // TODO: maybe we should always trigger a fetch?
        // We need a way to continously keep projects updated while at the same time
        // let unused projects expire.
        // TODO: trigger prefetch for the sampling projects, maybe take a resolver trait which can
        // also resolve the sampling project and fetch? Or do it explicit.
        let project = match self.shared.get_or_create(project_key) {
            Ok(project) => project,
            Err(missing) => missing.fetch(&self.service),
        };
        Project::new(project, &self.config)
    }

    pub fn fetch(&self, project_key: ProjectKey) {
        // TODO: does this make sense?
        self.service.send(ProjectCache::Fetch(project_key));
    }

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
