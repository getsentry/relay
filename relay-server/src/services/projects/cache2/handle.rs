use core::fmt;
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_system::Addr;

use super::state::Shared;
use crate::services::projects::cache2::service::ProjectCache;
use crate::services::projects::cache2::state::SharedProject;

#[derive(Clone)]
pub struct ProjectCacheHandle {
    shared: Arc<Shared>,
    service: Addr<ProjectCache>,
}

impl ProjectCacheHandle {
    /// Returns the current project state for the `project_key`.
    pub fn get(&self, project_key: ProjectKey) -> SharedProject {
        match self.shared.get_or_create(project_key) {
            Ok(project) => project,
            Err(missing) => missing.fetch(&self.service),
        }
    }
}

impl fmt::Debug for ProjectCacheHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectCacheHandle")
            .field("shared", &self.shared)
            .finish()
    }
}
