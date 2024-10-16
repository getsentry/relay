use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_system::Addr;

use super::state::Shared;
use crate::services::projects::cache2::service::ProjectCache;

pub struct ProjectCacheHandle {
    shared: Arc<Shared>,
    service: Addr<ProjectCache>,
}

impl ProjectCacheHandle {
    pub fn get(&self, project_key: ProjectKey) -> Option<()> {
        match self.shared.get(project_key) {
            Some(project) => Some(project),
            None => {
                self.service.send(ProjectCache::Fetch(project_key));
                None
            }
        }
    }
}
