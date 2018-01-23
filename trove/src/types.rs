use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::RwLock;

use smith_common::ProjectId;
use smith_aorta::{ProjectState, UpstreamDescriptor};

/// The trove holds project states.
pub struct Trove {
    states: RwLock<HashMap<ProjectId, Arc<ProjectState>>>,
    upstream: UpstreamDescriptor<'static>,
}

impl Trove {
    /// Creates a new empty trove for an upstream.
    pub fn new(upstream: &UpstreamDescriptor) -> Trove {
        Trove {
            states: RwLock::new(HashMap::new()),
            upstream: upstream.clone().into_owned(),
        }
    }

    /// Looks up a project state by project ID.
    pub fn state_for_project(&self, project_id: ProjectId) -> Arc<ProjectState> {
        // state already exists, return it.
        {
            let states = self.states.read();
            if let Some(ref rv) = states.get(&project_id) {
                return (*rv).clone();
            }
        }

        // insert an empty state
        {
            let state = ProjectState::new(project_id, &self.upstream);
            self.states.write().insert(project_id, Arc::new(state));
        }
        (*self.states.read().get(&project_id).unwrap()).clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_assert_sync() {
        struct Assert<T: Sync> {
            x: Option<T>,
        }
        let val: Assert<Trove> = Assert { x: None };
        assert_eq!(val.x.is_none(), true);
    }
}
