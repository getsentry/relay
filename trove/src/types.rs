use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::RwLock;

use smith_common::{Dsn, ProjectId};
use smith_aorta::{ProjectState, UpstreamDescriptor};

/// The trove holds project states.
///
/// The goal of the trove is that the server or another component that
/// also wants agent functionality can based on a DSN retrieve the current
/// state of the project. Likewise if a DSN does not exist it will trivially
/// figure out.  The trove can manage concurrent updates in the background
/// automatically.
pub struct Trove {
    states: RwLock<HashMap<(Option<UpstreamDescriptor<'static>>, ProjectId), Arc<ProjectState>>>,
    default_upstream: UpstreamDescriptor<'static>,
}

impl Trove {
    /// Creates a new empty trove for an upstream.
    pub fn new(upstream: &UpstreamDescriptor) -> Trove {
        Trove {
            states: RwLock::new(HashMap::new()),
            default_upstream: upstream.clone().into_owned(),
        }
    }

    /// Looks up the project state by dsn.
    ///
    /// This lookup also creates an entry if it does not exist yet.  The
    /// project state might be expired which is why an arc is returned.
    /// Until the arc is dropped the data can be retained.
    pub fn state_for_dsn(&self, dsn: &Dsn) -> Arc<ProjectState> {
        let upstream = UpstreamDescriptor::from_dsn(dsn);
        self.get_state(Some(upstream), dsn.project_id())
    }

    /// Looks up a project state by upstream descriptor and project id.
    pub fn state_for_project(&self, project_id: ProjectId) -> Arc<ProjectState> {
        self.get_state(None, project_id)
    }

    fn get_state(
        &self,
        upstream: Option<UpstreamDescriptor>,
        project_id: ProjectId,
    ) -> Arc<ProjectState> {
        // in case the upstream descriptor is the default descriptor (which it should
        // be in most cases), we just null it out for storing.
        let key = (
            match upstream {
                None => None,
                Some(upstream) => {
                    if upstream == self.default_upstream {
                        None
                    } else {
                        Some(upstream.into_owned())
                    }
                }
            },
            project_id,
        );

        // state already exists, return it.
        {
            let states = self.states.read();
            if let Some(ref rv) = states.get(&key) {
                return (*rv).clone();
            }
        }

        // insert an empty state
        {
            let state = ProjectState::new(key.1, key.0.as_ref().unwrap_or(&self.default_upstream));
            self.states.write().insert(key.clone(), Arc::new(state));
        }
        (*self.states.read().get(&key).unwrap()).clone()
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
