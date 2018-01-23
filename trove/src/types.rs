use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::RwLock;

use smith_common::{Dsn, ProjectId};
use smith_aorta::{ProjectState, UpstreamDescriptor};

#[derive(Hash, PartialEq, Eq, Clone)]
struct StateKey<'a>(UpstreamDescriptor<'a>, ProjectId);

impl<'a> StateKey<'a> {
    pub fn from_dsn(dsn: &'a Dsn) -> StateKey<'a> {
        StateKey(UpstreamDescriptor::from_dsn(dsn), dsn.project_id())
    }

    pub fn into_owned(self) -> StateKey<'static> {
        StateKey(self.0.into_owned(), self.1)
    }
}

/// The trove holds project states.
///
/// The goal of the trove is that the server or another component that
/// also wants agent functionality can based on a DSN retrieve the current
/// state of the project. Likewise if a DSN does not exist it will trivially
/// figure out.  The trove can manage concurrent updates in the background
/// automatically.
pub struct Trove {
    states: RwLock<HashMap<StateKey<'static>, Arc<ProjectState>>>,
}

impl Trove {
    /// Creates a new empty trove.
    pub fn new() -> Trove {
        Trove {
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Looks up the project state by dsn.
    ///
    /// This lookup also creates an entry if it does not exist yet.  The
    /// project state might be expired which is why an arc is returned.
    /// Until the arc is dropped the data can be retained.
    pub fn state_for_dsn(&self, dsn: &Dsn) -> Arc<ProjectState> {
        self.state_for_key(StateKey::from_dsn(dsn))
    }

    /// Looks up a project state by upstream descriptor and project id.
    pub fn state_for_project(
        &self,
        upstream: &UpstreamDescriptor,
        project_id: ProjectId,
    ) -> Arc<ProjectState> {
        self.state_for_key(StateKey(upstream.clone(), project_id))
    }

    fn state_for_key(&self, key: StateKey) -> Arc<ProjectState> {
        {
            let states = self.states.read();
            if let Some(ref rv) = states.get(&key) {
                return (*rv).clone();
            }
        }
        {
            let state = ProjectState::new(key.1, &key.0);
            self.states
                .write()
                .insert(key.clone().into_owned(), Arc::new(state));
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
