use std::sync::Arc;
use parking_lot::RwLock;

use upstream::UpstreamDescriptor;

/// The project state snapshot represents a known server state of
/// a project.
///
/// This is generally used by an indirection of `ProjectState` which
/// manages a view over it which supports concurrent updates in the
/// background.
#[derive(Debug, Clone)]
pub struct ProjectStateSnapshot {
    disabled: bool,
}

/// Gives access to the project's remote state.
///
/// This wrapper is sync and can be updated concurrently.  As the type is
/// sync all of the methods can be used on a shared instance.  The type
/// internally locks automatically.
#[derive(Debug)]
pub struct ProjectState {
    upstream: UpstreamDescriptor<'static>,
    project_id: String,
    current_snapshot: RwLock<Option<Arc<ProjectStateSnapshot>>>,
}

impl ProjectStateSnapshot {
    /// Returns `true` if the entire project should be considered
    /// disabled (blackholed, deleted etc.).
    pub fn disabled(&self) -> bool {
        self.disabled
    }
}

impl ProjectState {
    /// Creates a new project state.
    ///
    /// The project state is created without storing a snapshot.  This means
    /// that accessing the snapshot will panic until the data becomes available.
    pub fn new(project_id: &str, upstream: &UpstreamDescriptor) -> ProjectState {
        ProjectState {
            project_id: project_id.to_string(),
            upstream: upstream.clone().into_owned(),
            current_snapshot: RwLock::new(None),
        }
    }

    /// Returns `true` if the project state is available.
    pub fn snapshot_available(&self) -> bool {
        self.current_snapshot.read().is_some()
    }

    /// Returns the current project state.
    pub fn snapshot(&self) -> Arc<ProjectStateSnapshot> {
        let lock = self.current_snapshot.read();
        match *lock {
            Some(ref arc) => arc.clone(),
            None => panic!("Snapshot not yet available"),
        }
    }

    /// Sets a new snapshot.
    pub fn set_snapshot(&self, new_snapshot: ProjectStateSnapshot) {
        *self.current_snapshot.write() = Some(Arc::new(new_snapshot));
    }

    /// The project ID of this project.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// The direct upstream that reported the snapshot.
    pub fn upstream(&self) -> &UpstreamDescriptor {
        &self.upstream
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
        let val: Assert<ProjectState> = Assert { x: None };
        assert_eq!(val.x.is_none(), true);
    }
}
