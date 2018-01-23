use std::io;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;

use parking_lot::{Mutex, RwLock};

use smith_common::ProjectId;
use smith_aorta::{ProjectState, UpstreamDescriptor};

/// Raised for errors that happen in the context of trove governing.
#[derive(Debug, Fail)]
pub enum GovernorError {
    /// Raised if the governor could not be spawned.
    #[fail(display = "could not spawn governor thread")]
    SpawnError(#[cause] io::Error),
    /// Raised if the governor panicked.
    #[fail(display = "governor thread panicked")]
    Panic,
}

struct TroveInner {
    states: RwLock<HashMap<ProjectId, Arc<ProjectState>>>,
    upstream: UpstreamDescriptor<'static>,
    is_governed: AtomicBool,
}

/// The trove holds project states and manages the upstream aorta.
///
/// The trove can be used from multiple threads as the object is internally
/// synchronized.  It also typically has a governing thread running which
/// automatically manages the state for the individual projects.
pub struct Trove {
    inner: Arc<TroveInner>,
    join_handle: Mutex<Option<thread::JoinHandle<Result<(), GovernorError>>>>,
}

impl Trove {
    /// Creates a new empty trove for an upstream.
    pub fn new(upstream: &UpstreamDescriptor) -> Trove {
        Trove {
            inner: Arc::new(TroveInner {
                states: RwLock::new(HashMap::new()),
                upstream: upstream.clone().into_owned(),
                is_governed: AtomicBool::new(false),
            }),
            join_handle: Mutex::new(None),
        }
    }

    /// Looks up a project state by project ID.
    ///
    /// If the project state does not exist in the trove it's created.  The
    /// return value is inside an arc wrapper as the trove might periodically
    /// expired project states.
    ///
    /// Note that project states are also created if snapshots cannot be retrieved
    /// from upstream because the upstream is down or because the project does in
    /// fact not exist or the agent has no permissions to access the data.
    pub fn state_for_project(&self, project_id: ProjectId) -> Arc<ProjectState> {
        // state already exists, return it.
        {
            let states = self.inner.states.read();
            if let Some(ref rv) = states.get(&project_id) {
                return (*rv).clone();
            }
        }

        // insert an empty state
        {
            let state = ProjectState::new(project_id, &self.inner.upstream);
            self.inner
                .states
                .write()
                .insert(project_id, Arc::new(state));
        }
        (*self.inner.states.read().get(&project_id).unwrap()).clone()
    }

    /// Returns `true` if the trove is governed.
    pub fn is_governed(&self) -> bool {
        self.inner.is_governed.load(Ordering::Relaxed)
    }

    /// Spawns a trove governor thread.
    ///
    /// The trove governor is a controlling background thread that manages the
    /// state of the trove.  If the trove is already governed this function will
    /// panic.
    pub fn govern(&self) -> Result<(), GovernorError> {
        if self.is_governed() {
            panic!("trove is already governed");
        }

        let inner = self.inner.clone();
        inner.is_governed.store(true, Ordering::Relaxed);

        *self.join_handle.lock() = Some(thread::Builder::new()
            .name("trove-governor".into())
            .spawn(move || inner.run())
            .map_err(GovernorError::SpawnError)?);

        Ok(())
    }

    /// Abdicates the governor.
    ///
    /// Unlike governing it's permissible to call this method multiple times.
    /// If the trove is already not governed this method will just quietly
    /// do nothing.  Additionally dropping the trove will attempt to abdicate
    /// and kill the thread.  This however might not be executed as there is no
    /// guarantee that dtors are invoked in all cases.
    pub fn abdicate(&self) -> Result<(), GovernorError> {
        self.inner.is_governed.store(false, Ordering::Relaxed);
        if let Some(handle) = self.join_handle.lock().take() {
            match handle.join() {
                Err(_) => Err(GovernorError::Panic),
                Ok(result) => result,
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for Trove {
    fn drop(&mut self) {
        self.abdicate().unwrap();
    }
}

impl TroveInner {
    fn run(&self) -> Result<(), GovernorError> {
        Ok(())
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
