use std::io;
use std::thread;
use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::{Mutex, RwLock};
use tokio_core::reactor::{Core, Remote};
use futures::Stream;
use futures::sync::{mpsc, oneshot};

use smith_common::ProjectId;
use smith_aorta::{test_req, AortaConfig, ProjectState};

enum TroveEvent {
    /// Tells the trove governor to shut down.
    Shutdown,
}

/// Raised for errors that happen in the context of trove governing.
#[derive(Debug, Fail)]
pub enum GovernorError {
    /// Raised if the governor could not be spawned.
    #[fail(display = "could not spawn governor thread")]
    SpawnError(#[cause] io::Error),
    /// Raised if the event loop failed to spawn.
    #[fail(display = "cannot spawn event loop")]
    CannotSpawnEventLoop(#[cause] io::Error),
    /// Raised if the governor panicked.
    #[fail(display = "governor thread panicked")]
    Panic,
}

struct TroveInner {
    states: RwLock<HashMap<ProjectId, Arc<ProjectState>>>,
    config: Arc<AortaConfig>,
    governor_tx: RwLock<Option<mpsc::UnboundedSender<TroveEvent>>>,
    remote: RwLock<Option<Remote>>,
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
    ///
    /// The config must already be stored in an Arc so it can be effectively
    /// shared around.
    pub fn new(config: Arc<AortaConfig>) -> Trove {
        Trove {
            inner: Arc::new(TroveInner {
                states: RwLock::new(HashMap::new()),
                config: config,
                governor_tx: RwLock::new(None),
                remote: RwLock::new(None),
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
            let state = ProjectState::new(project_id, self.inner.config.clone());
            self.inner
                .states
                .write()
                .insert(project_id, Arc::new(state));
        }
        (*self.inner.states.read().get(&project_id).unwrap()).clone()
    }

    /// Returns `true` if the trove is governed.
    pub fn is_governed(&self) -> bool {
        self.inner.remote.read().is_some()
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
        debug!("spawning trove governor");

        *self.join_handle.lock() = Some(thread::Builder::new()
            .name("trove-governor".into())
            .spawn(move || {
                let (tx, rx) = mpsc::unbounded::<TroveEvent>();
                *inner.governor_tx.write() = Some(tx);
                run_governor(inner, rx)
            })
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
        // indicate on the trove inner that we're no longer governed and then
        // attempt to send a message into the event channel if we can safely
        // do so.
        self.emit_event(TroveEvent::Shutdown);

        if let Some(handle) = self.join_handle.lock().take() {
            match handle.join() {
                Err(_) => Err(GovernorError::Panic),
                Ok(result) => result,
            }
        } else {
            Ok(())
        }
    }

    /// Returns the remote of the underlying governor.
    ///
    /// In case the governor is not running this will panic.
    pub fn remote(&self) -> Remote {
        self.inner
            .remote
            .read()
            .as_ref()
            .expect("trove is not goverened, remote unavailable")
            .clone()
    }

    fn emit_event(&self, event: TroveEvent) -> bool {
        if let Some(ref tx) = *self.inner.governor_tx.read() {
            tx.unbounded_send(event).is_ok()
        } else {
            false
        }
    }
}

impl Drop for Trove {
    fn drop(&mut self) {
        self.abdicate().unwrap();
    }
}

fn run_governor(
    inner: Arc<TroveInner>,
    rx: mpsc::UnboundedReceiver<TroveEvent>,
) -> Result<(), GovernorError> {
    let mut core = Core::new().map_err(GovernorError::CannotSpawnEventLoop)?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut shutdown_tx = Some(shutdown_tx);

    // Make the remote available outside.
    *inner.remote.write() = Some(core.remote());
    debug!("spawned trove governor");

    // spawn a stream that just listens in on the events sent into the
    // trove governor so we can figure out when to terminate the thread
    // or do similar things.
    core.handle().spawn(rx.for_each(move |event| {
        match event {
            TroveEvent::Shutdown => {
                if let Some(shutdown_tx) = shutdown_tx.take() {
                    shutdown_tx.send(()).ok();
                }
            }
        }
        Ok(())
    }));

    // test some stuff
    test_req(core.handle());

    core.run(shutdown_rx).ok();
    *inner.remote.write() = None;
    debug!("shut down trove governor");

    Ok(())
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
