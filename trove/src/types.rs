use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::thread;

use futures::sync::{mpsc, oneshot};
use futures::{Future, IntoFuture, Stream};
use hyper;
use hyper::client::{Client, HttpConnector};
use hyper::header::ContentType;
use hyper::{Chunk, Request, StatusCode};
use hyper_tls::HttpsConnector;
use parking_lot::{Mutex, RwLock};
use serde::de::DeserializeOwned;
use serde_json;
use tokio_core::reactor::{Core, Handle, Remote};

use semaphore_aorta::{AortaConfig, ApiErrorResponse, ApiRequest, ProjectState, RequestManager};
use semaphore_common::ProjectId;

/// Represents an event that can be sent to the governor.
#[derive(Debug)]
pub(crate) enum GovernorEvent {
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

/// Raised for API Errors
#[derive(Debug, Fail)]
pub enum ApiError {
    /// On deserialization errors.
    #[fail(display = "could not deserialize response")]
    BadPayload(#[cause] serde_json::Error),
    /// On general http errors.
    #[fail(display = "http error")]
    HttpError(#[cause] hyper::Error),
    /// A server indicated api error ocurred.
    #[fail(display = "bad request ({}; {})", _0, _1)]
    ErrorResponse(StatusCode, ApiErrorResponse),
}

/// An internal helper struct that represents the shared state of the
/// trove.  An `Arc` of the state is passed around various systems.
#[derive(Debug)]
pub struct TroveState {
    config: Arc<AortaConfig>,
    states: RwLock<HashMap<ProjectId, Arc<ProjectState>>>,
    governor_tx: RwLock<Option<mpsc::UnboundedSender<GovernorEvent>>>,
    remote: RwLock<Option<Remote>>,
    request_manager: Arc<RequestManager>,
}

/// Convenience context that never crosses threads.
#[derive(Debug)]
pub struct TroveContext {
    handle: Handle,
    state: Arc<TroveState>,
    client: Client<HttpsConnector<HttpConnector>>,
}

impl TroveContext {
    fn new(handle: Handle, state: Arc<TroveState>) -> Arc<TroveContext> {
        let t = thread::current();
        debug!(
            "created new trove context for thread {} ({:?})",
            t.name().unwrap_or("no-name"),
            t.id()
        );
        Arc::new(TroveContext {
            handle: handle.clone(),
            state,
            client: Client::configure()
                .connector(HttpsConnector::new(4, &handle).unwrap())
                .build(&handle),
        })
    }

    /// Returns the handle of the core loop.
    ///
    /// This is the handle that is used for the underlying HTTP client on the
    /// trove context.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// Returns the state of the trove.
    pub fn state(&self) -> Arc<TroveState> {
        self.state.clone()
    }

    /// Returns a reference to the http client.
    pub fn http_client(&self) -> &Client<HttpsConnector<HttpConnector>> {
        &self.client
    }

    fn perform_aorta_request<D: DeserializeOwned + 'static>(
        &self,
        req: Request,
    ) -> Box<Future<Item = D, Error = ApiError>> {
        Box::new(
            self.http_client()
                .request(req)
                .map_err(ApiError::HttpError)
                .and_then(|res| -> Box<Future<Item = D, Error = ApiError>> {
                    let status = res.status();
                    if status.is_success() {
                        Box::new(res.body().concat2().map_err(ApiError::HttpError).and_then(
                            move |body: Chunk| {
                                Ok(serde_json::from_slice::<D>(&body)
                                    .map_err(ApiError::BadPayload)?)
                            },
                        ))
                    } else if res
                        .headers()
                        .get::<ContentType>()
                        .map(|x| x.type_() == "application" && x.subtype() == "json")
                        .unwrap_or(false)
                    {
                        // error response
                        Box::new(res.body().concat2().map_err(ApiError::HttpError).and_then(
                            move |body: Chunk| {
                                let err: ApiErrorResponse =
                                    serde_json::from_slice(&body).map_err(ApiError::BadPayload)?;
                                Err(ApiError::ErrorResponse(status, err))
                            },
                        ))
                    } else {
                        Box::new(
                            Err(ApiError::ErrorResponse(status, Default::default())).into_future(),
                        )
                    }
                }),
        )
    }

    /// Shortcut for aorta requests for specific types.
    pub fn aorta_request<T: ApiRequest>(
        &self,
        req: &T,
    ) -> Box<Future<Item = T::Response, Error = ApiError>> {
        let (method, path) = req.get_aorta_request_target();
        let req = self.state.config.prepare_aorta_req(method, &path, req);
        self.perform_aorta_request::<T::Response>(req)
    }
}

/// The trove holds project states and manages the upstream aorta.
///
/// The trove can be used from multiple threads as the object is internally
/// synchronized however the typical case is to work with the the trove
/// state or context.  It also typically has a governing thread running which
/// automatically manages the state for the individual projects.
pub struct Trove {
    state: Arc<TroveState>,
    join_handle: Mutex<Option<thread::JoinHandle<Result<(), GovernorError>>>>,
}

impl Trove {
    /// Creates a new empty trove for an upstream.
    ///
    /// The config must already be stored in an Arc so it can be effectively
    /// shared around.
    pub fn new(config: Arc<AortaConfig>) -> Trove {
        Trove {
            state: Arc::new(TroveState {
                states: RwLock::new(HashMap::new()),
                config: config.clone(),
                governor_tx: RwLock::new(None),
                remote: RwLock::new(None),
                request_manager: Arc::new(RequestManager::new(config)),
            }),
            join_handle: Mutex::new(None),
        }
    }

    /// Creates a new trove context.
    ///
    /// The trove as such is a type that exists in a place but cannot be
    /// conveniently passed from thread to thread.  Instead a trove context
    /// can be created for each thread that wants to interface with the
    /// trove which gives access to the most important data in it.
    ///
    /// Alternatively you can directly use the trove state if API requests
    /// do not need to be issued.
    ///
    /// Note that this trove context shares the underlying trove but it has
    /// its own HTTP client and core.  This is for instance used by the
    /// API server to work with the trove from another thread.  This is
    /// needed because the underlying HTTP client cannot be shared between
    /// threads.
    pub fn new_context(&self, handle: Handle) -> Arc<TroveContext> {
        TroveContext::new(handle, self.state.clone())
    }

    /// Returns the internal state of the trove.
    pub fn state(&self) -> Arc<TroveState> {
        self.state.clone()
    }

    /// Spawns a trove governor thread.
    ///
    /// The trove governor is a controlling background thread that manages the
    /// state of the trove.  If the trove is already governed this function will
    /// panic.
    pub fn govern(&self) -> Result<(), GovernorError> {
        if self.state.is_governed() {
            panic!("trove is already governed");
        }

        let state = self.state.clone();
        debug!("spawning trove governor");

        *self.join_handle.lock() = Some(
            thread::Builder::new()
                .name("trove-governor".into())
                .spawn(move || {
                    let (tx, rx) = mpsc::unbounded::<GovernorEvent>();
                    *state.governor_tx.write() = Some(tx);
                    run_governor(state, rx)
                })
                .map_err(GovernorError::SpawnError)?,
        );

        Ok(())
    }

    /// Abdicates the governor.
    ///
    /// Unlike governing it's permissible to call this method multiple times.
    /// If the trove is already not governed this method will just quietly
    /// do nothing.  Additionally dropping the trove will attempt to abdicate
    /// and kill the thread.  This however might not be executed as there is no
    /// guarantee that dtors are invoked in all cases .
    pub fn abdicate(&self) -> Result<(), GovernorError> {
        if !self.state.is_governed() {
            return Ok(());
        }

        // indicate on the trove state that we're no longer governed and then
        // attempt to send a message into the event channel if we can safely
        // do so.
        self.state.emit_governor_event(GovernorEvent::Shutdown);

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

impl TroveState {
    /// Returns `true` if the trove is governed.
    pub fn is_governed(&self) -> bool {
        self.remote.read().is_some()
    }

    /// Returns the aorta config.
    pub fn config(&self) -> Arc<AortaConfig> {
        self.config.clone()
    }

    /// Returns a project state if it exists.
    pub fn get_project_state(&self, project_id: ProjectId) -> Option<Arc<ProjectState>> {
        self.states.read().get(&project_id).cloned()
    }

    /// Gets or creates the project state.
    pub fn get_or_create_project_state(&self, project_id: ProjectId) -> Arc<ProjectState> {
        // state already exists, return it.
        {
            let states = self.states.read();
            if let Some(rv) = states.get(&project_id) {
                return (*rv).clone();
            }
        }

        // insert an empty state
        {
            let state = ProjectState::new(
                project_id,
                self.config.clone(),
                self.request_manager.clone(),
            );
            self.states.write().insert(project_id, Arc::new(state));
        }
        (*self.states.read().get(&project_id).unwrap()).clone()
    }

    /// Returns the current request manager.
    pub fn request_manager(&self) -> Arc<RequestManager> {
        self.request_manager.clone()
    }

    /// Emits an event to the governor.
    pub(crate) fn emit_governor_event(&self, event: GovernorEvent) -> bool {
        info!("emit governor event: {:?}", event);
        if let Some(ref tx) = *self.governor_tx.read() {
            tx.unbounded_send(event).is_ok()
        } else {
            false
        }
    }

    /// Returns the remote for the underlying state.
    pub fn remote(&self) -> Remote {
        self.remote
            .read()
            .as_ref()
            .expect("trove is not goverened, remote unavailable")
            .clone()
    }
}

fn run_governor(
    state: Arc<TroveState>,
    rx: mpsc::UnboundedReceiver<GovernorEvent>,
) -> Result<(), GovernorError> {
    let mut core = Core::new().map_err(GovernorError::CannotSpawnEventLoop)?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut shutdown_tx = Some(shutdown_tx);

    // Make the remote available outside.
    *state.remote.write() = Some(core.remote());
    debug!("spawned trove governor");

    // spawn a stream that just listens in on the events sent into the
    // trove governor so we can figure out when to terminate the thread
    // or do similar things.
    core.handle().spawn(rx.for_each(move |event| {
        match event {
            GovernorEvent::Shutdown => {
                if let Some(shutdown_tx) = shutdown_tx.take() {
                    shutdown_tx.send(()).ok();
                }
            }
        }
        Ok(())
    }));

    core.run(shutdown_rx).ok();
    *state.remote.write() = None;
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
