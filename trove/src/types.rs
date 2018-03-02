use std::io;
use std::fmt;
use std::thread;
use std::sync::Arc;
use std::collections::HashMap;

use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use parking_lot::{Mutex, RwLock};
use tokio_core::reactor::{Core, Handle, Remote};
use futures::{Future, IntoFuture, Stream};
use futures::sync::{mpsc, oneshot};
use hyper;
use hyper::{Chunk, Method, Request, StatusCode};
use hyper::header::ContentType;
use hyper::client::{Client, HttpConnector};
use hyper_tls::HttpsConnector;

use smith_common::ProjectId;
use smith_aorta::{AortaApiRequest, AortaConfig, ProjectState};

use auth::{spawn_authenticator, AuthError, AuthState};
use heartbeat::spawn_heartbeat;

/// Represents an event that can be sent to the governor.
#[derive(Debug)]
pub(crate) enum GovernorEvent {
    /// Tells the trove governor to shut down.
    Shutdown,
    /// The trove authenticated successfully.
    Authenticated,
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
    /// Raised if the authentication handler could not spawn.
    #[fail(display = "governor could not start authentication")]
    CannotSpawnAuthenticator(#[cause] AuthError),
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

/// An error response from an api.
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct ApiErrorResponse {
    detail: Option<String>,
}

impl fmt::Display for ApiErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref detail) = self.detail {
            write!(f, "{}", detail)
        } else {
            write!(f, "no error details")
        }
    }
}

/// An internal helper struct that represents the shared state of the
/// trove.  An `Arc` of the state is passed around various systems.
#[derive(Debug)]
pub(crate) struct TroveState {
    pub config: Arc<AortaConfig>,
    states: RwLock<HashMap<ProjectId, Arc<ProjectState>>>,
    governor_tx: RwLock<Option<mpsc::UnboundedSender<GovernorEvent>>>,
    remote: RwLock<Option<Remote>>,
    auth_state: RwLock<AuthState>,
}

/// Convenience context that never crosses threads.
#[derive(Debug)]
pub(crate) struct TroveContext {
    handle: Handle,
    state: Arc<TroveState>,
    client: Client<HttpsConnector<HttpConnector>>,
}

impl TroveContext {
    /// Returns the handle of the core loop.
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

    /// Sends an API request and handles data.
    pub fn perform_aorta_request<D: DeserializeOwned + 'static>(
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
                    } else if res.headers()
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
    pub fn aorta_request<T: AortaApiRequest>(
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
/// synchronized.  It also typically has a governing thread running which
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
                config: config,
                governor_tx: RwLock::new(None),
                remote: RwLock::new(None),
                auth_state: RwLock::new(AuthState::Unknown),
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
    /// fact not exist or the relay has no permissions to access the data.
    pub fn state_for_project(&self, project_id: ProjectId) -> Arc<ProjectState> {
        // state already exists, return it.
        {
            let states = self.state.states.read();
            if let Some(ref rv) = states.get(&project_id) {
                return (*rv).clone();
            }
        }

        // insert an empty state
        {
            let state = ProjectState::new(project_id, self.state.config.clone());
            self.state
                .states
                .write()
                .insert(project_id, Arc::new(state));
        }
        (*self.state.states.read().get(&project_id).unwrap()).clone()
    }

    /// Returns `true` if the trove is governed.
    pub fn is_governed(&self) -> bool {
        self.state.remote.read().is_some()
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

        let state = self.state.clone();
        debug!("spawning trove governor");

        *self.join_handle.lock() = Some(thread::Builder::new()
            .name("trove-governor".into())
            .spawn(move || {
                let (tx, rx) = mpsc::unbounded::<GovernorEvent>();
                *state.governor_tx.write() = Some(tx);
                run_governor(state, rx)
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

    /// Returns the remote of the underlying governor.
    ///
    /// In case the governor is not running this will panic.
    pub fn remote(&self) -> Remote {
        self.state.remote()
    }
}

impl Drop for Trove {
    fn drop(&mut self) {
        self.abdicate().unwrap();
    }
}

impl TroveState {
    /// Returns the current auth state.
    pub fn auth_state(&self) -> AuthState {
        *self.auth_state.read()
    }

    /// Transitions the auth state.
    pub fn set_auth_state(&self, new_state: AuthState) {
        let mut auth_state = self.auth_state.write();
        let old_state = *auth_state;
        if old_state != new_state {
            info!("changing auth state: {:?} -> {:?}", old_state, new_state);
            *auth_state = new_state;
        }

        // if we transition from non authenticated to authenticated
        // we emit an event.
        if !old_state.is_authenticated() && new_state.is_authenticated() {
            self.emit_governor_event(GovernorEvent::Authenticated);
        }
    }

    /// Emits an event to the governor.
    pub fn emit_governor_event(&self, event: GovernorEvent) -> bool {
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

    let ctx = Arc::new(TroveContext {
        handle: core.handle(),
        state: state.clone(),
        client: Client::configure()
            .connector(HttpsConnector::new(4, &core.handle()).unwrap())
            .build(&core.handle()),
    });

    // spawn a stream that just listens in on the events sent into the
    // trove governor so we can figure out when to terminate the thread
    // or do similar things.
    let loop_ctx = ctx.clone();
    core.handle().spawn(rx.for_each(move |event| {
        match event {
            GovernorEvent::Shutdown => {
                if let Some(shutdown_tx) = shutdown_tx.take() {
                    shutdown_tx.send(()).ok();
                }
            }
            GovernorEvent::Authenticated => {
                spawn_heartbeat(loop_ctx.clone());
            }
        }
        Ok(())
    }));

    // spawn the authentication handler
    spawn_authenticator(ctx.clone());

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
