use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use futures::{Future, IntoFuture, Stream};
use hyper;
use hyper::client::{Client, HttpConnector};
use hyper::header::ContentType;
use hyper::{Chunk, Request, StatusCode};
use hyper_tls::HttpsConnector;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde_json;
use tokio_core::reactor::{Handle, Remote};

use semaphore_aorta::{AortaConfig, ApiErrorResponse, ApiRequest, ProjectState, RequestManager};
use semaphore_common::ProjectId;

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
/// state or context.
pub struct Trove {
    state: Arc<TroveState>,
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
                remote: RwLock::new(None),
                request_manager: Arc::new(RequestManager::new(config)),
            }),
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
}

impl TroveState {
    /// Returns the aorta config.
    pub fn config(&self) -> Arc<AortaConfig> {
        self.config.clone()
    }

    /// Returns the current request manager.
    pub fn request_manager(&self) -> Arc<RequestManager> {
        self.request_manager.clone()
    }
}
