use std::sync::Arc;

use hyper;
use hyper::StatusCode;
use parking_lot::RwLock;
use serde_json;
use tokio_core::reactor::Remote;

use semaphore_aorta::{AortaConfig, ApiErrorResponse, RequestManager};

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
    remote: RwLock<Option<Remote>>,
    request_manager: Arc<RequestManager>,
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
                config: config.clone(),
                remote: RwLock::new(None),
                request_manager: Arc::new(RequestManager::new(config)),
            }),
        }
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
}
