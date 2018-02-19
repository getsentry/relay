use std::sync::Arc;

use tokio_core::reactor::Handle;

use smith_aorta::test_req;

use types::TroveState;

/// Represents the current auth state of the trove.
#[derive(Debug)]
pub enum AuthState {
    Unknown,
    Unregistered,
    Registering,
    Unauthenticated,
    Authenticated,
}

#[derive(Fail, Debug)]
#[fail(display="could not authenticate")]
pub struct AuthError;

pub(crate) fn spawn_authenticator(handle: Handle, trove_state: Arc<TroveState>) {
    debug!("Starting authenticator");
    let mut state = AuthState::Unknown;
    test_req(handle);
}
