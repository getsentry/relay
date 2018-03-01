use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use hyper::{Chunk, Method};
use tokio_core::reactor::Timeout;

use types::TroveContext;

use smith_aorta::{RegisterChallenge, RegisterRequest};
use serde_json;

/// Represents the current auth state of the trove.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum AuthState {
    Unknown,
    RegisterRequestChallenge,
    RegisterChallengeResponse,
    Registered,
    Error,
}

#[derive(Fail, Debug)]
#[fail(display = "could not authenticate")]
pub struct AuthError;

pub(crate) fn spawn_authenticator(ctx: Arc<TroveContext>) {
    let state = ctx.state();
    state.set_auth_state(AuthState::Unknown);
    debug!("starting authenticator");
    register_with_upstream(ctx);
}

fn register_with_upstream(ctx: Arc<TroveContext>) {
    let config = &ctx.state().config;

    info!("registering with upstream (upstream={})", &config.upstream);
    let state = ctx.state();

    state.set_auth_state(AuthState::RegisterRequestChallenge);

    let inner_ctx_success = ctx.clone();
    let inner_ctx_failure = ctx.clone();
    let reg_req = RegisterRequest::new(config.relay_id(), config.public_key());
    ctx.handle().spawn(
        ctx.aorta_request(&reg_req)
            .and_then(|challenge| {
                info!("got register challenge (token = {})", challenge.token());
                send_register_challenge_response(inner_ctx_success, challenge);
                Ok(())
            })
            .or_else(|err| {
                // XXX: do not schedule retries for fatal errors
                error!("authentication encountered error: {}", &err);
                let state = inner_ctx_failure.state();
                state.set_auth_state(AuthState::Error);
                schedule_auth_retry(inner_ctx_failure);
                Err(())
            }),
    );
}

fn send_register_challenge_response(ctx: Arc<TroveContext>, challenge: RegisterChallenge) {
    info!("sending register challenge response");
    let state = ctx.state();

    state.set_auth_state(AuthState::RegisterChallengeResponse);

    let inner_ctx_success = ctx.clone();
    let inner_ctx_failure = ctx.clone();
    let challenge_resp_req = challenge.create_response();
    ctx.handle().spawn(
        ctx.aorta_request(&challenge_resp_req)
            .and_then(move |_| {
                info!("relay successfully registered with upstream");
                let state = inner_ctx_success.state();
                state.set_auth_state(AuthState::Registered);
                Ok(())
            })
            .or_else(|err| {
                // XXX: do not schedule retries for fatal errors
                error!("failed to register relay with upstream: {}", &err);
                let state = inner_ctx_failure.state();
                state.set_auth_state(AuthState::Error);
                schedule_auth_retry(inner_ctx_failure);
                Err(())
            }),
    );
}

fn schedule_auth_retry(ctx: Arc<TroveContext>) {
    info!("scheduling authentication retry");
    let inner_ctx = ctx.clone();
    ctx.handle().spawn(
        Timeout::new(Duration::from_secs(15), &ctx.handle())
            .unwrap()
            .and_then(|_| {
                register_with_upstream(inner_ctx);
                Ok(())
            })
            .or_else(|_| -> Result<_, _> {
                panic!("failed to schedule register");
            }),
    );
}
