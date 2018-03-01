use std::io::{self, Write};
use std::sync::Arc;

use futures::{Future, Stream};
use hyper::{Chunk, Method};

use types::TroveContext;

use smith_aorta::{RegisterChallenge, RegisterRequest};
use serde_json;

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
#[fail(display = "could not authenticate")]
pub struct AuthError;

pub(crate) fn spawn_authenticator(ctx: &TroveContext) {
    debug!("Starting authenticator");
    check_relay_state(ctx);
}

fn check_relay_state(ctx: &TroveContext) {
    let mut state = AuthState::Unknown;
    let config = &ctx.state().config;

    let reg_req = RegisterRequest::new(config.relay_id(), config.public_key());
    ctx.handle().spawn(
        ctx.aorta_request(&reg_req)
            .and_then(|rv| {
                println!("{:?}", rv);
                Ok(())
            })
            .or_else(|err| {
                println!("error: {}", err);
                Err(())
            }),
    );
}
