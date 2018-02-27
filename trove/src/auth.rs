use std::io::{self, Write};
use std::sync::Arc;

use futures::{Future, Stream};
use hyper::Method;

use types::TroveContext;

use smith_aorta::RegisterRequest;

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
    let req = config.prepare_aorta_req(Method::Post, "relays/", &reg_req);
    let work = ctx.http_client().request(req).and_then(|res| {
        println!("Response: {}", res.status());

        res.body().for_each(|chunk| {
            io::stdout()
                .write_all(&chunk)
                .map(|_| ())
                .map_err(From::from)
        })
    });
    ctx.handle().spawn(work.map_err(|_| ()));
}
