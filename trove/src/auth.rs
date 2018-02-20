use std::io::{self, Write};
use std::sync::Arc;

use futures::{Future, Stream};

use types::TroveContext;

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
    let mut state = AuthState::Unknown;

    let uri = "https://httpbin.org/ip".parse().unwrap();
    let work = ctx.http_client().get(uri).and_then(|res| {
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
