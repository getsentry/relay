//! The Sentry relay server application.
//!
//! This module contains the [`run`] function which starts the relay server. It responds on
//! multiple supported endpoints, serves queries to downstream relays and send received events to
//! the upstream.
//!
//! See the documentation of the `Config` struct for more information on configuration options.
//!
//! [`run`]: fn.run.html
#![warn(missing_docs)]

mod actors;
mod body;
mod constants;
mod endpoints;
mod envelope;
mod extractors;
mod middlewares;
mod service;
mod utils;

#[cfg(feature = "processing")]
mod quotas;

use crate::actors::controller::Controller;
use crate::actors::server::Server;
use semaphore_common::Config;

pub use crate::actors::controller::ServerError;

/// Runs a relay web server and spawns all internal worker threads.
///
/// This effectively boots the entire server application. It blocks the current thread until a
/// shutdown signal is received or a fatal error happens. Behavior of the server is determined by
/// the `config` passed into this funciton.
pub fn run(config: Config) -> Result<(), ServerError> {
    // Run the controller and block until a shutdown signal is sent to this process. This will
    // create an actix system, start a web server and run all relevant actors inside. See the
    // `actors` module documentation for more information on all actors.
    Controller::run(|| Server::start(config))
}
