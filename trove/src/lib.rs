//! This library implements helpers for managing various upstream
//! aortas.  It's used by the server as a central point to buffer up
//! changes that need to be flushed with the heartbeat and stores
//! config updates from upstream.
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate futures;
#[macro_use]
extern crate log;
extern crate parking_lot;
extern crate smith_aorta;
extern crate smith_common;
extern crate tokio_core;

mod auth;
mod types;

pub use types::*;
