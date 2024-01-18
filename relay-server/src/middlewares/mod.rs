//! Middlewares for the HTTP server.
//!
//! This module exposes tower [layers](tower::Layer) and related utilities to configure the
//! axum/hyper HTTP server. Most of the middlewares should be registered as a layer on the
//! [`Router`](axum::Router).
//!
//! See the server startup in [`HttpServer`](crate::services::server::HttpServer) for where these
//! middlewares are registered.

mod cors;
mod decompression;
mod handle_panic;
mod metrics;
mod normalize_path;
mod trace;

pub use self::cors::*;
pub use self::decompression::*;
pub use self::handle_panic::*;
pub use self::metrics::*;
pub use self::normalize_path::*;
pub use self::trace::*;
