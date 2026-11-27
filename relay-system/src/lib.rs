//! Foundational system components for Relay's services.
//!
//! Relay's system is based on [`tokio`]. To use any of these components, ensure a tokio runtime is
//! available.
//!
//! # Services
//!
//! The basic building block in Relay are asynchronous [`Service`]s. Each service implements an
//! [`Interface`], which consists of one or many messages that can be sent to the service using its
//! [`Addr`]. See the docs of these types for more information on how to implement and use them.
//!
//! # Shutdown
//!
//! The [`shutdown`] function provides a unified interface to await a shutdown signal.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod controller;
mod monitor;
mod runtime;
mod service;
mod statsd;

pub use self::controller::*;
pub use self::monitor::*;
pub use self::runtime::*;
pub use self::service::*;
