//! Foundational system components for Relay's services.
//!
//! Actors require an actix system to run. The system can be started using the [`Controller`] actor,
//! which will also listen for shutdown signals and trigger a graceful shutdown. Note that actors
//! must implement a handler for the [`Shutdown`] message and register with the controller to
//! receive this signal. See the struct level documentation for more information.
//!
//! See the [`Controller`] struct for more information.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

pub mod compat;
mod controller;
pub mod service;

pub use self::controller::*;
