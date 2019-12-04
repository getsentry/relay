//! Web server endpoints.
//!
//! This module contains implementations for all supported relay endpoints, as well as a generic
//! `forward` endpoint that sends unknown requests to the upstream.

mod common;

pub mod events;
pub mod forward;
pub mod healthcheck;
pub mod minidump;
pub mod project_configs;
pub mod public_keys;
pub mod security_report;
pub mod store;
