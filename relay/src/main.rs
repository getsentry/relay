//! <p align="center">
//!   <img src="https://github.com/getsentry/relay/blob/master/artwork/relay-logo.png?raw=true" alt="Relay" width="480">
//! </p>
//!
//! Relay is a standalone service for scrubbing personal information and improving event submission
//! response time. It acts as a middle layer between your application and Sentry.io. For information
//! on setting up Relay in front of Sentry, see:
//!
//! [Relay documentation]
//!
//! # Event Ingestion
//!
//! Relay is also the primary event ingestion system at Sentry. When compiled with the `processing`
//! feature and configured for ingestion, Relay ships functionality for:
//!
//!  - Acknowledging and handling web requests on store endpoints
//!  - Applying rate limits and inbound data filters
//!  - Data scrubbing of event payloads and attachments
//!  - Validation and normalization of malformed or outdated data
//!  - Forwarding of all information into the event processing pipeline
//!
//! # Feature Flags
//!
//! - `processing`: Includes event ingestion and processing functionality. This should only be
//!   specified when compiling Relay as Sentry service. Standalone Relays do not need this feature.
//! - `crash-handler`: Allows native crash reporting for segfaults and out-of-memory situations when
//!   internal error reporting to Sentry is enabled.
//!
//! # Workspace Crates
//!
//! Relay is split into the following workspace crates:
//!
//!  - `relay`: Main entry point and command line interface.
//!  - [`relay-auth`]: Authentication and crypto.
//!  - [`relay-aws-extension`]: AWS extension implementation for Sentry's AWS Lambda layer.
//!  - [`relay-base-schema`]: Basic types for Relay's API schema used across multiple services.
//!  - [`relay-cabi`]: C-bindings for exposing functionality to Python.
//!  - [`relay-common`]: Common utilities and crate re-exports.
//!  - [`relay-config`]: Static configuration for the CLI and server.
//!  - [`relay-crash`]: Crash reporting for the Relay server.
//!  - [`relay-dynamic-config`]: Dynamic configuration passed from Sentry.
//!  - [`relay-event-derive`]: Derive for visitor traits on the Event schema.
//!  - [`relay-event-normalization`]: Event normalization and processing.
//!  - [`relay-event-schema`]: Event schema (Error, Transaction, Security) and types for event processing.
//!  - [`relay-ffi`]: Utilities for error handling in FFI bindings.
//!  - [`relay-ffi-macros`]: Macros for error handling in FFI bindings.
//!  - [`relay-filter`]: Inbound data filters.
//!  - [`relay-jsonschema-derive`]: Derive for JSON schema on Relay protocol types.
//!  - [`relay-kafka`]: Kafka-related functionality.
//!  - [`relay-log`]: Error reporting and logging.
//!  - [`relay-metrics`]: Metrics protocol and processing.
//!  - [`relay-monitors`]: Monitors protocol and processing for Sentry.
//!  - [`relay-pii`]: Scrubbing of personally identifiable information (PII) from events
//!  - [`relay-profiling`]: Profiling protocol and processing.
//!  - [`relay-protocol`]: Types and traits for building JSON-based protocols and schemas.
//!  - [`relay-protocol-derive`]: Derives for Relay's protocol traits.
//!  - [`relay-quotas`]: Sentry quotas and rate limiting.
//!  - [`relay-redis`]: Pooled Redis and Redis cluster abstraction.
//!  - [`relay-replays`]: Session replay protocol and processing.
//!  - [`relay-sampling`]: Dynamic sampling functionality.
//!  - [`relay-server`]: Endpoints and services.
//!  - [`relay-statsd`]: High-level StatsD metric client for internal measurements.
//!  - [`relay-system`]: Foundational system components for Relay's services.
//!  - [`relay-test`]: Helpers for testing the web server and services.
//!  - [`relay-ua`]: User agent parser with built-in rules.
//!
//! # Tools
//!
//! Complementary to the Relay binary, the workspace contains a set of tools to interface with some
//! of Relay's functionality or to export information:
//!
//!  - [`document-metrics`]: Generate documentation for metrics.
//!  - [`generate-schema`]: Generate an event payload schema file.
//!  - [`process-event`]: Process a Sentry event payload.
//!  - [`scrub-minidump`]: Scrub PII from a Minidump file.
//!
//! [relay documentation]: https://docs.sentry.io/product/relay/
//! [`relay-auth`]: ../relay_auth/index.html
//! [`relay-aws-extension`]: ../relay_aws_extension/index.html
//! [`relay-base-schema`]: ../relay_base_schema/index.html
//! [`relay-cabi`]: ../relay_cabi/index.html
//! [`relay-common`]: ../relay_common/index.html
//! [`relay-config`]: ../relay_config/index.html
//! [`relay-crash`]: ../relay_crash/index.html
//! [`relay-dynamic-config`]: ../relay_dynamic_config/index.html
//! [`relay-event-derive`]: ../relay_event_derive/index.html
//! [`relay-event-normalization`]: ../relay_event_normalization/index.html
//! [`relay-event-schema`]: ../relay_event_schema/index.html
//! [`relay-ffi`]: ../relay_ffi/index.html
//! [`relay-ffi-macros`]: ../relay_ffi_macros/index.html
//! [`relay-filter`]: ../relay_filter/index.html
//! [`relay-jsonschema-derive`]: ../relay_jsonschema_derive/index.html
//! [`relay-kafka`]: ../relay_kafka/index.html
//! [`relay-log`]: ../relay_log/index.html
//! [`relay-metrics`]: ../relay_metrics/index.html
//! [`relay-monitors`]: ../relay_monitors/index.html
//! [`relay-pii`]: ../relay_pii/index.html
//! [`relay-profiling`]: ../relay_profiling/index.html
//! [`relay-protocol`]: ../relay_protocol/index.html
//! [`relay-protocol-derive`]: ../relay_protocol_derive/index.html
//! [`relay-quotas`]: ../relay_quotas/index.html
//! [`relay-redis`]: ../relay_redis/index.html
//! [`relay-replays`]: ../relay_replays/index.html
//! [`relay-sampling`]: ../relay_sampling/index.html
//! [`relay-server`]: ../relay_server/index.html
//! [`relay-statsd`]: ../relay_statsd/index.html
//! [`relay-system`]: ../relay_system/index.html
//! [`relay-test`]: ../relay_test/index.html
//! [`relay-ua`]: ../relay_ua/index.html
//! [`document-metrics`]: ../document_metrics/index.html
//! [`generate-schema`]: ../generate_schema/index.html
//! [`process-event`]: ../process_event/index.html
//! [`scrub-minidump`]: ../scrub_minidump/index.html

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod cli;
mod cliapp;
mod setup;
mod utils;

use std::process;

use relay_log::Hub;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub fn main() {
    let exit_code = match cli::execute() {
        Ok(()) => 0,
        Err(err) => {
            relay_log::ensure_error(&err);
            1
        }
    };

    Hub::current().client().map(|x| x.close(None));
    process::exit(exit_code);
}
