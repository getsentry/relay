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
//! - `ssl` _(default)_: Enables SSL support using `native-tls`.
//! - `processing`: Includes event ingestion and processing functionality. This should only be
//!   specified when compiling Relay as Sentry service. Standalone Relays do not need this feature.
//!
//! # Workspace Crates
//!
//! Relay is split into the following workspace crates:
//!
//!  - `relay`: Main entry point and command line interface.
//!  - [`relay-auth`]: Authentication and crypto.
//!  - [`relay-cabi`]: C-bindings for exposing functionality to Python.
//!  - [`relay-common`]: Common utilities and crate re-exports.
//!  - [`relay-config`]: Configuration for the CLI and server.
//!  - [`relay-ffi`]: Utilities for error handling in FFI bindings.
//!  - [`relay-ffi-macros`]: Macros for error handling in FFI bindings.
//!  - [`relay-filter`]: Inbound data filters.
//!  - [`relay-general`]: Event protocol, normalization and data scrubbing.
//!  - [`relay-log`]: Error reporting and logging.
//!  - [`relay-quotas`]: Sentry quotas and rate limiting.
//!  - [`relay-redis`]: Pooled Redis and Redis cluster abstraction.
//!  - [`relay-server`]: Endpoints and services.
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
//! [`relay-cabi`]: ../relay_cabi/index.html
//! [`relay-common`]: ../relay_common/index.html
//! [`relay-config`]: ../relay_config/index.html
//! [`relay-ffi`]: ../relay_ffi/index.html
//! [`relay-ffi-macros`]: ../relay_ffi_macros/index.html
//! [`relay-filter`]: ../relay_filter/index.html
//! [`relay-general`]: ../relay_general/index.html
//! [`relay-log`]: ../relay_log/index.html
//! [`relay-quotas`]: ../relay_quotas/index.html
//! [`relay-redis`]: ../relay_redis/index.html
//! [`relay-server`]: ../relay_server/index.html
//! [`document-metrics`]: ../document_metrics/index.html
//! [`generate-schema`]: ../generate_schema/index.html
//! [`process-event`]: ../process_event/index.html
//! [`scrub-minidump`]: ../scrub_minidump/index.html

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod cli;
mod cliapp;
mod setup;
mod utils;

use std::process;

use relay_log::Hub;

pub fn main() {
    // on non windows machines we want to initialize the openssl envvars based on
    // what openssl probe tells us.  We will eventually stop doing that if we
    // kill openssl.
    #[cfg(not(windows))]
    {
        use openssl_probe::init_ssl_cert_env_vars;
        init_ssl_cert_env_vars();
    }

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
