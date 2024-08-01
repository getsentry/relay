//! Defines all services of Relay.
//!
//! Services require a tokio to run, see [`relay_system`] and particularly
//! [`Controller`](relay_system::Controller) for more information.
//!
//! The web server is wrapped by [`HttpServer`](server::HttpServer). It starts the actual web server
//! and dispatches the graceful shutdown signal. Internally, it creates several other services
//! comprising the service state:
//!
//!  - [`ProjectCache`](project_cache::ProjectCache): A cache that serves queries for project
//!    configurations. Its requests are debounced and batched based on a configured interval (100ms
//!    by default). Also, missing projects are cached for some time.
//!  - [`EnvelopeProcessor`](processor::EnvelopeProcessor): A worker pool for CPU-intensive tasks.
//!    Most importantly, this implements envelope processing to verify their projects, execute PII
//!    stripping and finally send the envelope to the upstream.
//!  - [`UpstreamRelay`](upstream::UpstreamRelay): Abstraction for communication with the upstream
//!    (either another Relay or Sentry). It manages an internal client connector to throttle
//!    requests and ensures this relay is authenticated before sending queries (e.g. project config
//!    or public keys).
//!
//! # Example
//!
//! ```ignore
//! use relay_server::controller::Controller;
//! use relay_server::server::Server;
//!
//! Controller::run(|| Server::start())
//!     .expect("failed to start relay");
//! ```
pub mod buffer;
pub mod cogs;
pub mod global_config;
pub mod health_check;
pub mod metrics;
pub mod outcome;
pub mod outcome_aggregator;
pub mod processor;
pub mod project;
pub mod project_cache;
pub mod project_local;
pub mod project_upstream;
pub mod relays;
pub mod server;
pub mod spooler;
pub mod stats;
pub mod test_store;
pub mod upstream;

#[cfg(feature = "processing")]
pub mod project_redis;
#[cfg(feature = "processing")]
pub mod store;
