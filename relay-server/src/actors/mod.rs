//! Defines all actors of the relay.
//!
//! Actors require an actix system to run. The system can be started using the
//! [`Controller`](controller::Controller) actor, which will also listen for shutdown signals and
//! trigger a graceful shutdown. Note that actors must implement a handler for the
//! [`Shutdown`](controller::Shutdown) message and register with the controller to receive this
//! signal. See the struct level documentation for more information.
//!
//! The web server is wrapped by the [`Server`](server::Server) actor. It starts the actix http web
//! server and relays the graceful shutdown signal. Internally, it creates several other actors
//! comprising the service state:
//!
//!  - [`ProjectCache`](project_cache::ProjectCache): A cache that serves queries for project
//!    configurations. Its requests are debounced and batched based on a configured interval (100ms
//!    by default). Also, missing projects are cached for some time.
//!  - [`EnvelopeManager`](envelopes::EnvelopeManager) and
//!    [`EnvelopeProcessor`](envelopes::EnvelopeProcessor): Handle a queue of envelopes, verify
//!    their projects, execute PII stripping and finally send the envelope to the upstream. The
//!    processor is spawned in multiple synchronous worker threads (via `SyncArbiter`).
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

pub mod connector;
pub mod controller;
pub mod envelopes;
pub mod healthcheck;
pub mod outcome;
pub mod project;
pub mod project_cache;
pub mod project_local;
pub mod project_upstream;
pub mod relays;
pub mod server;
pub mod upstream;

#[cfg(feature = "processing")]
pub mod project_redis;
#[cfg(feature = "processing")]
pub mod store;
