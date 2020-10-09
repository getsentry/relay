//! Defines all actors of the relay.
//!
//! Actors require an actix system to run. The system can be started using the [`Controller`]
//! actor, which will also listen for shutdown signals and trigger a graceful shutdown. Note that
//! actors must implement a handler for the [`Shutdown`] message and register with the controller
//! to receive this signal. See the struct level documentation for more information.
//!
//! The web server is wrapped by the [`Server`] actor. It starts the actix http web server and
//! relays the graceful shutdown signal. Internally, it creates several other actors comprising the
//! service state:
//!
//!  - [`ProjectCache`] and [`KeyCache`]: Two caches that serve queries for project configurations
//!    and relay public keys, respectively. Their requests are debounced and batched based on a
//!    configured interval (100ms by default). Also, missing projects and keys are cached for some
//!    time.
//!  - [`EventManager`] and [`EventProcessor`]: Handle a queue of events, verify their projects,
//!    execute PII stripping and finally send the event to the upstream. The processor is spawned
//!    in multiple synchronous worker threads (via `SyncArbiter`).
//!  - [`UpstreamRelay`]: Abstraction for communication with the upstream (either another Relay or
//!    Sentry). It manages an internal client connector to throttle requests and ensures this relay
//!    is authenticated before sending queries (e.g. project config or public keys).
//!
//! ### Example
//!
//! ```ignore
//! use relay_server::controller::Controller;
//! use relay_server::server::Server;
//!
//! Controller::run(|| Server::start())
//!     .expect("failed to start relay");
//! ```
//!
//! [`Controller`]: controller/struct.Controller.html
//! [`Shutdown`]: controller/struct.Shutdown.html
//! [`Server`]: controller/struct.Server.html
//! [`ProjectCache`]: controller/struct.ProjectCache.html
//! [`KeyCache`]: controller/struct.KeyCache.html
//! [`EventManager`]: controller/struct.EventManager.html
//! [`EventProcessor`]: controller/struct.EventProcessor.html
//! [`UpstreamRelay`]: controller/struct.UpstreamRelay.html

pub mod connector;
pub mod controller;
pub mod events;
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
