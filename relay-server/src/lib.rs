//! The Sentry relay server application.
//!
//! This module contains the [`run`] function which starts the relay server. It responds on
//! multiple supported endpoints, serves queries to downstream relays and send received events to
//! the upstream.
//!
//! See the [`Config`] documentation for more information on configuration options.
//!
//! # Path of an Event through Relay
//!
//! ## Overview
//!
//! Simplified overview of event ingestion (ignores snuba/postprocessing):
//!
//! ```mermaid
//! graph LR
//!
//! loadbalancer(Load Balancer)
//! relay(Relay)
//! projectconfigs("Project config endpoint (in Sentry)")
//! ingestconsumer(Ingest Consumer)
//! outcomesconsumer(Outcomes Consumer)
//! preprocess{"<code>preprocess_event</code><br>(just a function call now)"}
//! process(<code>process_event</code>)
//! save(<code>save_event</code>)
//!
//! loadbalancer-->relay
//! relay---projectconfigs
//! relay-->ingestconsumer
//! relay-->outcomesconsumer
//! ingestconsumer-->preprocess
//! preprocess-->process
//! preprocess-->save
//! process-->save
//!
//! ```
//!
//! ## Processing enabled vs not?
//!
//! Relay can run as part of a Sentry installation, such as within `sentry.io`'s
//! infrastructure, or next to the application as a forwarding proxy. A lot of
//! steps described here are skipped or run in a limited form when Relay is *not*
//! running with processing enabled:
//!
//! *  Event normalization does different (less) things.
//!
//! *  In certain modes, project config is not fetched from Sentry at all (but
//!    rather from disk or filled out with defaults).
//!
//! *  Events are forwarded to an HTTP endpoint instead of being written to Kafka.
//!
//! *  Rate limits are not calculated using Redis, instead Relay just honors 429s
//!    from previously mentioned endpoint.
//!
//! *  Filters are not applied at all.
//!
//! ## Inside the endpoint
//!
//! When an SDK hits `/api/X/store` on Relay, the code in
//! `server/src/endpoints/store.rs` is called before returning a HTTP response.
//!
//! That code looks into an in-memory cache to answer basic information about a project such as:
//!
//! *  Does it exist? Is it suspended/disabled?
//!
//! *  Is it rate limited right now? If so, which key is rate limited?
//!
//! *  Which DSNs are valid for this project?
//!
//! Some of the data for this cache is coming from the [projectconfigs
//! endpoint](https://github.com/getsentry/sentry/blob/c868def30e013177383f8ca5909090c8bdbd8f6f/src/sentry/api/endpoints/relay_projectconfigs.py).
//! It is refreshed every couple of minutes, depending on configuration (`project_expiry`).
//!
//! If the cache is fresh, we may return a `429` for rate limits or a `4xx` for
//! invalid auth information.
//!
//! That cache might be empty or stale. If that is the case, Relay does not
//! actually attempt to populate it at this stage. **It just returns a `200` even
//! though the event might be dropped later.** This implies:
//!
//! *  The first store request that runs into a rate limit doesn't actually result
//!    in a `429`, but a subsequent request will (because by that time the project
//!    cache will have been updated).
//!
//! *  A store request for a non-existent project may result in a `200`, but
//!    subsequent ones will not.
//!
//! *  A store request with wrong auth information may result in a `200`, but
//!    subsequent ones will not.
//!
//! *  Filters are also not applied at this stage, so **a filtered event will
//!    always result in a `200`**. This matches the Python behavior since [a while
//!    now](https://github.com/getsentry/sentry/pull/14561).
//!
//! These examples assume that a project receives one event at a time. In practice
//! one may observe that a highly concurrent burst of store requests for a single
//! project results in `200 OK`s only. However, a multi-second flood of incoming
//! events should quickly result in eventually consistent and correct status codes.
//!
//! The response is completed at this point. All expensive work (such as talking to
//! external services) is deferred into a background task. Except for responding to
//! the HTTP request, there's no I/O done in the endpoint in any form. We didn't
//! even hit Redis to calculate rate limits.
//!
//! ### Summary
//!
//! The HTTP response returned is just a best-effort guess at what the actual
//! outcome of the event is going to be. We only return a `4xx` code if we know that
//! the response will fail (based on cached information), if we don't we return a
//! 200 and continue to process the event asynchronously. This asynchronous
//! processing used to happen synchronously in the Python implementation of
//! `StoreView`.
//!
//! The effect of this is that the server will respond much faster that before but
//! we might return 200 for events that will ultimately not be accepted.
//!
//! Generally Relay will return a 200 in many more situations than the old
//! `StoreView`.
//!
//! ## The background task
//!
//! The HTTP response is out by now. The rest of what used to happen synchronously in the
//! Python `StoreView` is done asynchronously, but still in the same process.
//!
//! So, now to the real work:
//!
//! 1.  **Project config is fetched.** If the project cache is stale or missing, we
//!     fetch it. We may wait a couple milliseconds (`batch_interval`) here to be
//!     able to batch multiple project config fetches into the same HTTP request to
//!     not overload Sentry too much.
//!
//!     At this stage Relay may drop the event because it realized that the DSN was
//!     invalid or the project didn't even exist. The next incoming event will get a
//!     proper 4xx status code.
//!
//! 1.  **The event is parsed.** In the endpoint we only did decompression, a basic
//!     JSON syntax check, and extraction of the event ID to be able to return it as
//!     part of the response.
//!
//!     Now we create an `Event` struct, which conceptually is the equivalent to
//!     parsing it into a Python dictionary: We allocate more memory.
//!
//! 1.  **The event is normalized.** Event normalization is probably the most
//!     CPU-intensive task running in Relay. It discards invalid data, moves data
//!     from deprecated fields to newer fields and generally just does schema
//!     validation.
//!
//! 1.  **Filters ("inbound filters") are applied.** Event may be discarded because of IP
//!     addresses, patterns on the error message or known web crawlers.
//!
//! 1.  **Exact rate limits ("quotas") are applied.** `is_rate_limited.lua` is
//!     executed on Redis. The input parameters for `is_rate_limited.lua` ("quota
//!     objects") are part of the project config. See [this pull
//!     request](https://github.com/getsentry/sentry/pull/14558) for an explanation
//!     of what quota objects are.
//!
//!     The event may be discarded here. If so, we write the rate limit info
//!     (reason and expiration timestamp) into the in-memory project cache so that
//!     the next store request returns a 429 in the endpoint and doesn't hit Redis
//!     at all.
//!
//!     This contraption has the advantage that suspended or permanently
//!     rate-limited projects are very cheap to handle, and do not involve external
//!     services (ignoring the polling of the project config every couple of
//!     minutes).
//!
//! 1.  **The event is datascrubbed.** We have a PII config (new format) and a
//!     datascrubbing config (old format, converted to new format on the fly) as
//!     part of the project config fetched from Sentry.
//!
//! 1.  **Event is written to Kafka.**
//!
//! **Note:** If we discard an event at any point, an outcome is written to Kafka
//! if Relay is configured to do so.
//!
//! ### Summary
//!
//! For events that returned a `200` we spawn an in-process background task
//! that does the rest of what the old `StoreView` did.
//!
//! This task updates in-memory state for rate limits and disabled
//! projects/keys.
//!
//! ## The outcomes consumer
//!
//! Outcomes are small messages in Kafka that contain an event ID and information
//! about whether that event was rejected, and if so, why.
//!
//! The outcomes consumer is mostly responsible for updating (user-visible)
//! counters in Sentry (buffers/counters and tsdb, which are two separate systems).
//!
//! ## The ingest consumer
//!
//! The ingest consumer reads accepted events from Kafka, and also updates some
//! stats. Some of *those* stats are billing-relevant.
//!
//! Its main purpose is to do what `insert_data_to_database` in Python store did:
//! Call `preprocess_event`, after which comes sourcemap processing, native
//! symbolication, grouping, snuba and all that other stuff that is of no concern
//! to Relay.
//!
//! ## Sequence diagram of components inside Relay
//!
//! ```mermaid
//! sequenceDiagram
//! participant sdk as SDK
//! participant endpoint as Endpoint
//! participant projectcache as ProjectCache
//! participant envelopemanager as EnvelopeManager
//! participant cpupool as CPU Pool
//!
//! sdk->>endpoint:POST /api/42/store
//! activate endpoint
//! endpoint->>projectcache: get project (cached only)
//! activate projectcache
//! projectcache-->>endpoint: return project
//! deactivate projectcache
//! Note over endpoint: Checking rate limits and auth (fast path)
//! endpoint->>envelopemanager: queue event
//!
//! activate envelopemanager
//! envelopemanager-->>endpoint:event ID
//! endpoint-->>sdk:200 OK
//! deactivate endpoint
//!
//! envelopemanager->>projectcache:fetch project
//! activate projectcache
//! Note over envelopemanager,projectcache: web request (batched with other projects)
//! projectcache-->>envelopemanager: return project
//! deactivate projectcache
//!
//! envelopemanager->>cpupool: .
//! activate cpupool
//! Note over envelopemanager,cpupool: normalization, datascrubbing, redis rate limits, ...
//! cpupool-->>envelopemanager: .
//! deactivate cpupool
//!
//! Note over envelopemanager: Send event to kafka
//!
//! deactivate envelopemanager
//! ```
//!
//! <script src="https://cdn.jsdelivr.net/npm/mermaid@8.8.4/dist/mermaid.min.js"></script>
//! <script>
//! mermaid.init({}, ".language-mermaid code");
//! // Could not get dark mode in mermaid to work
//! Array.from(document.getElementsByTagName('svg')).map(x => x.style.background = "white")
//! </script>
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod actors;
mod body;
mod constants;
mod endpoints;
mod envelope;
mod extractors;
mod http;
mod metrics_extraction;
mod middlewares;
mod service;
mod statsd;
mod utils;

use relay_config::Config;
use relay_system::Controller;

use crate::actors::server::Server;

pub use crate::service::ServerError;

/// Runs a relay web server and spawns all internal worker threads.
///
/// This effectively boots the entire server application. It blocks the current thread until a
/// shutdown signal is received or a fatal error happens. Behavior of the server is determined by
/// the `config` passed into this funciton.
pub fn run(config: Config) -> Result<(), ServerError> {
    // Run the controller and block until a shutdown signal is sent to this process. This will
    // create an actix system, start a web server and run all relevant actors inside. See the
    // `actors` module documentation for more information on all actors.
    Controller::run(|| Server::start(config))
}
