//! Metric protocol, aggregation and processing for Sentry.
//!
//! Metrics are high-volume values sent from Sentry clients, integrations, or extracted from errors
//! and transactions, that can be aggregated and queried over large time windows. As opposed to rich
//! errors and transactions, metrics carry relatively little context information in tags with low
//! cardinality.
//!
//! # Protocol
//!
//! Clients submit metrics in a [text-based protocol](Bucket) based on StatsD. See the [field
//! documentation](Bucket#fields) on `Bucket` for more information on the components. A sample
//! submission looks like this:
//!
//! ```text
#![doc = include_str!("../tests/fixtures/buckets.statsd.txt")]
//! ```
//!
//! The metric type is part of its signature just like the unit. Therefore, it is allowed to reuse a
//! metric name for multiple metric types, which will result in multiple metrics being recorded.
//!
//! # Metric Envelopes
//!
//! To send one or more metrics to Relay, the raw protocol is enclosed in an envelope item of type
//! `metrics`:
//!
//! ```text
//! {}
//! {"type": "statsd", ...}
#![doc = include_str!("../tests/fixtures/buckets.statsd.txt")]
//! ...
//! ```
//!
//! Note that the name format used in the statsd protocol is different from the MRI: Metric names
//! are not prefixed with `<ty>:` as the type is somewhere else in the protocol. If no metric
//! namespace is specified, the `"custom"` namespace is assumed.
//!
//! Optionally, a timestamp can be added to every line of the submitted envelope. The timestamp has
//! to be a valid Unix timestamp (UTC) and must be prefixed with `T`. If it is omitted, the
//! `received` time of the envelope is assumed.
//!
//! # Aggregation
//!
//! Relay accumulates all metrics in [time buckets](Bucket) before sending them onwards. Aggregation
//! is handled by the [`Aggregator`], which should be created once for the entire system. It flushes
//! aggregates in regular intervals, either shortly after their original time window has passed or
//! with a debounce delay for backdated submissions.
//!
//! **Warning**: With chained Relays submission delays accumulate.
//!
//! Aggregate buckets are encoded in JSON with the following schema:
//!
//! ```json
#![doc = include_str!("../tests/fixtures/buckets.json")]
//! ```
//!
//! # Ingestion
//!
//! Processing Relays write aggregate buckets into the ingestion Kafka stream. The schema is similar
//! to the aggregation payload, with the addition of scoping information. Each bucket is sent in a
//! separate message:
//!
//! ```json
#![doc = include_str!("../tests/fixtures/kafka.json")]
//! ```

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub mod aggregator;

mod aggregatorservice;
mod bucket;
mod protocol;
mod router;
mod statsd;

pub use aggregatorservice::*;
pub use bucket::*;
pub use protocol::*;
pub use router::*;
