//! Metric protocol, aggregation and processing for Sentry.
//!
//! Metrics are high-volume values sent from Sentry clients, integrations, or extracted from errors
//! and transactions, that can be aggregated and queried over large time windows. As opposed to rich
//! errors and transactions, metrics carry relatively little context information in tags with low
//! cardinality.
//!
//! The metric type is part of its signature just like the unit. Therefore, it is allowed to reuse a
//! metric name for multiple metric types, which will result in multiple metrics being recorded.
//!
//! # Aggregation
//!
//! Relay accumulates all metrics in [time buckets](Bucket) before sending them onwards. Aggregation
//! is handled by the [`aggregator::Aggregator`], which should be created once for the entire system. It flushes
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
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub mod aggregator;
pub mod cogs;

mod bucket;
mod protocol;
mod statsd;
mod utils;
mod view;

pub use bucket::*;
pub use protocol::*;
pub use utils::ByNamespace;
pub use view::*;
