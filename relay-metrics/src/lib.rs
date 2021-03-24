//! Metric protocol, aggregation and processing for Sentry.
//!
//! Metrics are high-volume values sent from Sentry clients, integrations, or extracted from errors
//! and transactions, that can be aggregated and queried over large time windows. As opposed to rich
//! errors and transactions, metrics carry relatively little context information in tags with low
//! cardinality.
//!
//! # Protocol
//!
//! Clients submit metrics in a [text-based protocol](Metric) based on StatsD. A sample submission
//! looks like this:
//!
//! ```text
//! endpoint.response_time@ms:57|d|'1615889449|#route:user_index
//! endpoint.hits:1|c|'1615889449|#route:user_index
//! ```
//!
//! # Aggregation
//!
//! Relay accumulates all metrics in time buckets before sending them onwards.
//! Buckets are flushed when their time window plus a grace period has passed
//! (see [`aggregation::Aggregator`]).
#![warn(missing_docs)]

mod aggregation;
mod protocol;

pub use aggregation::*;
pub use protocol::*;
