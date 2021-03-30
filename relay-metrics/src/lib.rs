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
//! Relay accumulates all metrics in [time buckets](Bucket) before sending them onwards. Aggregation
//! is handled by the [`Aggregator`], which should be created once per Project Key (DSN). It flushes
//! aggregates in regular intervals, either shortly after their original time window has passed or
//! with a debounce delay for backdated submissions.
//!
//! **Warning**: With chained Relays submission delays accumulate.
//!
//! Aggregate buckets are encoded in JSON with the following schema:
//!
//! ```json
//! [
//!   {
//!     "name": "endpoint.response_time",
//!     "unit": "ms",
//!     "value": [36, 49, 57, 68],
//!     "type": "d",
//!     "timestamp": 1615889440,
//!     "tags": {
//!       "route": "user_index"
//!     }
//!   },
//!   {
//!     "name": "endpoint.hits",
//!     "value": 4,
//!     "type": "c",
//!     "timestamp": 1615889440,
//!     "tags": {
//!       "route": "user_index"
//!     }
//!   }
//! ]
//! ```
//!
//! # Ingestion
//!
//! Processing Relays write aggregate buckets into the ingestion Kafka stream. The schema is similar
//! to the aggregation payload, with the addition of scoping information:
//!
//! ```json
//! [
//!   {
//!     "org_id": 1,
//!     "project_id": 42,
//!     "name": "endpoint.response_time",
//!     "unit": "ms",
//!     "value": [36, 49, 57, 68],
//!     "type": "d",
//!     "timestamp": 1615889440,
//!     "tags": {
//!       "route": "user_index"
//!     }
//!   },
//!   {
//!     "org_id": 1,
//!     "project_id": 42,
//!     "name": "endpoint.hits",
//!     "value": 4,
//!     "type": "c",
//!     "timestamp": 1615889440,
//!     "tags": {
//!       "route": "user_index"
//!     }
//!   }
//! ]
//! ```
#![warn(missing_docs)]

mod aggregation;
mod protocol;

pub use aggregation::*;
pub use protocol::*;
