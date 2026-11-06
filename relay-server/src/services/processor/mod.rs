//! Services that use an [`relay_threading::AsyncPool`] to handle a mix of CPU-heavy processing and redis calls.
pub mod bucket;
mod common;
pub mod envelope;
mod metrics;

#[cfg(feature = "processing")]
pub use common::create_rate_limiter;
pub use common::{SendEnvelope, SubmitClientReports, encode_payload};
pub use envelope::{
    BucketSource, EnvelopeProcessor, MINIMUM_CLOCK_DRIFT, MetricData, ProcessBatchedMetrics,
    ProcessEnvelope, ProcessMetrics, ProcessingError, ProcessingExtractedMetrics,
};
