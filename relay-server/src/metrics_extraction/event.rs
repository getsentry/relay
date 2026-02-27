use std::collections::BTreeMap;

use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::{Event, Span};
use relay_metrics::{Bucket, BucketMetadata, BucketValue};
use relay_sampling::evaluation::SamplingDecision;

use crate::metrics_extraction::generic::Extractable;
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::statsd::RelayTimers;

impl Extractable for Event {
    fn timestamp(&self) -> Option<UnixTimestamp> {
        self.timestamp
            .value()
            .and_then(|ts| UnixTimestamp::from_datetime(ts.0))
    }
}

impl Extractable for Span {
    fn timestamp(&self) -> Option<UnixTimestamp> {
        self.timestamp
            .value()
            .and_then(|ts| UnixTimestamp::from_datetime(ts.0))
    }
}

/// Configuration for [`extract_metrics`].
#[derive(Debug, Copy, Clone)]
pub struct ExtractMetricsConfig<'a> {
    pub sampling_decision: SamplingDecision,
    pub target_project_id: ProjectId,
    pub extract_spans: bool,
    pub transaction_from_dsc: Option<&'a str>,
}

/// Extracts metrics from an [`Event`].
///
/// The event must have a valid timestamp; if the timestamp is missing or invalid, no metrics are
/// extracted. Timestamp and clock drift correction should occur before metrics extraction to ensure
/// valid timestamps.
///
/// If this is a transaction event with spans, metrics will also be extracted from the spans.
pub fn extract_metrics(event: &mut Event, config: ExtractMetricsConfig) -> ExtractedMetrics {
    let mut metrics = ExtractedMetrics::default();

    if config.extract_spans {
        extract_span_metrics_for_event(event, config, &mut metrics);
    }

    metrics
}

fn extract_span_metrics_for_event(
    event: &mut Event,
    config: ExtractMetricsConfig<'_>,
    output: &mut ExtractedMetrics,
) {
    macro_rules! create_span_root_counter {
        ($count:expr, $is_segment:expr) => {{
            create_span_root_counter(
                event,
                config.transaction_from_dsc.map(|tx| tx.to_owned()),
                $count,
                $is_segment,
                config.sampling_decision,
                config.target_project_id,
            )
        }};
    }

    relay_statsd::metric!(timer(RelayTimers::EventProcessingSpanMetricsExtraction), {
        // Segment span (the transaction itself as a span).
        // The segment span from a transaction always has was_transaction=true.
        let bucket = create_span_root_counter!(1, true);
        output.sampling_metrics.extend(bucket);
        output.project_metrics.extend(create_span_usage(
            event,
            1,
            true,  // is_segment
            true,  // was_transaction (segment span from a transaction event)
        ));

        // Child spans
        let span_count = event
            .spans
            .value()
            .map(|spans| spans.len() as u32)
            .unwrap_or(0);

        let bucket = create_span_root_counter!(span_count, false);
        output.sampling_metrics.extend(bucket);
        output
            .project_metrics
            .extend(create_span_usage(event, span_count, false, false));
    });
}

/// Creates the `c:spans/usage@none` metric for spans within an event.
fn create_span_usage(
    event: &Event,
    count: u32,
    is_segment: bool,
    was_transaction: bool,
) -> Option<Bucket> {
    if count == 0 {
        return None;
    }

    let timestamp = <Event as Extractable>::timestamp(event)?;

    let received_at = if cfg!(not(test)) {
        UnixTimestamp::now()
    } else {
        UnixTimestamp::from_secs(0)
    };

    let mut tags = BTreeMap::new();
    tags.insert("is_segment".to_owned(), is_segment.to_string());
    if is_segment && was_transaction {
        tags.insert("was_transaction".to_owned(), "true".to_owned());
    }

    Some(Bucket {
        timestamp,
        width: 0,
        name: "c:spans/usage@none".into(),
        value: BucketValue::counter(count.into()),
        tags,
        metadata: BucketMetadata::new(received_at),
    })
}

/// Creates the metric `c:spans/count_per_root_project@none`.
///
/// This metric counts the number of spans per root project of the trace. This is used for dynamic
/// sampling biases to compute weights of projects including all spans in the trace.
pub fn create_span_root_counter<T: Extractable>(
    instance: &T,
    transaction: Option<String>,
    span_count: u32,
    is_segment: bool,
    sampling_decision: SamplingDecision,
    target_project_id: ProjectId,
) -> Option<Bucket> {
    if span_count == 0 {
        return None;
    }

    let timestamp = instance.timestamp()?;

    // For extracted metrics we assume the `received_at` timestamp is equivalent to the time
    // in which the metric is extracted.
    let received_at = if cfg!(not(test)) {
        UnixTimestamp::now()
    } else {
        UnixTimestamp::from_secs(0)
    };

    let mut tags = BTreeMap::new();
    tags.insert("decision".to_owned(), sampling_decision.to_string());
    tags.insert(
        "target_project_id".to_owned(),
        target_project_id.to_string(),
    );
    if let Some(transaction) = transaction {
        tags.insert("transaction".to_owned(), transaction);
    }
    tags.insert("is_segment".to_owned(), is_segment.to_string());

    Some(Bucket {
        timestamp,
        width: 0,
        name: "c:spans/count_per_root_project@none".into(),
        value: BucketValue::counter(span_count.into()),
        tags,
        metadata: BucketMetadata::new(received_at),
    })
}
