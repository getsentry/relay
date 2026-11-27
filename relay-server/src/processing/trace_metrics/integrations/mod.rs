use relay_event_schema::protocol::{TraceMetric, TraceMetricHeader};
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, IntegrationItem, WithHeader};
use crate::integrations::{Integration, TraceMetricsIntegration};
use crate::managed::RecordKeeper;
use crate::processing::trace_metrics::{Error, Settings};
use crate::services::outcome::DiscardReason;

mod otel;

/// Expands a trace metric [`Integration`] item.
///
/// The function expects *only* trace metric item integrations.
pub fn expand(
    records: &mut RecordKeeper<'_>,
    item: IntegrationItem,
    max_ops: usize,
    max_size: Option<usize>,
) -> (Settings, ContainerItems<TraceMetric>) {
    let settings = Settings {
        histograms: true,
        ..Default::default()
    };
    let mut result = Vec::new();

    let integration = match item.integration() {
        Some(Integration::TraceMetrics(integration)) => integration,
        integration => {
            records.internal_error(InvalidIntegration(integration), item);
            return (settings, result);
        }
    };

    // Forwarded trace metrics are sent in a single container, which must not exceed `max_size`.
    //
    // Only half of the size is used for the expanded trace metrics, normalization still adds
    // attributes to every trace metric. Trace metrics exceeding the size are dropped.
    let mut remaining = max_size.map(|max_size| max_size / 2);
    let produce = |metric: TraceMetric| {
        let byte_size = relay_otel::calculate_trace_metric_size(&metric);
        let metric = WithHeader {
            header: Some(TraceMetricHeader {
                byte_size: Some(byte_size),
                other: Default::default(),
            }),
            value: metric.into(),
        };
        records.add(&metric);

        if let Some(remaining) = &mut remaining {
            let size = serde_json::to_vec(&metric).map_or(usize::MAX, |json| json.len());
            match remaining.checked_sub(size) {
                Some(r) => *remaining = r,
                None => return drop(records.reject_err(Error::TooLarge, metric)),
            }
        }
        result.push(metric);
    };

    let payload = item.data().payload();

    let dropped = match integration {
        TraceMetricsIntegration::OtelV1 { format } => {
            otel::expand(format, &payload, max_ops, produce)
        }
    };

    match dropped {
        Err(err) => drop(records.reject_err(err, item)),
        Ok(dropped) => {
            // Undo all the base item quantities, as they will be completely taken over by the parsed
            // contents, which contains an arbitrary amount of items (even 0).
            records.remove(&item);

            // Data points which could not be converted, are still accounted for.
            if dropped > 0 {
                let dropped = (DataCategory::TraceMetric, dropped);
                records.add(dropped);
                records.reject_err(Error::Invalid(DiscardReason::InvalidTraceMetric), dropped);
            }
        }
    }

    (settings, result)
}

#[derive(Debug, thiserror::Error)]
#[error("Expected a trace metrics integration, got: {0:?}")]
struct InvalidIntegration(Option<Integration>);
