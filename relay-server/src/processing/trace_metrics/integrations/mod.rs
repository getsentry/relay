use relay_event_schema::protocol::{TraceMetric, TraceMetricHeader};
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, IntegrationItem, WithHeader};
use crate::integrations::{Integration, TraceMetricsIntegration};
use crate::managed::RecordKeeper;
use crate::processing::trace_metrics::utils::calculate_size;
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

    let produce = |metric: TraceMetric| {
        let byte_size = calculate_size(&metric);
        let metric = WithHeader {
            header: Some(TraceMetricHeader {
                byte_size: Some(byte_size),
                other: Default::default(),
            }),
            value: metric.into(),
        };
        records.add(&metric);
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
