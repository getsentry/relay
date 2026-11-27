use relay_event_schema::protocol::TraceMetric;
use relay_otel::descriptors::Decodable;
use relay_otel::otel_metrics::MetricsData;

use crate::integrations::OtelFormat;
use crate::processing::trace_metrics::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL metrics into the [`TraceMetric`] format.
///
/// Returns the amount of data points and exemplars which could not be converted.
pub fn expand<F>(
    format: OtelFormat,
    payload: &[u8],
    max_ops: usize,
    mut produce: F,
) -> Result<usize>
where
    F: FnMut(TraceMetric),
{
    let metrics = parse_metrics_data(format, payload, max_ops)?;

    let mut dropped = 0;
    for resource_metrics in metrics.resource_metrics {
        let resource = resource_metrics.resource.as_ref();
        for scope_metrics in resource_metrics.scope_metrics {
            let scope = scope_metrics.scope.as_ref();
            for metric in scope_metrics.metrics {
                dropped +=
                    relay_otel::otel_to_sentry_trace_metrics(metric, resource, scope, &mut produce);
            }
        }
    }

    Ok(dropped)
}

fn parse_metrics_data(format: OtelFormat, payload: &[u8], max_ops: usize) -> Result<MetricsData> {
    match format {
        OtelFormat::Json => {
            relay_serialization::serde::deserialize_json(payload, max_ops).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse metrics data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })
        }
        OtelFormat::Protobuf => MetricsData::decode_bounded(payload, max_ops).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse metrics data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}
