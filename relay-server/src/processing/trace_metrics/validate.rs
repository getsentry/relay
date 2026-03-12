use relay_event_schema::protocol::{MetricType, TraceMetric};
use relay_protocol::Annotated;

use crate::envelope::WithHeader;
use crate::managed::{Managed, Rejected};
use crate::processing::Context;
use crate::processing::trace_metrics::{
    Error, ExpandedTraceMetrics, Result, SerializedTraceMetrics,
};
use crate::services::outcome::DiscardReason;
use crate::statsd::RelayDistributions;

/// Validates that there is only a single trace metric container processed at a time.
///
/// Similar to logs, the `TraceMetric` item must always be sent as an `ItemContainer`, currently it is not allowed to
/// send multiple containers for trace metrics.
///
/// This restriction may be lifted in the future.
///
/// This limit mostly exists to incentivise SDKs to batch multiple trace metrics into a single container,
/// technically it can be removed without issues.
pub fn container(metrics: &Managed<SerializedTraceMetrics>) -> Result<(), Rejected<Error>> {
    // It's fine if there was no trace metric container, as we may accept OTel trace metrics in the future.
    if metrics.metrics.len() > 1 {
        return Err(metrics.reject_err(Error::DuplicateContainer));
    }

    Ok(())
}

/// Validates contained metrics do not exceed the maximum size limit.
///
/// Currently this only considers the maximum metric size configured in the configuration.
///
/// In the future we may want to increase the limit or start trimming excessive
/// attributes/payloads. For now we drop metrics which exceed our size limit.
///
/// This matches the logic defined in [`check_envelope_size_limits`](crate::utils::check_envelope_size_limits),
/// when validating envelope sizes, the actual size of individual metrics is not known and therefore
/// must be enforced consistently after parsing again.
pub fn size(metrics: &mut Managed<ExpandedTraceMetrics>, ctx: Context<'_>) {
    let max_size_bytes = ctx.config.max_trace_metric_size();

    metrics.retain(
        |metrics| &mut metrics.metrics,
        |metric, _| {
            let size = calculate_size(metric);
            let is_too_large = size > max_size_bytes;

            relay_statsd::metric!(
                distribution(RelayDistributions::TraceItemCanonicalSize) = size as u64,
                item = "trace_metric",
                too_large = if is_too_large { "true" } else { "false" },
            );

            match is_too_large {
                true => Err(Error::TooLarge),
                false => Ok(()),
            }
        },
    );
}

/// Calculates the byte size for a trace metric.
///
/// Similar to [`relay_ourlogs::calculate_size`].
fn calculate_size(metric: &WithHeader<TraceMetric>) -> usize {
    let Some(metric) = metric.value() else {
        // Same logic we use for logs.
        return 1;
    };

    let mut size = 0;

    size += metric.name.value().map_or(0, |s| s.len());
    size += relay_event_normalization::eap::value_size(&metric.value);

    if let Some(attributes) = metric.attributes.value() {
        size += relay_event_normalization::eap::attributes_size(attributes);
    }

    size.max(1)
}

/// Validates the semantic validity of a trace metric.
pub fn validate(metrics: &mut Managed<ExpandedTraceMetrics>) {
    metrics.retain(
        |metrics| &mut metrics.metrics,
        |metric, _| {
            validate_trace_metric(metric).inspect_err(|err| {
                relay_log::debug!("failed to validate trace metric: {err}");
            })
        },
    );
}

fn validate_trace_metric(metric: &Annotated<TraceMetric>) -> Result<()> {
    match metric.value().and_then(|m| m.ty.value()) {
        Some(MetricType::Gauge | MetricType::Distribution | MetricType::Counter) => {}
        Some(MetricType::Unknown(_)) | None => {
            return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
        }
    }
    Ok(())
}
