use relay_event_schema::protocol::{MetricType, TraceMetric};
use relay_protocol::Annotated;

use crate::managed::{Managed, Rejected};
use crate::processing::trace_metrics::{
    Error, ExpandedTraceMetrics, Result, SerializedTraceMetrics,
};
use crate::services::outcome::DiscardReason;

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
