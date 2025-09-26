use relay_event_schema::protocol::{MetricType, TraceMetric};
use relay_protocol::Annotated;

use crate::processing::trace_metrics::{Error, Result};
use crate::services::outcome::DiscardReason;

pub fn validate(metric: &Annotated<TraceMetric>) -> Result<()> {
    let Some(metric) = metric.value() else {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    };

    if metric.timestamp.value().is_none() {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    }

    if metric.trace_id.value().is_none() {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    }

    if metric.metric_name.value().is_none() {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    }

    match metric.metric_type.value() {
        Some(
            MetricType::Set | MetricType::Gauge | MetricType::Distribution | MetricType::Counter,
        ) => {}
        Some(MetricType::Unknown(_)) | None => {
            return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
        }
    }

    if metric.value.value().is_none() {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    }

    Ok(())
}
