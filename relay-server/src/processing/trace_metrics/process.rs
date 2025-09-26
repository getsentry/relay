use relay_event_normalization::SchemaProcessor;
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{Attributes, TraceMetric};
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, Item, ItemContainer};
use crate::extractors::{RequestMeta, RequestTrust};
use crate::managed::ManagedEnvelope;
use crate::processing::Managed;
use crate::processing::trace_metrics::{
    Error, ExpandedTraceMetrics, Result, SerializedTraceMetrics,
};
use crate::services::outcome::DiscardReason;

pub struct SerializedTraceMetricsContainer {
    pub metrics: SerializedTraceMetrics,
    pub headers: RequestMeta,
}

pub fn expand(metrics: Managed<SerializedTraceMetricsContainer>) -> Managed<ExpandedTraceMetrics> {
    let trust = metrics.headers.meta().request_trust();

    metrics.map(|metrics, records| {
        records.lenient(DataCategory::TraceMetric);

        let mut all_metrics = Vec::new();
        for metric_bytes in metrics.metrics {
            let expanded = expand_trace_metric_container(&metric_bytes, trust);
            let expanded = records.or_default(expanded, metric_bytes);
            all_metrics.extend(expanded);
        }

        all_metrics
    })
}

pub fn normalize(metrics: &mut Managed<ExpandedTraceMetrics>, meta: &RequestMeta) {
    metrics.retain_with_context(
        |metrics| (metrics, meta),
        |metric, meta| {
            normalize_trace_metric(metric, meta).inspect_err(|err| {
                relay_log::debug!("failed to normalize trace metric: {err}");
            })
        },
    );
}

pub fn scrub(
    metrics: &mut Managed<ExpandedTraceMetrics>,
    project_state: &relay_dynamic_config::ProjectInfo,
    managed_envelope: &ManagedEnvelope,
) {
    metrics.retain(
        |metrics| metrics,
        |metric| {
            scrub_trace_metric(metric, project_state, managed_envelope).inspect_err(|err| {
                relay_log::debug!("failed to scrub pii from trace metric: {err}")
            })
        },
    );
}

fn expand_trace_metric_container(
    item_bytes: &[u8],
    _trust: RequestTrust,
) -> Result<ContainerItems<TraceMetric>> {
    let item = Item::parse(item_bytes).map_err(|err| {
        relay_log::debug!("failed to parse trace metrics item: {err}");
        Error::Invalid(DiscardReason::InvalidJson)
    })?;

    let metrics = ItemContainer::parse(&item)
        .map_err(|err| {
            relay_log::debug!("failed to parse trace metrics container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    Ok(metrics)
}

fn scrub_trace_metric(
    metric: &mut Annotated<TraceMetric>,
    project_state: &relay_dynamic_config::ProjectInfo,
    managed_envelope: &ManagedEnvelope,
) -> Result<()> {
    let pii_config_from_scrubbing =
        project_state.has_feature(relay_dynamic_config::Feature::DatascrubberPiiScrubbing);

    let pii_config = if pii_config_from_scrubbing {
        managed_envelope
            .config()
            .datascrubbing_settings
            .pii_config()
    } else {
        None
    };

    if let Some(pii_config) = pii_config {
        let mut processor = PiiProcessor::new(pii_config);

        if let Some(metric) = metric.value_mut() {
            if let Some(attrs) = metric.attributes.value_mut() {
                processor.process_value(attrs, &mut ProcessingState::root(), ValueType::Object)?;
            }
        }
    }

    Ok(())
}

fn normalize_trace_metric(metric: &mut Annotated<TraceMetric>, _meta: &RequestMeta) -> Result<()> {
    if metric.value().is_none() {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    }

    let Some(metric_value) = metric.value_mut() else {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    };

    if metric_value.metric_type.value().is_none() {
        return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
    }

    let mut processor = SchemaProcessor::default();
    process_value(metric, &mut processor, ProcessingState::root())?;

    Ok(())
}
