use relay_event_normalization::SchemaProcessor;
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::TraceMetric;
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, Item, ItemContainer};
use crate::extractors::{RequestMeta, RequestTrust};
use crate::processing::Managed;
use crate::processing::trace_metrics::{Error, ExpandedTraceMetrics, Result};
use crate::services::outcome::DiscardReason;
use crate::services::projects::project::ProjectInfo;

pub struct SerializedTraceMetricsContainer {
    pub metrics: Vec<Item>,
}

pub fn expand(
    metrics: Managed<SerializedTraceMetricsContainer>,
    meta: &RequestMeta,
) -> Managed<ExpandedTraceMetrics> {
    let trust = meta.request_trust();

    metrics.map(|metrics, records| {
        records.lenient(DataCategory::TraceMetric);

        let mut all_metrics = Vec::new();
        for item in metrics.metrics {
            let expanded = expand_trace_metric_container(&item, trust);
            let expanded = records.or_default(expanded, item);
            all_metrics.extend(expanded);
        }

        all_metrics
    })
}

pub fn normalize(metrics: &mut Managed<ExpandedTraceMetrics>, meta: &RequestMeta) {
    metrics.retain(
        |metrics| metrics,
        |metric| {
            normalize_trace_metric(metric, meta).inspect_err(|err| {
                relay_log::debug!("failed to normalize trace metric: {err}");
            })
        },
    );
}

pub fn scrub(metrics: &mut Managed<ExpandedTraceMetrics>, project_state: &ProjectInfo) {
    metrics.retain(
        |metrics| metrics,
        |metric| {
            scrub_trace_metric(metric, project_state).inspect_err(|err| {
                relay_log::debug!("failed to scrub pii from trace metric: {err}")
            })
        },
    );
}

fn expand_trace_metric_container(
    item: &Item,
    _trust: RequestTrust,
) -> Result<ContainerItems<TraceMetric>> {
    let metrics = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse trace metrics container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    Ok(metrics)
}

fn scrub_trace_metric(
    metric: &mut Annotated<TraceMetric>,
    project_state: &ProjectInfo,
) -> Result<()> {
    if let Ok(Some(pii_config)) = project_state.config.datascrubbing_settings.pii_config() {
        let compiled_config = pii_config.compiled();
        let mut processor = PiiProcessor::new(compiled_config);

        if let Some(metric) = metric.value_mut() {
            process_value(
                &mut metric.attributes,
                &mut processor,
                ProcessingState::root(),
            )
            .ok();
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

    let mut processor = SchemaProcessor;
    process_value(metric, &mut processor, ProcessingState::root()).ok();

    Ok(())
}
