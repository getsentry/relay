use relay_event_normalization::{RequiredMode, SchemaProcessor, eap};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::TraceMetric;
use relay_pii::{AttributeMode, PiiProcessor};
use relay_protocol::Annotated;

use crate::envelope::{ContainerItems, Item, ItemContainer};
use crate::extractors::RequestMeta;
use crate::processing::Context;
use crate::processing::Managed;
use crate::processing::trace_metrics::{Error, Result};
use crate::processing::trace_metrics::{ExpandedTraceMetrics, SerializedTraceMetrics};
use crate::services::outcome::DiscardReason;

/// Parses all serialized trace metrics into their [`ExpandedTraceMetrics`] representation.
///
/// Individual, invalid trace metrics will be discarded.
pub fn expand(metrics: Managed<SerializedTraceMetrics>) -> Managed<ExpandedTraceMetrics> {
    metrics.map(|metrics, records| {
        let mut all_metrics = Vec::new();
        for item in metrics.metrics {
            let expanded = expand_trace_metric_container(&item);
            let expanded = records.or_default(expanded, item);
            all_metrics.extend(expanded);
        }

        ExpandedTraceMetrics {
            headers: metrics.headers,
            metrics: all_metrics,
        }
    })
}

/// Normalizes individual trace metric entries.
///
/// Normalization must happen before any filters are applied or other procedures which rely on the
/// presence and well-formedness of attributes and fields.
pub fn normalize(metrics: &mut Managed<ExpandedTraceMetrics>, _ctx: Context<'_>) {
    metrics.retain_with_context(
        |metrics| (&mut metrics.metrics, metrics.headers.meta()),
        |metric, meta, _| {
            normalize_trace_metric(metric, meta).inspect_err(|err| {
                relay_log::debug!("failed to normalize trace metric: {err}");
            })
        },
    );
}

/// Applies PII scrubbing to individual trace metric entries.
pub fn scrub(metrics: &mut Managed<ExpandedTraceMetrics>, ctx: Context<'_>) {
    metrics.retain(
        |metrics| &mut metrics.metrics,
        |metric, _| {
            scrub_trace_metric(metric, ctx).inspect_err(|err| {
                relay_log::debug!("failed to scrub pii from trace metric: {err}")
            })
        },
    );
}

/// Parses a trace metric container into its [`ContainerItems<TraceMetric>`] representation.
fn expand_trace_metric_container(item: &Item) -> Result<ContainerItems<TraceMetric>> {
    let metrics = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse trace metrics container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    Ok(metrics)
}

/// Applies PII scrubbing to an individual trace metric entry.
fn scrub_trace_metric(metric: &mut Annotated<TraceMetric>, ctx: Context<'_>) -> Result<()> {
    let pii_config_from_scrubbing = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| Error::PiiConfig(e.clone()))?;

    let state = ProcessingState::root().enter_borrowed("", None, [ValueType::TraceMetric]);

    if let Some(ref config) = ctx.project_info.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled())
            // For advanced rules we want to treat attributes as objects.
            .attribute_mode(AttributeMode::Object);
        process_value(metric, &mut processor, &state)?;
    }

    if let Some(config) = pii_config_from_scrubbing {
        let mut processor = PiiProcessor::new(config.compiled())
            // For "legacy" rules we want to identify attributes with their values.
            .attribute_mode(AttributeMode::ValueOnly);
        process_value(metric, &mut processor, &state)?;
    }

    Ok(())
}

/// Normalizes an individual trace metric entry.
fn normalize_trace_metric(metric: &mut Annotated<TraceMetric>, meta: &RequestMeta) -> Result<()> {
    if let Some(metric_value) = metric.value_mut() {
        eap::normalize_received(&mut metric_value.attributes, meta.received_at());
        eap::normalize_user_agent(
            &mut metric_value.attributes,
            meta.user_agent(),
            meta.client_hints(),
        );
        eap::normalize_attribute_types(&mut metric_value.attributes);
    };

    process_value(
        metric,
        &mut SchemaProcessor::new().with_required(RequiredMode::DeleteParent),
        ProcessingState::root(),
    )?;

    if let Annotated(None, meta) = metric {
        relay_log::debug!("empty metric: {meta:?}");
        return Err(Error::Invalid(DiscardReason::NoData));
    }

    Ok(())
}
