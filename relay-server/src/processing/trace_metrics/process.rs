use relay_event_normalization::{RequiredMode, SchemaProcessor, eap};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{TraceMetric, TraceMetricHeader};
use relay_pii::{AttributeMode, PiiProcessor};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemContainer};
use crate::extractors::RequestTrust;
use crate::processing::Managed;
use crate::processing::trace_metrics::{Error, Result, utils::calculate_size};
use crate::processing::trace_metrics::{ExpandedTraceMetrics, SerializedTraceMetrics};
use crate::processing::{Context, utils};
use crate::services::outcome::DiscardReason;

/// Parses all serialized trace metrics into their [`ExpandedTraceMetrics`] representation.
///
/// Individual, invalid trace metrics will be discarded.
pub fn expand(metrics: Managed<SerializedTraceMetrics>) -> Managed<ExpandedTraceMetrics> {
    let trust = metrics.headers.meta().request_trust();

    metrics.map(|metrics, records| {
        records.lenient(DataCategory::TraceMetricByte);

        let mut all_metrics = Vec::new();
        for item in metrics.metrics {
            let expanded = expand_trace_metric_container(&item, trust);
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
pub fn normalize(metrics: &mut Managed<ExpandedTraceMetrics>, ctx: Context<'_>) {
    metrics.retain_with_context(
        |metrics| (&mut metrics.metrics, &metrics.headers),
        |metric, headers, _| {
            normalize_trace_metric(metric, headers, ctx).inspect_err(|err| {
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
fn expand_trace_metric_container(
    item: &Item,
    trust: RequestTrust,
) -> Result<ContainerItems<TraceMetric>> {
    let mut metrics = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse trace metrics container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    for metric in &mut metrics {
        let byte_size = metric
            .header
            .as_ref()
            .and_then(|h: &TraceMetricHeader| h.byte_size);
        if trust.is_untrusted() || matches!(byte_size, None | Some(0)) {
            let byte_size = metric.value().map(calculate_size).unwrap_or(1);
            metric.header.get_or_insert_default().byte_size = Some(byte_size);
        }
    }

    Ok(metrics)
}

/// Applies PII scrubbing to an individual trace metric entry.
fn scrub_trace_metric(metric: &mut Annotated<TraceMetric>, ctx: Context<'_>) -> Result<()> {
    let pii_config_from_scrubbing = ctx.project_info.config.datascrubbing_settings.pii_config();

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
fn normalize_trace_metric(
    metric: &mut Annotated<TraceMetric>,
    headers: &EnvelopeHeaders,
    ctx: Context<'_>,
) -> Result<()> {
    let meta = headers.meta();

    eap::time::normalize(
        metric,
        utils::normalize::time_config(headers, |f| f.trace_metric.as_ref(), ctx),
    );

    if let Some(metric_value) = metric.value_mut() {
        eap::trace_metric::normalize_metric_name(metric_value)?;
        if ctx.is_processing() {
            eap::trace_metric::normalize_metric_unit(metric_value);
        }
        eap::normalize_received(&mut metric_value.attributes, meta.received_at());
        eap::normalize_client_address(&mut metric_value.attributes, meta.client_addr());
        eap::normalize_user_agent(
            &mut metric_value.attributes,
            meta.user_agent(),
            meta.client_hints(),
        );
        eap::normalize_attribute_types(&mut metric_value.attributes);
    };

    process_value(
        metric,
        &mut SchemaProcessor::new()
            .with_required(RequiredMode::DeleteParent)
            .with_verbose_errors(relay_log::enabled!(relay_log::Level::DEBUG)),
        ProcessingState::root(),
    )?;

    if let Annotated(None, meta) = metric {
        relay_log::debug!("empty metric: {meta:?}");
        return Err(Error::Invalid(DiscardReason::NoData));
    }

    Ok(())
}
