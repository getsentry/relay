use relay_dynamic_config::Feature;
use relay_event_schema::protocol::TraceMetric;
use relay_protocol::Annotated;

use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::trace_metrics::{Error, ExpandedTraceMetrics, Result};

pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    match ctx.should_filter(Feature::TraceMetricsIngestion) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}
/// Applies inbound filters to individual trace metrics.
pub fn filter(metrics: &mut Managed<ExpandedTraceMetrics>, ctx: Context<'_>) {
    metrics.retain_with_context(
        |metrics| (&mut metrics.metrics, metrics.headers.meta()),
        |metric, meta, _| filter_metric(metric, meta, ctx),
    );
}

fn filter_metric(
    metric: &Annotated<TraceMetric>,
    meta: &RequestMeta,
    ctx: Context<'_>,
) -> Result<()> {
    let Some(metric) = metric.value() else {
        return Ok(());
    };

    relay_filter::should_filter(
        metric,
        meta.client_addr(),
        &ctx.project_info.config.filter_settings,
        ctx.global_config.filters(),
    )
    .map_err(Error::Filtered)
}
