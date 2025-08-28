use relay_dynamic_config::Feature;
use relay_event_schema::protocol::SpanV2;
use relay_protocol::Annotated;

use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::spans::{Error, ExpandedSpans, Result};

/// Filters standalone spans sent for a project which does not allow standalone span ingestion.
pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    match ctx.should_filter(Feature::StandaloneSpanIngestion) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}

/// Applies inbound filters to individual spans.
pub fn filter(spans: &mut Managed<ExpandedSpans>, ctx: Context<'_>) {
    spans.modify(|spans, records| {
        let meta = spans.headers.meta();
        spans.spans.retain_mut(|span| {
            let r = filter_span(span, meta, ctx);
            records.or_default(r.map(|_| true), &*span)
        })
    });
}

fn filter_span(span: &Annotated<SpanV2>, meta: &RequestMeta, ctx: Context<'_>) -> Result<()> {
    let Some(span) = span.value() else {
        return Ok(());
    };

    relay_filter::should_filter(
        span,
        meta.client_addr(),
        &ctx.project_info.config.filter_settings,
        ctx.global_config.filters(),
    )
    .map_err(Error::Filtered)
}
