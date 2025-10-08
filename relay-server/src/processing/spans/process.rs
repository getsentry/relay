use relay_event_normalization::{
    GeoIpLookup, SchemaProcessor, TimestampProcessor, TrimmingProcessor, eap,
};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::SpanV2;
use relay_protocol::Annotated;

use crate::envelope::{ContainerItems, Item, ItemContainer};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::processing::spans::{Error, ExpandedSpans, Result, SampledSpans};
use crate::services::outcome::DiscardReason;

/// Parses all serialized spans.
///
/// Individual, invalid spans are discarded.
pub fn expand(spans: Managed<SampledSpans>) -> Managed<ExpandedSpans> {
    spans.map(|spans, records| {
        let mut all_spans = Vec::new();

        for item in &spans.spans {
            let expanded = expand_span(item);
            let expanded = records.or_default(expanded, item);
            all_spans.extend(expanded);
        }

        ExpandedSpans {
            headers: spans.headers,
            server_sample_rate: spans.server_sample_rate,
            spans: all_spans,
        }
    })
}

fn expand_span(item: &Item) -> Result<ContainerItems<SpanV2>> {
    let spans = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse span container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    Ok(spans)
}

/// Normalizes individual spans.
pub fn normalize(spans: &mut Managed<ExpandedSpans>, geo_lookup: &GeoIpLookup) {
    spans.retain_with_context(
        |spans| (&mut spans.spans, spans.headers.meta()),
        |span, meta, _| {
            normalize_span(span, meta, geo_lookup).inspect_err(|err| {
                relay_log::debug!("failed to normalize span: {err}");
            })
        },
    );
}

fn normalize_span(
    span: &mut Annotated<SpanV2>,
    meta: &RequestMeta,
    geo_lookup: &GeoIpLookup,
) -> Result<()> {
    process_value(span, &mut SchemaProcessor, ProcessingState::root())?;
    process_value(span, &mut TimestampProcessor, ProcessingState::root())?;

    // TODO: `validate_span()` (start/end timestamps)

    if let Some(span) = span.value_mut() {
        eap::normalize_received(&mut span.attributes, meta.received_at());
        eap::normalize_user_agent(&mut span.attributes, meta.user_agent(), meta.client_hints());
        eap::normalize_attribute_types(&mut span.attributes);
        eap::normalize_user_geo(&mut span.attributes, || {
            meta.client_addr().and_then(|ip| geo_lookup.lookup(ip))
        });

        // TODO: ai model costs
    } else {
        return Err(Error::Invalid(DiscardReason::NoData));
    };

    process_value(span, &mut TrimmingProcessor::new(), ProcessingState::root())?;

    Ok(())
}
