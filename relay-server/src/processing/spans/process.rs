use relay_event_schema::protocol::SpanV2;

use crate::envelope::{ContainerItems, Item, ItemContainer};
use crate::managed::Managed;
use crate::processing::spans::{Error, ExpandedSpans, Result, SerializedSpans};
use crate::services::outcome::DiscardReason;

/// Parses all serialized spans.
///
/// Individual, invalid spans are discarded.
pub fn expand(spans: Managed<SerializedSpans>) -> Managed<ExpandedSpans> {
    spans.map(|spans, records| {
        let mut all_spans = Vec::with_capacity(spans.count());

        for item in &spans.spans {
            let expanded = expand_span(item);
            let expanded = records.or_default(expanded, item);
            all_spans.extend(expanded);
        }

        ExpandedSpans {
            headers: spans.headers,
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
