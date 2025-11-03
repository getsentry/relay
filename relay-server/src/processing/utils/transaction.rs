use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;

/// Creates a span from the transaction and applies tag extraction on it.
///
/// Returns `None` when [`tag_extraction::extract_span_tags`] clears the span, which it shouldn't.
pub fn extract_segment_span(
    event: &Event,
    max_tag_value_size: usize,
    span_allowed_hosts: &[String],
) -> Option<Span> {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(event, &mut spans, max_tag_value_size, span_allowed_hosts);
    tag_extraction::extract_segment_span_tags(event, &mut spans);

    spans.into_iter().next().and_then(Annotated::into_value)
}
