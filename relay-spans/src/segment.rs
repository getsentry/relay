use relay_event_schema::protocol::Span;
use relay_protocol::Empty;

/// Normalizes various segment-related fields on a span:
///
/// * Web vital spans have `is_segment`, `parent_span_id`, and `segment_id` erased.
/// * Spans whose `span_id` is equal to the `segment_id` or which don't have a `parent_span_id` have
///   `is_segment` set to `true`.
/// * Finally, segment spans have the `segment_id` set to their own `span_id`.
pub fn set_segment_attributes(span: &mut Span) {
    // Identify INP spans or other WebVital spans and make sure they are not wrapped in a segment.
    if let Some(span_op) = span.op.value()
        && (span_op.starts_with("ui.interaction.") || span_op.starts_with("ui.webvital."))
    {
        span.is_segment = None.into();
        span.parent_span_id = None.into();
        span.segment_id = None.into();
        return;
    }

    let Some(span_id) = span.span_id.value() else {
        return;
    };

    if let Some(segment_id) = span.segment_id.value() {
        // The span is a segment if and only if the segment_id matches the span_id.
        span.is_segment = (segment_id == span_id).into();
    } else if span.parent_span_id.is_empty() {
        // If the span has no parent, it is automatically a segment:
        span.is_segment = true.into();
    }

    // If the span is a segment, always set the segment_id to the current span_id:
    if span.is_segment.value() == Some(&true) {
        span.segment_id = span.span_id.clone();
    }
}
