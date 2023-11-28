//! Processor code related to standalone spans.
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::Span;
use relay_protocol::Annotated;

pub fn normalize_span(annotated_span: &mut Annotated<Span>, max_tag_value_size: usize) {
    let Some(span) = annotated_span.value_mut() else {
        return;
    };

    let config = tag_extraction::Config { max_tag_value_size };
    let tags = tag_extraction::extract_tags(span, &config, None, None);
    span.sentry_tags = Annotated::new(
        tags.into_iter()
            .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
            .collect(),
    );
}
