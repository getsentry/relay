use relay_event_schema::protocol::SpanV2;

/// Returns the size of a SpanV2.
pub fn calculate_size(span: &SpanV2) -> u64 {
    let mut total_size = 0u64;

    if let Some(attributes) = span.attributes.value() {
        total_size += attributes
            .0
            .iter()
            .map(|(k, v)| k.len() + relay_event_schema::protocol::attribute_size(v))
            .sum::<usize>() as u64;
    }

    total_size += span
        .other
        .iter()
        .map(|(k, v)| k.len() + relay_event_schema::protocol::value_size(v))
        .sum::<usize>() as u64;

    total_size
}
