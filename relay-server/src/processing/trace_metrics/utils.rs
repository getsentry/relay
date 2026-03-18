use relay_event_schema::protocol::TraceMetric;

use crate::envelope::WithHeader;

/// Returns the calculated size of a trace metric.
///
/// Unlike [`validate::calculate_size`], this accesses the already materialized byte size
/// from the header, instead of calculating it.
///
/// When compiled with debug assertions the function asserts the presence of a materialized byte size.
pub fn get_calculated_byte_size(metric: &WithHeader<TraceMetric>) -> usize {
    let bytes = metric.header.as_ref().and_then(|header| header.byte_size);

    debug_assert!(
        bytes.is_some(),
        "processed trace metrics should always have a byte size assigned"
    );

    let bytes = bytes.unwrap_or_else(|| metric.value().map_or(1, calculate_size));

    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// Calculates the byte size for a trace metric.
///
/// Similar to [`relay_ourlogs::calculate_size`].
pub fn calculate_size(metric: &TraceMetric) -> u64 {
    let mut total_size = 0;

    total_size += metric.name.value().map_or(0, |s| s.len());
    total_size += relay_event_normalization::eap::value_size(&metric.value);

    if let Some(attributes) = metric.attributes.value() {
        total_size += relay_event_normalization::eap::attributes_size(attributes);
    }

    u64::try_from(total_size).unwrap_or(u64::MAX).max(1)
}
