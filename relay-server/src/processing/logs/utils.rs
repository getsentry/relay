use relay_event_schema::protocol::OurLog;

use crate::envelope::WithHeader;

/// Returns the calculated size of a [`OurLog`].
///
/// Unlike [`relay_ourlogs::calculate_size`], this accesses the already manifested byte size
/// of the log, instead of calculating it.
///
/// When compiled with debug assertion the function asserts the presence of a manifested byte size.
pub fn get_calculated_byte_size(log: &WithHeader<OurLog>) -> usize {
    // Use the remembered byte size here, as this is the one Relay stored when initially
    // receiving the log item. This will not include any modifications done to the log.
    let bytes = log.header.as_ref().and_then(|header| header.byte_size);

    debug_assert!(
        bytes.is_some(),
        "processed logs should always have a byte size assigned"
    );

    // Only fall back to a calculated size, when there is nothing stored.
    // This should never happen, logs should have a size assigned to them immediately after
    // they have been expanded.
    let bytes = bytes.unwrap_or_else(|| log.value().map_or(1, relay_ourlogs::calculate_size));

    usize::try_from(bytes).unwrap_or(usize::MAX)
}
