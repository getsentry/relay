use relay_event_schema::protocol::OurLog;
use relay_protocol::Annotated;

/// Returns the calculated size of a [`OurLog`].
///
/// Unlike [`relay_ourlogs::calculate_size`], this access the already manifested byte size
/// of the log, instead of calculating it.
///
/// When compiled with debug assertion the function asserts the presence of a manifested byte size.
pub fn get_calculated_byte_size(log: &Annotated<OurLog>) -> usize {
    // Use the remembered byte size here, as this is the one Relay stored when initially
    // receiving the log item. This will not include any modifications done to the log.
    let bytes = log
        .value()
        .and_then(|v| v.__headers.value())
        .and_then(|v| v.byte_size.value())
        .copied();

    debug_assert!(
        bytes.is_some(),
        "processed logs should always have a byte size assigned"
    );

    // Only fall back to a calculated size, when there is nothing stored.
    // This should never happen, logs should have a size assigned to them immediately after
    // they have been expanded.
    let bytes = bytes.unwrap_or_else(|| log.value().map_or(0, relay_ourlogs::calculate_size));

    usize::try_from(bytes).unwrap_or(usize::MAX)
}
