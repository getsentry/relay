//! Measurements normalizations for SpanV2 attributes.

use relay_conventions::consts::*;
use relay_event_schema::protocol::Attributes;

/// Compute additional measurements derived from existing ones.
///
/// The added measurements are:
///
/// ```text
/// frames_slow_rate := measurements.frames_slow / measurements.frames_total
/// frames_frozen_rate := measurements.frames_frozen / measurements.frames_total
/// stall_percentage := measurements.stall_total_time / transaction.duration
/// ```
pub fn compute_additional_measurements(
    transaction_duration_ms: Option<f64>,
    attributes: &mut Attributes,
) {
    if let Some(frames_total) = attributes
        .get_value(APP__VITALS__FRAMES__TOTAL__COUNT)
        .and_then(|v| v.as_f64())
        && frames_total > 0.0
    {
        if let Some(frames_frozen) = attributes
            .get_value(APP__VITALS__FRAMES__FROZEN__COUNT)
            .and_then(|v| v.as_f64())
        {
            let frames_frozen_rate = frames_frozen / frames_total;
            attributes.insert("frames_frozen_rate".to_owned(), frames_frozen_rate);
        }

        if let Some(frames_slow) = attributes
            .get_value(APP__VITALS__FRAMES__SLOW__COUNT)
            .and_then(|v| v.as_f64())
        {
            let frames_slow_rate = frames_slow / frames_total;
            attributes.insert("frames_slow_rate".to_owned(), frames_slow_rate);
        }
    }

    // Get stall_percentage
    if let Some(transaction_duration_ms) = transaction_duration_ms
        && transaction_duration_ms > 0.0
        && let Some(stall_total_time) = attributes
            .get_value("stall_total_time")
            .and_then(|v| v.as_f64())
    {
        let stall_percentage = stall_total_time / transaction_duration_ms;
        attributes.insert("stall_percentage".to_owned(), stall_percentage);
    }
}
