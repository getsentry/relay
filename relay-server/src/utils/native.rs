//! Utility methods for native event processing.
//!
//! These functions are invoked by the `EventProcessor`, and are used to prepare native event
//! payloads. See [`process_minidump`] and [`process_apple_crash_report`] for more information.

use chrono::{TimeZone, Utc};
use minidump::Minidump;

use relay_general::protocol::{Event, Exception, JsonLenientString, Level, Mechanism, Values};
use relay_general::types::Annotated;

/// Placeholder payload fragments indicating a native event.
///
/// These payload attributes tell the processing pipeline that the event requires attachment
/// processing and serve as defaults for failed events. When updating these values, also check the
/// processing pipeline in Sentry.
///
/// The [`mechanism_type`](Self::mechanism_type) field is the most important field, as this is the
/// primary indicator for processing. All other fields are mere defaults.
#[derive(Debug)]
struct NativePlaceholder {
    /// The `exception.type` attribute value rendered in the issue.
    exception_type: &'static str,
    /// The default `exception.value` shown in the issue if processing fails.
    exception_value: &'static str,
    /// The `exception.mechanism.type` attribute, which is the primary indicator for processing.
    mechanism_type: &'static str,
}

/// Writes a placeholder to indicate that this event has an associated minidump or an apple
/// crash report.
///
/// This will indicate to the ingestion pipeline that this event will need to be processed. The
/// payload can be checked via `is_minidump_event`.
fn write_native_placeholder(event: &mut Event, placeholder: NativePlaceholder) {
    // Events must be native platform.
    let platform = event.platform.value_mut();
    *platform = Some("native".to_string());

    // Assume that this minidump is the result of a crash and assign the fatal
    // level. Note that the use of `setdefault` here doesn't generally allow the
    // user to override the minidump's level as processing will overwrite it
    // later.
    event.level.get_or_insert_with(|| Level::Fatal);

    // Create a placeholder exception. This signals normalization that this is an
    // error event and also serves as a placeholder if processing of the minidump
    // fails.
    let exceptions = event
        .exceptions
        .value_mut()
        .get_or_insert_with(Values::default)
        .values
        .value_mut()
        .get_or_insert_with(Vec::new);

    exceptions.clear(); // clear previous errors if any

    exceptions.push(Annotated::new(Exception {
        ty: Annotated::new(placeholder.exception_type.to_string()),
        value: Annotated::new(JsonLenientString(placeholder.exception_value.to_string())),
        mechanism: Annotated::new(Mechanism {
            ty: Annotated::from(placeholder.mechanism_type.to_string()),
            handled: Annotated::from(false),
            synthetic: Annotated::from(true),
            ..Mechanism::default()
        }),
        ..Exception::default()
    }));
}

/// Extracts information from the minidump and writes it into the given event.
///
/// This function operates at best-effort. It always attaches the placeholder and returns
/// successfully, even if the minidump or part of its data cannot be parsed.
pub fn process_minidump(event: &mut Event, data: &[u8]) {
    let placeholder = NativePlaceholder {
        exception_type: "Minidump",
        exception_value: "Invalid Minidump",
        mechanism_type: "minidump",
    };
    write_native_placeholder(event, placeholder);

    let minidump = match Minidump::read(data) {
        Ok(minidump) => minidump,
        Err(err) => {
            relay_log::debug!("Failed to parse minidump: {:?}", err);
            return;
        }
    };

    // Use the minidump's timestamp as the event's primary time. This timestamp can lie multiple
    // days in the past, in which case the event may be rejected in store normalization.
    let timestamp = Utc.timestamp(minidump.header.time_date_stamp.into(), 0);
    event.timestamp.set_value(Some(timestamp.into()));
}

/// Writes minimal information into the event to indicate it is associated with an Apple Crash
/// Report.
pub fn process_apple_crash_report(event: &mut Event, _data: &[u8]) {
    let placeholder = NativePlaceholder {
        exception_type: "AppleCrashReport",
        exception_value: "Invalid Apple Crash Report",
        mechanism_type: "applecrashreport",
    };
    write_native_placeholder(event, placeholder);
}
