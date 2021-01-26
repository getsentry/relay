//! Utility methods for native event processing.
//!
//! These functions are invoked by the `EventProcessor`, and are used to prepare native event
//! payloads. See [`process_minidump`] and [`process_apple_crash_report`] for more information.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use minidump::{MinidumpAnnotation, MinidumpCrashpadInfo, MinidumpModuleList, Module};

use relay_general::protocol::{
    Context, ContextInner, Contexts, Event, Exception, JsonLenientString, Level, Mechanism, Values,
};
use relay_general::types::{Annotated, Value};

type Minidump<'a> = minidump::Minidump<'a, &'a [u8]>;

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

/// Generates crashpad contexts for annotations stored in the minidump.
///
/// Returns an error if either the minidump module list or the crashpad information stream cannot be
/// loaded from the minidump. Returns `Ok(())` in all other cases, including when no annotations are
/// present.
///
/// Crashpad has global annotations, and per-module annotations. For each of these, a separate
/// context of type "crashpad" is added, which contains the annotations as key-value mapping. List
/// annotations are added to an "annotations" JSON list.
fn write_crashpad_annotations(
    event: &mut Event,
    minidump: &Minidump<'_>,
) -> Result<(), minidump::Error> {
    let module_list = minidump.get_stream::<MinidumpModuleList>()?;
    let crashpad_info = match minidump.get_stream::<MinidumpCrashpadInfo>() {
        Err(minidump::Error::StreamNotFound) => return Ok(()),
        result => result?,
    };

    let contexts = event.contexts.get_or_insert_with(Contexts::new);

    if !crashpad_info.simple_annotations.is_empty() {
        // First, create a generic crashpad context with top-level simple annotations. This context does
        // not need a type field, since its type matches the the key.
        let crashpad_context = crashpad_info
            .simple_annotations
            .into_iter()
            .map(|(key, value)| (key, Annotated::new(Value::from(value))))
            .collect();

        contexts.insert(
            "crashpad".to_string(),
            Annotated::new(ContextInner(Context::Other(crashpad_context))),
        );
    }

    if crashpad_info.module_list.is_empty() {
        return Ok(());
    }

    let modules = module_list.iter().collect::<Vec<_>>();

    for module_info in crashpad_info.module_list {
        // Resolve the actual module entry in the minidump module list. This entry should always
        // exist and crashpad module info with an invalid link can be discarded. Since this is
        // non-essential information, we skip gracefully and only emit debug logs.
        let module = match modules.get(module_info.module_index) {
            Some(module) => module,
            None => {
                relay_log::debug!(
                    "Skipping invalid minidump module index {}",
                    module_info.module_index
                );
                continue;
            }
        };

        // Use the basename of the code file (library or executable name) as context name. The
        // context type must be set explicitly in this case, which will render in Sentry as
        // "Module.dll (crashpad)".
        let code_file = module.code_file();
        let (_, module_name) = symbolic::common::split_path(&code_file);

        let mut module_context = BTreeMap::new();
        module_context.insert(
            "type".to_owned(),
            Annotated::new(Value::String("crashpad".to_owned())),
        );

        for (key, value) in module_info.simple_annotations {
            module_context.insert(key, Annotated::new(Value::String(value)));
        }

        for (key, annotation) in module_info.annotation_objects {
            if let MinidumpAnnotation::String(value) = annotation {
                module_context.insert(key, Annotated::new(Value::String(value)));
            }
        }

        if !module_info.list_annotations.is_empty() {
            // Annotation lists do not maintain a key-value mapping, so instead write them to an
            // "annotations" key within the module context. This will render as a JSON list in Sentry.
            let annotation_list = module_info
                .list_annotations
                .into_iter()
                .map(|s| Annotated::new(Value::String(s)))
                .collect();

            module_context.insert(
                "annotations".to_owned(),
                Annotated::new(Value::Array(annotation_list)),
            );
        }

        contexts.insert(
            module_name.to_owned(),
            Annotated::new(ContextInner(Context::Other(module_context))),
        );
    }

    Ok(())
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

    // Write annotations from the crashpad info stream, but skip gracefully on error. Annotations
    // are non-essential to processing.
    if let Err(err) = write_crashpad_annotations(event, &minidump) {
        // TODO: Consider adding an event error for failed annotation extraction.
        relay_log::debug!("Failed to parse minidump module list: {:?}", err);
    }
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
