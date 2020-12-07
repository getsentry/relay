#![cfg_attr(test, allow(unused_must_use))]

use std::borrow::Cow;

use dynfmt::{Argument, Format, FormatArgs, PythonFormat, SimpleCurlyFormat};

use crate::protocol::LogEntry;
use crate::types::{Annotated, Empty, Error, Meta, ProcessingAction, ProcessingResult, Value};

impl FormatArgs for Value {
    fn get_index(&self, index: usize) -> Result<Option<Argument<'_>>, ()> {
        match self {
            Value::Array(array) => Ok(array
                .get(index)
                .and_then(Annotated::value)
                .map(|v| v as Argument<'_>)),
            _ => Err(()),
        }
    }

    fn get_key(&self, key: &str) -> Result<Option<Argument<'_>>, ()> {
        match self {
            Value::Object(object) => Ok(object
                .get(key)
                .and_then(Annotated::value)
                .map(|v| v as Argument<'_>)),
            _ => Err(()),
        }
    }
}

fn format_message(format: &str, params: &Value) -> Option<String> {
    // NB: This currently resembles the historic logic for formatting strings. It could be much more
    // lenient however, and try multiple formats one after another without exiting early.
    if format.contains('%') {
        PythonFormat
            .format(format, params)
            .ok()
            .map(Cow::into_owned)
    } else if format.contains('{') {
        SimpleCurlyFormat
            .format(format, params)
            .ok()
            .map(Cow::into_owned)
    } else {
        None
    }
}

pub fn normalize_logentry(logentry: &mut LogEntry, meta: &mut Meta) -> ProcessingResult {
    // An empty logentry should just be skipped during serialization. No need for an error.
    if logentry.is_empty() {
        return Ok(());
    }

    if logentry.formatted.value().is_none() && logentry.message.value().is_none() {
        meta.add_error(Error::invalid("no message present"));
        return Err(ProcessingAction::DeleteValueSoft);
    }

    if let Some(params) = logentry.params.value() {
        if logentry.formatted.value().is_none() {
            if let Some(message) = logentry.message.value() {
                if let Some(formatted) = format_message(message.as_ref(), params) {
                    logentry.formatted = Annotated::new(formatted.into());
                }
            }
        }
    }

    // Move `message` to `formatted` if they are equal or only message is given. This also
    // overwrites the meta data on formatted. However, do not move if both of them are None to
    // retain potential meta data on `formatted`.
    if logentry.formatted.value().is_none()
        || logentry.message.value() == logentry.formatted.value()
    {
        logentry.formatted = std::mem::take(&mut logentry.message);
    }

    Ok(())
}

#[cfg(test)]
use crate::types::Object;

#[test]
fn test_format_python() {
    let mut logentry = LogEntry {
        message: Annotated::new("hello, %s!".to_string().into()),
        params: Annotated::new(Value::Array(vec![Annotated::new(Value::String(
            "world".to_string(),
        ))])),
        ..LogEntry::default()
    };

    normalize_logentry(&mut logentry, &mut Meta::default());
    assert_eq_dbg!(logentry.formatted.as_str(), Some("hello, world!"));
}

#[test]
fn test_format_python_named() {
    let mut logentry = LogEntry {
        message: Annotated::new("hello, %(name)s!".to_string().into()),
        params: Annotated::new(Value::Object({
            let mut object = Object::new();
            object.insert(
                "name".to_string(),
                Annotated::new(Value::String("world".to_string())),
            );
            object
        })),
        ..LogEntry::default()
    };

    normalize_logentry(&mut logentry, &mut Meta::default());
    assert_eq_dbg!(logentry.formatted.as_str(), Some("hello, world!"));
}

#[test]
fn test_format_java() {
    let mut logentry = LogEntry {
        message: Annotated::new("hello, {}!".to_string().into()),
        params: Annotated::new(Value::Array(vec![Annotated::new(Value::String(
            "world".to_string(),
        ))])),
        ..LogEntry::default()
    };

    normalize_logentry(&mut logentry, &mut Meta::default());
    assert_eq_dbg!(logentry.formatted.as_str(), Some("hello, world!"));
}

#[test]
fn test_format_dotnet() {
    let mut logentry = LogEntry {
        message: Annotated::new("hello, {0}!".to_string().into()),
        params: Annotated::new(Value::Array(vec![Annotated::new(Value::String(
            "world".to_string(),
        ))])),
        ..LogEntry::default()
    };

    normalize_logentry(&mut logentry, &mut Meta::default());
    assert_eq_dbg!(logentry.formatted.as_str(), Some("hello, world!"));
}

#[test]
fn test_format_no_params() {
    let mut logentry = LogEntry {
        message: Annotated::new("hello, %s!".to_string().into()),
        ..LogEntry::default()
    };

    normalize_logentry(&mut logentry, &mut Meta::default());
    assert_eq_dbg!(logentry.formatted.as_str(), Some("hello, %s!"));
}

#[test]
fn test_only_message() {
    let mut logentry = LogEntry {
        message: Annotated::new("hello, world!".to_string().into()),
        ..LogEntry::default()
    };

    normalize_logentry(&mut logentry, &mut Meta::default());
    assert_eq_dbg!(logentry.message.value(), None);
    assert_eq_dbg!(logentry.formatted.as_str(), Some("hello, world!"));
}

#[test]
fn test_message_formatted_equal() {
    let mut logentry = LogEntry {
        message: Annotated::new("hello, world!".to_string().into()),
        formatted: Annotated::new("hello, world!".to_string().into()),
        ..LogEntry::default()
    };

    normalize_logentry(&mut logentry, &mut Meta::default());
    assert_eq_dbg!(logentry.message.value(), None);
    assert_eq_dbg!(logentry.formatted.as_str(), Some("hello, world!"));
}

#[test]
fn test_empty_missing_message() {
    let mut logentry = LogEntry {
        params: Value::U64(0).into(), // Ensure the logentry is not empty
        ..LogEntry::default()
    };
    let mut meta = Meta::default();

    assert_eq_dbg!(
        normalize_logentry(&mut logentry, &mut meta),
        Err(ProcessingAction::DeleteValueSoft)
    );
    assert!(meta.has_errors());
}

#[test]
fn test_empty_logentry() {
    let mut logentry = LogEntry::default();
    let mut meta = Meta::default();

    assert_eq_dbg!(normalize_logentry(&mut logentry, &mut meta), Ok(()));
    assert!(!meta.has_errors());
}
