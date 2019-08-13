//! Implements event filtering based on the error message
//!
//! Specific values in the error message or in the exception values can be used to
//! filter messages with this filter.

use crate::filter::common::is_glob_match;
use crate::filter::config::ErrorMessagesFilterConfig;
use crate::filter::FilterStatKey;
use crate::protocol::Event;
use crate::types::Empty;

/// Filters events by patterns in their error messages.
pub fn should_filter(
    event: &Event,
    config: &ErrorMessagesFilterConfig,
) -> Result<(), FilterStatKey> {
    if let Some(logentry) = event.logentry.value() {
        if let Some(message) = logentry.formatted.value() {
            should_filter_impl(message, config)?;
        } else if let Some(message) = logentry.message.value() {
            should_filter_impl(message, config)?;
        }
    }

    if let Some(exception_values) = event.exceptions.value() {
        if let Some(exceptions) = exception_values.values.value() {
            for exception in exceptions {
                if let Some(exception) = exception.value() {
                    let message_owned;
                    let message = match (exception.ty.is_empty(), exception.value.is_empty()) {
                        (true, true) => "",
                        (false, true) => exception.ty.value().unwrap(),
                        (true, false) => exception.value.value().unwrap(),
                        (false, false) => {
                            message_owned = format!(
                                "{}: {}",
                                exception.ty.value().unwrap(),
                                exception.value.value().unwrap().0
                            );
                            &message_owned
                        }
                    };

                    should_filter_impl(&message, config)?;
                }
            }
        }
    }

    Ok(())
}

fn should_filter_impl(
    message: &str,
    config: &ErrorMessagesFilterConfig,
) -> Result<(), FilterStatKey> {
    if !message.is_empty() {
        for pattern in &config.patterns {
            if is_glob_match(pattern, message) {
                return Err(FilterStatKey::ErrorMessage);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Exception, LogEntry, Values};
    use crate::types::Annotated;

    #[test]
    fn test_should_filter_exception() {
        let configs = &[
            // with globs
            ErrorMessagesFilterConfig {
                patterns: vec![
                    "filteredexception*".to_string(),
                    "*this is a filtered exception.".to_string(),
                    "".to_string(),
                    "this is".to_string(),
                ],
            },
            // without globs
            ErrorMessagesFilterConfig {
                patterns: vec![
                    "filteredexception: this is a filtered exception.".to_string(),
                    "filteredexception".to_string(),
                    "this is a filtered exception.".to_string(),
                ],
            },
        ];

        let cases = &[
            (
                Some("UnfilteredException"),
                None,
                "UnfilteredException",
                true,
            ),
            (
                None,
                Some("This is an unfiltered exception."),
                "This is an unfiltered exception.",
                true,
            ),
            (None, None, "This is an unfiltered exception.", true),
            (None, None, "", true),
            (
                Some("UnfilteredException"),
                Some("This is an unfiltered exception."),
                "UnfilteredException: This is an unfiltered exception.",
                true,
            ),
            (Some("FilteredException"), None, "FilteredException", false),
            (
                None,
                Some("This is a filtered exception."),
                "This is a filtered exception.",
                false,
            ),
            (None, None, "This is a filtered exception.", false),
            (
                Some("FilteredException"),
                Some("This is a filtered exception."),
                "FilteredException: This is a filtered exception.",
                false,
            ),
            (
                Some("OtherException"),
                Some("This is a random exception."),
                "FilteredException: This is a filtered exception.",
                false,
            ),
            (
                None,
                None,
                "FilteredException: This is a filtered exception.",
                false,
            ),
            (
                Some("FilteredException"),
                Some("This is a filtered exception."),
                "hi this is a legit log message",
                false,
            ),
        ];

        for config in &configs[..] {
            for &case in &cases[..] {
                // Useful output to debug which testcase fails. Hidden if the test passes.
                println!(
                    "------------------------------------------------------------------------"
                );
                println!("Config: {:?}", config);
                println!("Case: {:?}", case);

                let (exc_type, exc_value, logentry_formatted, should_ingest) = case;
                let event = Event {
                    exceptions: Annotated::new(Values::new(vec![Annotated::new(Exception {
                        ty: Annotated(exc_type.map(ToString::to_string), Default::default()),
                        value: Annotated(
                            exc_value.map(ToString::to_string).map(From::from),
                            Default::default(),
                        ),
                        ..Default::default()
                    })])),
                    logentry: Annotated::new(LogEntry {
                        formatted: Annotated::new(logentry_formatted.to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                assert_eq!(
                    should_filter(&event, config),
                    if should_ingest {
                        Ok(())
                    } else {
                        Err(FilterStatKey::ErrorMessage)
                    }
                );
            }
        }
    }
}
