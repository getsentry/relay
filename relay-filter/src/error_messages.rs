//! Implements event filtering based on the error message
//!
//! Specific values in the error message or in the exception values can be used to
//! filter messages with this filter.

use std::borrow::Cow;

use relay_general::protocol::Event;

use crate::{ErrorMessagesFilterConfig, FilterStatKey, GlobPatterns};

/// Filters events by patterns in their error messages.
pub fn matches(event: &Event, patterns: &GlobPatterns) -> bool {
    if let Some(logentry) = event.logentry.value() {
        if let Some(message) = logentry.formatted.value() {
            if patterns.is_match(message.as_ref()) {
                return true;
            }
        } else if let Some(message) = logentry.message.value() {
            if patterns.is_match(message.as_ref()) {
                return true;
            }
        }
    }

    if let Some(exception_values) = event.exceptions.value() {
        if let Some(exceptions) = exception_values.values.value() {
            for exception in exceptions {
                if let Some(exception) = exception.value() {
                    let ty = exception.ty.as_str().unwrap_or_default();
                    let value = exception.value.as_str().unwrap_or_default();
                    let message = match (ty, value) {
                        ("", value) => Cow::Borrowed(value),
                        (ty, "") => Cow::Borrowed(ty),
                        (ty, value) => Cow::Owned(format!("{}: {}", ty, value)),
                    };
                    if patterns.is_match(message.as_ref()) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Filters events by patterns in their error messages.
pub fn should_filter(
    event: &Event,
    config: &ErrorMessagesFilterConfig,
) -> Result<(), FilterStatKey> {
    if matches(event, &config.patterns) {
        Err(FilterStatKey::ErrorMessage)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_general::protocol::{Exception, LogEntry, Values};
    use relay_general::types::Annotated;

    use crate::GlobPatterns;

    #[test]
    fn test_should_filter_exception() {
        let configs = &[
            // with globs
            ErrorMessagesFilterConfig {
                patterns: GlobPatterns::new(vec![
                    "filteredexception*".to_string(),
                    "*this is a filtered exception.".to_string(),
                    "".to_string(),
                    "this is".to_string(),
                ]),
            },
            // without globs
            ErrorMessagesFilterConfig {
                patterns: GlobPatterns::new(vec![
                    "filteredexception: this is a filtered exception.".to_string(),
                    "filteredexception".to_string(),
                    "this is a filtered exception.".to_string(),
                ]),
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
                        ty: Annotated::from(exc_type.map(str::to_string)),
                        value: Annotated::from(exc_value.map(str::to_owned).map(From::from)),
                        ..Default::default()
                    })])),
                    logentry: Annotated::new(LogEntry {
                        formatted: Annotated::new(logentry_formatted.to_string().into()),
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
