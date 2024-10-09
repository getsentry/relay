//! Implements event filtering based on the error message
//!
//! Specific values in the error message or in the exception values can be used to
//! filter messages with this filter.

use std::borrow::Cow;

use relay_pattern::Patterns;

use crate::{ErrorMessagesFilterConfig, FilterStatKey, Filterable};

/// Checks events by patterns in their error messages.
fn matches<F: Filterable>(item: &F, patterns: &Patterns) -> bool {
    if let Some(logentry) = item.logentry() {
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

    if let Some(exception_values) = item.exceptions() {
        if let Some(exceptions) = exception_values.values.value() {
            for exception in exceptions {
                if let Some(exception) = exception.value() {
                    let ty = exception.ty.as_str().unwrap_or_default();
                    let value = exception.value.as_str().unwrap_or_default();
                    let message = match (ty, value) {
                        ("", value) => Cow::Borrowed(value),
                        (ty, "") => Cow::Borrowed(ty),
                        (ty, value) => Cow::Owned(format!("{ty}: {value}")),
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
pub fn should_filter<F: Filterable>(
    item: &F,
    config: &ErrorMessagesFilterConfig,
) -> Result<(), FilterStatKey> {
    if matches(item, &config.patterns) {
        Err(FilterStatKey::ErrorMessage)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{Event, Exception, LogEntry, Values};
    use relay_pattern::TypedPatterns;
    use relay_protocol::Annotated;

    use super::*;

    #[test]
    fn test_should_filter_exception() {
        let configs = &[
            // with globs
            ErrorMessagesFilterConfig {
                patterns: TypedPatterns::from([
                    "filteredexception*".to_owned(),
                    "*this is a filtered exception.".to_owned(),
                    "".to_owned(),
                    "this is".to_owned(),
                ]),
            },
            // without globs
            ErrorMessagesFilterConfig {
                patterns: TypedPatterns::from([
                    "filteredexception: this is a filtered exception.".to_owned(),
                    "filteredexception".to_owned(),
                    "this is a filtered exception.".to_owned(),
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

    #[test]
    fn test_filter_hydration_error() {
        let pattern =
            "*https://reactjs.org/docs/error-decoder.html?invariant={418,419,422,423,425}*";
        let config = ErrorMessagesFilterConfig {
            patterns: TypedPatterns::from([pattern.to_owned()]),
        };

        let event = Annotated::<Event>::from_json(
            r#"{
                "exception": {
                    "values": [
                        {
                            "type": "Error",
                            "value": "Minified React error #423; visit https://reactjs.org/docs/error-decoder.html?invariant=423 for the full message or use the non-minified dev environment for full errors and additional helpful warnings."
                        }
                    ]
                }
            }"#,
        ).unwrap();

        assert!(should_filter(&event.0.unwrap(), &config) == Err(FilterStatKey::ErrorMessage));
    }

    #[test]
    fn test_filter_chunk_load_error() {
        let errors = [
            "Error: Uncaught (in promise): ChunkLoadError: Loading chunk 175 failed.",
            "Uncaught (in promise): ChunkLoadError: Loading chunk 175 failed.",
            "ChunkLoadError: Loading chunk 552 failed.",
        ];

        let config = ErrorMessagesFilterConfig {
            patterns: TypedPatterns::from([
                "ChunkLoadError: Loading chunk *".to_owned(),
                "*Uncaught *: ChunkLoadError: Loading chunk *".to_owned(),
            ]),
        };

        for error in errors {
            let event = Event {
                logentry: Annotated::new(LogEntry {
                    formatted: Annotated::new(error.to_owned().into()),
                    ..Default::default()
                }),
                ..Default::default()
            };

            assert_eq!(
                should_filter(&event, &config),
                Err(FilterStatKey::ErrorMessage)
            );
        }
    }
}
