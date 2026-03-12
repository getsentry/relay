use relay_event_schema::protocol::OurLog;

/// Calculates a canonical size of a log item.
///
/// This size is passed along from Relay until storage and will eventually be emitted as an
/// outcome.
///
/// Simple data types have a fixed size assigned to them:
///  - Boolean: 1 byte
///  - Integer: 8 byte
///  - Double: 8 Byte
///  - Strings are counted by their byte (UTF-8 encoded) representation.
///
/// Complex types like objects and arrays are counted as the sum of all contained simple data types.
///
/// Considered for the size of a log are all attribute keys, values and the log message body.
/// An empty log (no message, no attributes) is counted as 1 byte.
///
/// The byte size should only be calculated once before any processing, enrichment and other
/// modifications done by Relay.
pub fn calculate_size(log: &OurLog) -> u64 {
    let mut total_size = 0;

    total_size += log.body.value().map_or(0, |s| s.len());

    if let Some(attributes) = log.attributes.value() {
        total_size += relay_event_normalization::eap::attributes_size(attributes);
    }

    u64::try_from(total_size).unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;

    macro_rules! assert_calculated_size_of {
        ($expected:expr, $json:expr) => {{
            let json = $json;

            let log = Annotated::<OurLog>::from_json(json)
                .unwrap()
                .into_value()
                .unwrap();

            let size = calculate_size(&log);
            assert_eq!(size, $expected, "log: {json}");
        }};
    }

    #[test]
    fn test_calculate_size_string_with_body() {
        assert_calculated_size_of!(
            43,
            r#"{
            "body": "7 bytes",
            "attributes": {
                "foo": {
                    "value": "ඞ and some more equals 33 bytes",
                    "type": "string"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_integer_with_body() {
        assert_calculated_size_of!(
            18,
            r#"{
            "body": "7 bytes",
            "attributes": {
                "foo": {
                    "value": 12,
                    "type": "integer"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_double_with_body() {
        assert_calculated_size_of!(
            18,
            r#"{
            "body": "7 bytes",
            "attributes": {
                "foo": {
                    "value": 42.0,
                    "type": "double"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_bool_with_body() {
        assert_calculated_size_of!(
            11,
            r#"{
            "body": "7 bytes",
            "attributes": {
                "foo": {
                    "value": true,
                    "type": "boolean"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_null() {
        assert_calculated_size_of!(
            10,
            r#"{
            "body": "7 bytes",
            "attributes": {
                "foo": {
                    "value": null,
                    "type": "double"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_missing() {
        assert_calculated_size_of!(
            10,
            r#"{
            "body": "7 bytes",
            "attributes": {
                "foo": {
                    "type": "integer"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_invalid_string() {
        assert_calculated_size_of!(
            43,
            r#"{
            "body": "7 bytes",
            "attributes": {
                "foo": {
                    "value": "ඞ and some more equals 33 bytes",
                    "type": "this is invalid and still counts"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_full_log() {
        assert_calculated_size_of!(
            89,
            r#"{
            "timestamp": 946684800.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "level": "warn",
            "body": "7 bytes",
            "some other": "fields that do not exist",
            "attributes": {
                "k1": {
                    "value": "string value",
                    "type": "string"
                },
                "k2": {
                    "value": 18446744073709551615,
                    "type": "integer"
                },
                "k3": {
                    "value": 42.01234567891234567899,
                    "type": "double"
                },
                "k4": {
                    "value": false,
                    "type": "boolean"
                },
                "k5": {
                    "value": {
                        "nested": {
                            "array": [1.0, 2, -12, "7 bytes", false]
                        }
                    },
                    "type": "not yet supported"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_empty_log_is_1byte() {
        assert_calculated_size_of!(1, r#"{}"#);
    }
}
