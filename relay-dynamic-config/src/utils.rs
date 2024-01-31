use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Normalizes the given value by deserializing it and serializing it back.
pub fn normalize_json<'de, S>(value: &'de str) -> anyhow::Result<String>
where
    S: Serialize + Deserialize<'de>,
{
    let deserialized: S = serde_json::from_str(value)?;
    let serialized = serde_json::to_value(&deserialized)?.to_string();
    Ok(serialized)
}

/// Validate that the given JSON resolves to a valid instance of type `S`.
///
/// If `strict` is true, verify that reserializing the instance results in the same fields.
pub fn validate_json<'de, S>(value: &'de str, strict: bool) -> anyhow::Result<()>
where
    S: Serialize + Deserialize<'de>,
{
    let deserialized: S = serde_json::from_str(value)?;
    if strict {
        let deserialized_value: Value = serde_json::from_str(value)?;
        let reserialized = serde_json::to_value(&deserialized)?;
        return assert_json_matches_no_panic(
            &deserialized_value,
            &reserialized,
            Config::new(CompareMode::Strict),
        )
        .map_err(|e| anyhow::anyhow!(e));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[derive(Default, Serialize, Deserialize)]
    struct TestMe {
        a: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        b: Option<Box<TestMe>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        features: Option<BTreeSet<Feature>>,
    }

    use std::collections::BTreeSet;

    use crate::Feature;

    use super::*;

    #[test]
    fn test_validate_json() {
        for (input, strict, expected_error) in [
            (r#"{}"#, false, "missing field `a` at line 1 column 2"),
            (r#"{}"#, true, "missing field `a` at line 1 column 2"),
            (r#"{"a": 1}"#, false, ""),
            (r#"{"a": 1}"#, true, ""),
            (r#"{"a": 1, "other": 666}"#, false, ""),
            (
                r#"{"a": 1, "other": 666}"#,
                true,
                "json atom at path \".other\" is missing from rhs",
            ),
            (r#"{"a": 1, "b": {"a": 2}}"#, true, ""),
            (r#"{"b": {"a": 2}, "a": 1}"#, true, ""),
            (r#"{"a": 1, "b": {"a": 2, "other": 666}}"#, false, ""),
            (
                r#"{"a": 1, "b": {"a": 2, "other": 666}}"#,
                true,
                "json atom at path \".b.other\" is missing from rhs",
            ),
            (
                // An unknown feature flag will not be serialized by Relay.
                r#"{"a": 1, "features": ["organizations:is-cool"]}"#,
                true,
                r#"json atoms at path ".features[0]" are not equal:
    lhs:
        "organizations:is-cool"
    rhs:
        "Unknown""#,
            ),
            (
                // A deprecated feature flag for this relay is still serialized
                // for the following relays in chain.
                r#"{"a": 1, "features": ["organizations:profiling"]}"#,
                true,
                "",
            ),
        ] {
            let res = validate_json::<TestMe>(input, strict);
            if expected_error.is_empty() {
                assert!(res.is_ok(), "{:?}", (input, res.unwrap_err()));
            } else {
                assert!(res.is_err(), "{input}");
                assert_eq!(res.unwrap_err().to_string(), expected_error, "{input}");
            }
        }
    }
}
