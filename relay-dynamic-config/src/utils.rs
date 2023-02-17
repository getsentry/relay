use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Validate that the given JSON resolves to a valid object of type `S`.
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
            Config::new(CompareMode::Inclusive),
        )
        .map_err(|e| anyhow::anyhow!(e));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProjectConfig;

    #[test]
    fn test_validate_json() {
        // Empty config:
        assert!(validate_json::<ProjectConfig>("{}", false).is_ok());
        // Invalid config:
        assert!(validate_json::<ProjectConfig>(
            "{\"dynamicSampling\": \"this should be an object\"}",
            false
        )
        .is_err());
        // Config with additional fields:
        assert!(validate_json::<ProjectConfig>("{\"more\": 1}", false).is_ok());
        assert!(validate_json::<ProjectConfig>("{\"more\": 1}", true).is_err());

        // Config with nested additional fields:
        let config = r#"{"dynamicSampling": {"rulesV2": [], "notRules": []}}"#;
        assert!(validate_json::<ProjectConfig>(config, false).is_ok());
        assert!(validate_json::<ProjectConfig>(config, true).is_err());

        // Correct config, with strict check:
        let config = r#"{"dynamicSampling": {"rulesV2": []}}"#;
        let res = validate_json::<ProjectConfig>(config, true);
        assert!(res.is_ok(), "{:?}", res);
    }
}
