use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use relay_event_normalization::MeasurementsConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A dynamic configuration for all Relays passed down from Sentry.
///
/// Values shared across all projects may also be included here, to keep
/// [`ProjectConfig`](crate::ProjectConfig)s small.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct GlobalConfig {
    /// Configuration for measurements normalization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<MeasurementsConfig>,
    /// Sentry options passed down to Relay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Options>,
}

impl GlobalConfig {
    /// Loads the [`GlobalConfig`] from a file if it's provided.
    ///
    /// The folder_path argument should be the path to the folder where the Relay config and
    /// credentials are stored.
    pub fn load(folder_path: &Path) -> anyhow::Result<Option<Self>> {
        let path = folder_path.join("global_config.json");

        if path.exists() {
            let file = BufReader::new(File::open(path)?);
            Ok(Some(serde_json::from_reader(file)?))
        } else {
            Ok(None)
        }
    }

    /// Returns the [`Options::cardinality_limiter_mode`] option.
    pub fn cardinality_limiter_mode(&self) -> CardinalityLimiterMode {
        self.options
            .as_ref()
            .map(|o| o.cardinality_limiter_mode)
            .unwrap_or_default()
    }
}

/// All options passed down from Sentry to Relay.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Options {
    /// List of platform names for which we allow using unsampled profiles for the purpose
    /// of improving profile (function) metrics
    #[serde(rename = "profiling.profile_metrics.unsampled_profiles.platforms")]
    pub profile_metrics_allowed_platforms: Vec<String>,

    /// Sample rate for tuning the amount of unsampled profiles that we "let through"
    #[serde(rename = "profiling.profile_metrics.unsampled_profiles.sample_rate")]
    pub profile_metrics_sample_rate: f32,

    /// Kill switch for shutting down profile metrics
    #[serde(rename = "profiling.profile_metrics.unsampled_profiles.enabled")]
    pub unsampled_profiles_enabled: bool,

    /// Kill switch for controlling the cardinality limiter.
    #[serde(rename = "relay.cardinality-limiter.mode")]
    pub cardinality_limiter_mode: CardinalityLimiterMode,

    /// All other unknown options.
    #[serde(flatten)]
    other: HashMap<String, Value>,
}

/// Kill switch for controlling the cardinality limiter.
#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub enum CardinalityLimiterMode {
    /// Cardinality limiter is enabled.
    #[default]
    // De-serialize from the empty string, because the option was added to
    // Sentry incorrectly which makes Sentry send the empty string as a default.
    #[serde(alias = "")]
    Enabled,
    /// Cardinality limiter is enabled but cardinality limits are not enforced.
    Passive,
    /// Cardinality limiter is disabled.
    Disabled,
}

#[cfg(test)]
mod tests {
    use relay_base_schema::metrics::MetricUnit;
    use relay_event_normalization::{BuiltinMeasurementKey, MeasurementsConfig};

    use super::*;

    #[test]
    fn test_global_config_roundtrip() {
        let global_config = GlobalConfig {
            measurements: Some(MeasurementsConfig {
                builtin_measurements: vec![
                    BuiltinMeasurementKey::new("foo", MetricUnit::None),
                    BuiltinMeasurementKey::new("bar", MetricUnit::None),
                    BuiltinMeasurementKey::new("baz", MetricUnit::None),
                ],
                max_custom_measurements: 5,
            }),
            options: Some(Options {
                other: HashMap::from([("relay.unknown".to_owned(), Value::Bool(true))]),
                ..Default::default()
            }),
        };

        let serialized =
            serde_json::to_string(&global_config).expect("failed to serialize GlobalConfig");

        let deserialized = serde_json::from_str::<GlobalConfig>(serialized.as_str())
            .expect("failed to deserialize GlobalConfig");

        assert_eq!(deserialized, global_config);
    }

    #[test]
    fn test_cardinality_limiter_mode_de_serialize() {
        let m: CardinalityLimiterMode = serde_json::from_str("\"\"").unwrap();
        assert_eq!(m, CardinalityLimiterMode::Enabled);
        let m: CardinalityLimiterMode = serde_json::from_str("\"enabled\"").unwrap();
        assert_eq!(m, CardinalityLimiterMode::Enabled);
        let m: CardinalityLimiterMode = serde_json::from_str("\"disabled\"").unwrap();
        assert_eq!(m, CardinalityLimiterMode::Disabled);
        let m: CardinalityLimiterMode = serde_json::from_str("\"passive\"").unwrap();
        assert_eq!(m, CardinalityLimiterMode::Passive);

        let m = serde_json::to_string(&CardinalityLimiterMode::Enabled).unwrap();
        assert_eq!(m, "\"enabled\"");
    }
}
