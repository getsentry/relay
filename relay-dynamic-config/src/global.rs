use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use relay_base_schema::metrics::MetricNamespace;
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
    #[serde(
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub options: Options,
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
}

/// All options passed down from Sentry to Relay.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Options {
    /// List of platform names for which we allow using unsampled profiles for the purpose
    /// of improving profile (function) metrics
    #[serde(
        rename = "profiling.profile_metrics.unsampled_profiles.platforms",
        deserialize_with = "default_on_error",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub profile_metrics_allowed_platforms: Vec<String>,

    /// Sample rate for tuning the amount of unsampled profiles that we "let through"
    #[serde(
        rename = "profiling.profile_metrics.unsampled_profiles.sample_rate",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub profile_metrics_sample_rate: f32,

    /// Kill switch for shutting down profile metrics
    #[serde(
        rename = "profiling.profile_metrics.unsampled_profiles.enabled",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub unsampled_profiles_enabled: bool,

    /// Kill switch for controlling the cardinality limiter.
    #[serde(
        rename = "relay.cardinality-limiter.mode",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub cardinality_limiter_mode: CardinalityLimiterMode,

    /// Sample rate for Cardinality Limiter Sentry errors.
    ///
    /// Rate needs to be between `0.0` and `1.0`.
    /// If set to `1.0` all cardinality limiter rejections will be logged as a Sentry error.
    #[serde(
        rename = "relay.cardinality-limiter.error-sample-rate",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub cardinality_limiter_error_sample_rate: f32,

    /// Kill switch for disabling the span usage metric.
    ///
    /// This metric is converted into outcomes in a sentry-side consumer.
    #[serde(
        rename = "relay.span-usage-metric",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub span_usage_metric: bool,

    /// Metric bucket encoding configuration by metric namespace.
    #[serde(
        rename = "relay.metric-bucket-encodings",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub metric_bucket_encodings: MetricBucketEncodings,

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

/// Configuration container to control [`MetricEncoding`] per namespace.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[serde(default)]
pub struct MetricBucketEncodings {
    sessions: MetricEncoding,
    transactions: MetricEncoding,
    spans: MetricEncoding,
    custom: MetricEncoding,
    unsupported: MetricEncoding,
}

impl MetricBucketEncodings {
    /// Returns the configured encoding for a specific namespace.
    pub fn for_namespace(&self, namespace: MetricNamespace) -> MetricEncoding {
        match namespace {
            MetricNamespace::Sessions => self.sessions,
            MetricNamespace::Transactions => self.transactions,
            MetricNamespace::Spans => self.spans,
            MetricNamespace::Custom => self.custom,
            MetricNamespace::Unsupported => self.unsupported,
        }
    }
}

/// All supported metric bucket encodings.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MetricEncoding {
    /// The default compatibility encoding.
    ///
    /// A simple JSON array of numbers.
    #[default]
    Compat,
    /// The array encoding.
    ///
    /// Uses already the dynamic value format but still encodes
    /// all values as a JSON number array.
    Array,
}

/// Returns `true` if this value is equal to `Default::default()`.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

fn default_on_error<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: Default + serde::de::DeserializeOwned,
{
    match T::deserialize(deserializer) {
        Ok(value) => Ok(value),
        Err(error) => {
            relay_log::error!(
                error = %error,
                "Error deserializing global config option: {}",
                std::any::type_name::<T>(),
            );
            Ok(T::default())
        }
    }
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
            options: Options {
                unsampled_profiles_enabled: true,
                ..Default::default()
            },
        };

        let serialized =
            serde_json::to_string(&global_config).expect("failed to serialize GlobalConfig");

        let deserialized = serde_json::from_str::<GlobalConfig>(serialized.as_str())
            .expect("failed to deserialize GlobalConfig");

        assert_eq!(deserialized, global_config);
    }

    #[test]
    fn test_global_config_invalid_value_is_default() {
        let options: Options = serde_json::from_str(
            r#"{"relay.cardinality-limiter.mode":"passive","relay.span-usage-metric":123}"#,
        )
        .unwrap();

        let expected = Options {
            cardinality_limiter_mode: CardinalityLimiterMode::Passive,
            ..Default::default()
        };

        assert_eq!(options, expected);
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

    #[test]
    fn test_minimal_serialization() {
        let config = r#"{"options":{"foo":"bar"}}"#;
        let deserialized: GlobalConfig = serde_json::from_str(config).unwrap();
        let serialized = serde_json::to_string(&deserialized).unwrap();
        assert_eq!(config, &serialized);
    }
}
