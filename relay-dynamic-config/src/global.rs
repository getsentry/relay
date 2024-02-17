use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::iter::Chain;
use std::path::Path;
use std::slice::Iter;

use relay_event_normalization::MeasurementsConfig;
use relay_quotas::Quota;
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
    /// Quotas that apply to all projects.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quotas: Vec<Quota>,
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

/// Returns `true` if this value is equal to `Default::default()`.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

/// Container for global and project level [`Quota`].
#[derive(Copy, Clone)]
pub struct DynamicQuotas<'a> {
    global_quotas: &'a [Quota],
    project_quotas: &'a [Quota],
}

impl<'a> DynamicQuotas<'a> {
    /// Returns a new [`DynamicQuotas`]
    pub fn new(global_config: &'a GlobalConfig, quotas: &'a [Quota]) -> Self {
        Self {
            global_quotas: &global_config.quotas,
            project_quotas: quotas,
        }
    }

    /// Returns `true` if both global quotas and project quotas are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of both global and project quotas.
    pub fn len(&self) -> usize {
        self.global_quotas.len() + self.project_quotas.len()
    }
}

// Implementing IntoIterator for &DynamicQuotas to allow iterating over the quotas.
impl<'a> IntoIterator for DynamicQuotas<'a> {
    type Item = &'a Quota;
    type IntoIter = Chain<Iter<'a, Quota>, Iter<'a, Quota>>;

    fn into_iter(self) -> Self::IntoIter {
        self.global_quotas.iter().chain(self.project_quotas.iter())
    }
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
    use relay_quotas::{DataCategory, QuotaScope};

    use super::*;

    fn mock_quota(id: &str) -> Quota {
        Quota {
            id: Some(id.into()),
            categories: smallvec::smallvec![DataCategory::MetricBucket],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
        }
    }

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
            quotas: vec![mock_quota("foo"), mock_quota("bar")],
        };

        let serialized =
            serde_json::to_string(&global_config).expect("failed to serialize GlobalConfig");

        let deserialized = serde_json::from_str::<GlobalConfig>(serialized.as_str())
            .expect("failed to deserialize GlobalConfig");

        assert_eq!(deserialized, global_config);
    }

    #[test]
    fn test_dynamic_quotas() {
        let global_config = GlobalConfig {
            measurements: None,
            quotas: vec![mock_quota("foo"), mock_quota("bar")],
            options: Options::default(),
        };

        let project_quotas = vec![mock_quota("baz"), mock_quota("qux")];

        let dynamic_quotas = DynamicQuotas::new(&global_config, &project_quotas);

        for (expected_id, quota) in ["foo", "bar", "baz", "qux"].iter().zip(dynamic_quotas) {
            assert_eq!(Some(expected_id.to_string()), quota.id);
        }
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
