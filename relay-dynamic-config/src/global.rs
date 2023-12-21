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
}

/// All options passed down from Sentry to Relay.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Options {
    // Example:
    // ```rs
    // #[serde(default, rename = "relay.some-option.name")]
    // pub some_option: Vec<u32>,
    // ```
    /// org IDs for which we'll allow using profiles dropped due to DS for function metrics.
    /// This is only intended to be be used initially to limit the feature to sentry org.
    /// Once we start to gradually rollout to other orgs this option can be deprecated
    #[serde(
        default,
        rename = "profiling.profile_metrics.unsampled_profiles.allowed_org_ids"
    )]
    pub profile_metrics_allowed_org_ids: Vec<u32>,

    /// org IDs for which we want to avoid using the unsampled profiles for function metrics.
    /// This will let us selectively disable the behaviour for entire orgs that may have an
    /// extremely high volume increase
    #[serde(
        default,
        rename = "profiling.profile_metrics.unsampled_profiles.excluded_org_ids"
    )]
    pub profile_metrics_excluded_orgs_ids: Vec<u32>,

    /// project IDs for which we want to avoid using the unsampled profiles for function metrics.
    /// This will let us selectively disable the behaviour for project that may have an extremely
    /// high volume increase
    #[serde(
        default,
        rename = "profiling.profile_metrics.unsampled_profiles.excluded_project_ids"
    )]
    pub profile_metrics_excluded_project_ids: Vec<u32>,

    /// list of platform names for which we allow using unsampled profiles for the purpose
    /// of improving profile (function) metrics
    #[serde(
        default,
        rename = "profiling.profile_metrics.unsampled_profiles.platforms"
    )]
    pub profile_metrics_allowed_platforms: Vec<String>,

    /// sample rate for tuning the amount of unsampled profiles that we "let through"
    #[serde(
        default,
        rename = "profiling.profile_metrics.unsampled_profiles.sample_rate"
    )]
    pub profile_metrics_sample_rate: f32,

    /// All other unknown options.
    #[serde(flatten)]
    other: HashMap<String, Value>,
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
            }),
        };

        let serialized =
            serde_json::to_string(&global_config).expect("failed to serialize GlobalConfig");

        let deserialized = serde_json::from_str::<GlobalConfig>(serialized.as_str())
            .expect("failed to deserialize GlobalConfig");

        assert_eq!(deserialized, global_config);
    }
}
