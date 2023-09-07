use std::path::{Path, PathBuf};

use relay_event_normalization::MeasurementsConfig;
use serde::{Deserialize, Serialize};

/// A dynamic configuration for all Relays passed down from Sentry.
///
/// Values shared across all projects may also be included here, to keep
/// [`ProjectConfig`](crate::ProjectConfig)s small.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct GlobalConfig {
    /// Configuration for measurements normalization.
    pub measurements: Option<MeasurementsConfig>,
}

impl GlobalConfig {
    /// Returns the full filepath of the global config.
    fn path(base: &Path) -> PathBuf {
        base.join("global_config.json")
    }

    /// Loads the [`GlobalConfig`] from a file if it's provided.
    pub fn load(base: &Path) -> anyhow::Result<Option<Self>> {
        let path = Self::path(base);

        match path.exists() {
            true => {
                let file_contents = std::fs::read_to_string(path)?;
                let global_config = serde_json::from_str::<GlobalConfig>(file_contents.as_str())?;

                Ok(Some(global_config))
            }
            false => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {

    use relay_base_schema::metrics::MetricUnit;
    use relay_event_normalization::{BuiltinMeasurementKey, MeasurementsConfig};

    use super::GlobalConfig;

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
        };

        let serialized =
            serde_json::to_string(&global_config).expect("failed to serialize GlobalConfig");

        let deserialized = serde_json::from_str::<GlobalConfig>(serialized.as_str())
            .expect("failed to deserialize GlobalConfig");

        assert_eq!(deserialized, global_config);
    }
}
