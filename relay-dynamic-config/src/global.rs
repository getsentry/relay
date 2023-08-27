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
    pub measurements: MeasurementsConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_global_config() {
        let json = r#"{
"measurements":
       {
        "builtinMeasurements": [
            {"name": "app_start_cold", "unit": "millisecond"},
            {"name": "app_start_warm", "unit": "millisecond"},
            {"name": "cls", "unit": "none"},
            {"name": "fcp", "unit": "millisecond"},
            {"name": "fid", "unit": "millisecond"},
            {"name": "fp", "unit": "millisecond"},
            {"name": "frames_frozen_rate", "unit": "ratio"},
            {"name": "frames_frozen", "unit": "none"},
            {"name": "frames_slow_rate", "unit": "ratio"},
            {"name": "frames_slow", "unit": "none"},
            {"name": "frames_total", "unit": "none"},
            {"name": "inp", "unit": "millisecond"},
            {"name": "lcp", "unit": "millisecond"},
            {"name": "stall_count", "unit": "none"},
            {"name": "stall_longest_time", "unit": "millisecond"},
            {"name": "stall_percentage", "unit": "ratio"},
            {"name": "stall_total_time", "unit": "millisecond"},
            {"name": "ttfb.requesttime", "unit": "millisecond"},
            {"name": "ttfb", "unit": "millisecond"},
            {"name": "time_to_full_display", "unit": "millisecond"},
            {"name": "time_to_initial_display", "unit": "millisecond"}
        ],
        "maxCustomMeasurements": 10
       }
    }"#;

        let deserialized: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, GlobalConfig::default());
    }
}
