use std::collections::HashMap;
use std::collections::btree_map::Entry;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use relay_base_schema::metrics::MetricNamespace;
use relay_event_normalization::{MeasurementsConfig, ModelCosts, SpanOpDefaults};
use relay_filter::GenericFiltersConfig;
use relay_quotas::Quota;
use serde::{Deserialize, Serialize, de};
use serde_json::Value;

use crate::{ErrorBoundary, MetricExtractionGroup, MetricExtractionGroups, defaults};

/// A dynamic configuration for all Relays passed down from Sentry.
///
/// Values shared across all projects may also be included here, to keep
/// [`ProjectConfig`](crate::ProjectConfig)s small.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GlobalConfig {
    /// Configuration for measurements normalization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<MeasurementsConfig>,
    /// Quotas that apply to all projects.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quotas: Vec<Quota>,
    /// Configuration for global inbound filters.
    ///
    /// These filters are merged with generic filters in project configs before
    /// applying.
    #[serde(skip_serializing_if = "is_err_or_empty")]
    pub filters: ErrorBoundary<GenericFiltersConfig>,
    /// Sentry options passed down to Relay.
    #[serde(
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub options: Options,

    /// Configuration for global metrics extraction rules.
    ///
    /// These are merged with rules in project configs before
    /// applying.
    #[serde(skip_serializing_if = "is_ok_and_empty")]
    pub metric_extraction: ErrorBoundary<MetricExtractionGroups>,

    /// Configuration for AI span measurements.
    #[serde(skip_serializing_if = "is_model_costs_empty")]
    pub ai_model_costs: ErrorBoundary<ModelCosts>,

    /// Configuration to derive the `span.op` from other span fields.
    #[serde(
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub span_op_defaults: SpanOpDefaults,
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

    /// Returns the generic inbound filters.
    pub fn filters(&self) -> Option<&GenericFiltersConfig> {
        match &self.filters {
            ErrorBoundary::Err(_) => None,
            ErrorBoundary::Ok(f) => Some(f),
        }
    }

    /// Modifies the global config after deserialization.
    ///
    /// - Adds hard-coded groups to metrics extraction configs.
    pub fn normalize(&mut self) {
        if let ErrorBoundary::Ok(config) = &mut self.metric_extraction {
            for (group_name, metrics, tags) in defaults::hardcoded_span_metrics() {
                // We only define these groups if they haven't been defined by the upstream yet.
                // This ensures that the innermost Relay always defines the metrics.
                if let Entry::Vacant(entry) = config.groups.entry(group_name) {
                    entry.insert(MetricExtractionGroup {
                        is_enabled: false, // must be enabled via project config
                        metrics,
                        tags,
                    });
                }
            }
        }
    }
}

fn is_err_or_empty(filters_config: &ErrorBoundary<GenericFiltersConfig>) -> bool {
    match filters_config {
        ErrorBoundary::Err(_) => true,
        ErrorBoundary::Ok(config) => config.version == 0 && config.filters.is_empty(),
    }
}

/// All options passed down from Sentry to Relay.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Options {
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

    /// Metric bucket encoding configuration for sets by metric namespace.
    #[serde(
        rename = "relay.metric-bucket-set-encodings",
        deserialize_with = "de_metric_bucket_encodings",
        skip_serializing_if = "is_default"
    )]
    pub metric_bucket_set_encodings: BucketEncodings,
    /// Metric bucket encoding configuration for distributions by metric namespace.
    #[serde(
        rename = "relay.metric-bucket-distribution-encodings",
        deserialize_with = "de_metric_bucket_encodings",
        skip_serializing_if = "is_default"
    )]
    pub metric_bucket_dist_encodings: BucketEncodings,

    /// Rollout rate for metric stats.
    ///
    /// Rate needs to be between `0.0` and `1.0`.
    /// If set to `1.0` all organizations will have metric stats enabled.
    #[serde(
        rename = "relay.metric-stats.rollout-rate",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub metric_stats_rollout_rate: f32,

    /// Overall sampling of span extraction.
    ///
    /// This number represents the fraction of transactions for which
    /// spans are extracted.
    ///
    /// `None` is the default and interpreted as a value of 1.0 (extract everything).
    ///
    /// Note: Any value below 1.0 will cause the product to break, so use with caution.
    #[serde(
        rename = "relay.span-extraction.sample-rate",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub span_extraction_sample_rate: Option<f32>,

    /// Sample rate at which to ingest logs.
    ///
    /// This number represents the fraction of received logs that are processed. It only applies if
    /// [`crate::Feature::OurLogsIngestion`] is enabled.
    ///
    /// `None` is the default and interpreted as a value of 1.0 (ingest everything).
    ///
    /// Note: Any value below 1.0 will cause the product to not show all the users data, so use with caution.
    #[serde(
        rename = "relay.ourlogs-ingestion.sample-rate",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub ourlogs_ingestion_sample_rate: Option<f32>,

    /// List of values on span description that are allowed to be sent to Sentry without being scrubbed.
    ///
    /// At this point, it doesn't accept IP addresses in CIDR format.. yet.
    #[serde(
        rename = "relay.span-normalization.allowed_hosts",
        deserialize_with = "default_on_error",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub http_span_allowed_hosts: Vec<String>,

    /// Whether or not relay should drop attachments submitted with transactions.
    #[serde(
        rename = "relay.drop-transaction-attachments",
        deserialize_with = "default_on_error",
        skip_serializing_if = "is_default"
    )]
    pub drop_transaction_attachments: bool,

    /// All other unknown options.
    #[serde(flatten)]
    other: HashMap<String, Value>,
}

/// Kill switch for controlling the cardinality limiter.
#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
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

/// Configuration container to control [`BucketEncoding`] per namespace.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct BucketEncodings {
    transactions: BucketEncoding,
    spans: BucketEncoding,
    profiles: BucketEncoding,
    custom: BucketEncoding,
    metric_stats: BucketEncoding,
}

impl BucketEncodings {
    /// Returns the configured encoding for a specific namespace.
    pub fn for_namespace(&self, namespace: MetricNamespace) -> BucketEncoding {
        match namespace {
            MetricNamespace::Transactions => self.transactions,
            MetricNamespace::Spans => self.spans,
            MetricNamespace::Custom => self.custom,
            MetricNamespace::Stats => self.metric_stats,
            // Always force the legacy encoding for sessions,
            // sessions are not part of the generic metrics platform with different
            // consumer which are not (yet) updated to support the new data.
            MetricNamespace::Sessions => BucketEncoding::Legacy,
            _ => BucketEncoding::Legacy,
        }
    }
}

/// Deserializes individual metric encodings or all from a string.
///
/// Returns a default when failing to deserialize.
fn de_metric_bucket_encodings<'de, D>(deserializer: D) -> Result<BucketEncodings, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    struct Visitor;

    impl<'de> de::Visitor<'de> for Visitor {
        type Value = BucketEncodings;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("metric bucket encodings")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let encoding = BucketEncoding::deserialize(de::value::StrDeserializer::new(v))?;
            Ok(BucketEncodings {
                transactions: encoding,
                spans: encoding,
                profiles: encoding,
                custom: encoding,
                metric_stats: encoding,
            })
        }

        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            BucketEncodings::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    match deserializer.deserialize_any(Visitor) {
        Ok(value) => Ok(value),
        Err(error) => {
            relay_log::error!(
                error = %error,
                "Error deserializing metric bucket encodings",
            );
            Ok(BucketEncodings::default())
        }
    }
}

/// All supported metric bucket encodings.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BucketEncoding {
    /// The default legacy encoding.
    ///
    /// A simple JSON array of numbers.
    #[default]
    Legacy,
    /// The array encoding.
    ///
    /// Uses already the dynamic value format but still encodes
    /// all values as a JSON number array.
    Array,
    /// Base64 encoding.
    ///
    /// Encodes all values as Base64.
    Base64,
    /// Zstd.
    ///
    /// Compresses all values with zstd.
    Zstd,
}

/// Returns `true` if this value is equal to `Default::default()`.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    t == &T::default()
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

fn is_ok_and_empty(value: &ErrorBoundary<MetricExtractionGroups>) -> bool {
    matches!(
        value,
        &ErrorBoundary::Ok(MetricExtractionGroups { ref groups }) if groups.is_empty()
    )
}

fn is_model_costs_empty(value: &ErrorBoundary<ModelCosts>) -> bool {
    matches!(value, ErrorBoundary::Ok(model_costs) if model_costs.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_config_roundtrip() {
        let json = r#"{
  "measurements": {
    "builtinMeasurements": [
      {
        "name": "foo",
        "unit": "none"
      },
      {
        "name": "bar",
        "unit": "none"
      },
      {
        "name": "baz",
        "unit": "none"
      }
    ],
    "maxCustomMeasurements": 5
  },
  "quotas": [
    {
      "id": "foo",
      "categories": [
        "metric_bucket"
      ],
      "scope": "organization",
      "limit": 0,
      "namespace": null
    },
    {
      "id": "bar",
      "categories": [
        "metric_bucket"
      ],
      "scope": "organization",
      "limit": 0,
      "namespace": null
    }
  ],
  "filters": {
    "version": 1,
    "filters": [
      {
        "id": "myError",
        "isEnabled": true,
        "condition": {
          "op": "eq",
          "name": "event.exceptions",
          "value": "myError"
        }
      }
    ]
  }
}"#;

        let deserialized = serde_json::from_str::<GlobalConfig>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&deserialized).unwrap();
        assert_eq!(json, serialized.as_str());
    }

    #[test]
    fn test_global_config_invalid_value_is_default() {
        let options: Options = serde_json::from_str(
            r#"{
                "relay.cardinality-limiter.mode": "passive"
            }"#,
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

    #[test]
    fn test_metric_bucket_encodings_de_from_str() {
        let o: Options = serde_json::from_str(
            r#"{
                "relay.metric-bucket-set-encodings": "legacy",
                "relay.metric-bucket-distribution-encodings": "zstd"
        }"#,
        )
        .unwrap();

        assert_eq!(
            o.metric_bucket_set_encodings,
            BucketEncodings {
                transactions: BucketEncoding::Legacy,
                spans: BucketEncoding::Legacy,
                profiles: BucketEncoding::Legacy,
                custom: BucketEncoding::Legacy,
                metric_stats: BucketEncoding::Legacy,
            }
        );
        assert_eq!(
            o.metric_bucket_dist_encodings,
            BucketEncodings {
                transactions: BucketEncoding::Zstd,
                spans: BucketEncoding::Zstd,
                profiles: BucketEncoding::Zstd,
                custom: BucketEncoding::Zstd,
                metric_stats: BucketEncoding::Zstd,
            }
        );
    }

    #[test]
    fn test_metric_bucket_encodings_de_from_obj() {
        let original = BucketEncodings {
            transactions: BucketEncoding::Base64,
            spans: BucketEncoding::Zstd,
            profiles: BucketEncoding::Base64,
            custom: BucketEncoding::Zstd,
            metric_stats: BucketEncoding::Base64,
        };
        let s = serde_json::to_string(&original).unwrap();
        let s = format!(
            r#"{{
            "relay.metric-bucket-set-encodings": {s},
            "relay.metric-bucket-distribution-encodings": {s}
        }}"#
        );

        let o: Options = serde_json::from_str(&s).unwrap();
        assert_eq!(o.metric_bucket_set_encodings, original);
        assert_eq!(o.metric_bucket_dist_encodings, original);
    }
}
