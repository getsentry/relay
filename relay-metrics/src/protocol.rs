use std::hash::Hasher as _;

use hash32::{FnvHasher, Hasher as _};
use relay_protocol::FiniteF64;

#[doc(inline)]
pub use relay_base_schema::metrics::{
    CustomUnit, DurationUnit, FractionUnit, InformationUnit, MetricName, MetricNamespace,
    MetricResourceIdentifier, MetricType, MetricUnit, ParseMetricError, ParseMetricUnitError,
};
#[doc(inline)]
pub use relay_common::time::UnixTimestamp;

use crate::{Bucket, MetricTags};

/// Type used for Counter metric
pub type CounterType = FiniteF64;

/// Type of distribution entries
pub type DistributionType = FiniteF64;

/// Type used for set elements in Set metric
pub type SetType = u32;

/// Type used for Gauge entries
pub type GaugeType = FiniteF64;

/// Error returned from [`normalize_bucket`].
#[derive(Debug, thiserror::Error)]
pub enum NormalizationError {
    /// The metric name includes an invalid or unsupported metric namespace.
    #[error("unsupported metric namespace")]
    UnsupportedNamespace,
    /// The metric name cannot be parsed and is invalid.
    #[error("invalid metric name: {0:?}")]
    InvalidMetricName(MetricName),
}

/// Normalizes a bucket.
///
/// The passed metric will have its name and tags normalized and tested for validity.
/// Invalid characters in the metric name may be replaced,
/// see [`relay_base_schema::metrics::try_normalize_metric_name`].
///
/// Invalid tags are removed and tag keys are normalized, for example control characters are
/// removed from tag keys.
pub fn normalize_bucket(bucket: &mut Bucket) -> Result<(), NormalizationError> {
    normalize_metric_name(&mut bucket.name)?;
    normalize_metric_tags(&mut bucket.tags);
    Ok(())
}

/// Normalizes a metric name.
///
/// Normalization includes expanding valid metric names without a namespace to the default
/// namespace.
///
/// Invalid metric names are rejected with [`NormalizationError`].
fn normalize_metric_name(name: &mut MetricName) -> Result<(), NormalizationError> {
    *name = match MetricResourceIdentifier::parse(name) {
        Ok(mri) => {
            if matches!(mri.namespace, MetricNamespace::Unsupported) {
                return Err(NormalizationError::UnsupportedNamespace);
            }

            // We can improve this code part, by not always re-creating the name, if the name is
            // already a valid MRI with namespace we can use the original name instead.
            mri.to_string().into()
        }
        Err(_) => {
            return Err(NormalizationError::InvalidMetricName(name.clone()));
        }
    };

    Ok(())
}

/// Removes tags with invalid characters in the key, and validates tag values.
///
/// Tag values are validated with [`normalize_tag_value`].
fn normalize_metric_tags(tags: &mut MetricTags) {
    tags.retain(|tag_key, tag_value| {
        if !is_valid_tag_key(tag_key) {
            relay_log::debug!("invalid metric tag key {tag_key:?}");
            return false;
        }

        normalize_tag_value(tag_value);

        true
    });
}

/// Validates a tag key.
///
/// Tag keys currently only need to not contain ASCII control characters. This might change.
pub(crate) fn is_valid_tag_key(tag_key: &str) -> bool {
    // iterating over bytes produces better asm, and we're only checking for ascii chars
    for &byte in tag_key.as_bytes() {
        if (byte as char).is_ascii_control() {
            return false;
        }
    }
    true
}

/// Normalizes a tag value.
///
/// Tag values are never entirely rejected, but invalid characters (control characters) are stripped
/// out.
pub(crate) fn normalize_tag_value(tag_value: &mut String) {
    tag_value.retain(|c| !c.is_control());
}

/// Hashes the given set value.
///
/// Converts a string to the 32-bit FNV hash stored in a set bucket.
pub(crate) fn hash_set_value(string: &str) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(string.as_bytes());
    hasher.finish32()
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;

    use crate::BucketValue;

    use super::*;

    #[test]
    fn test_normalize_invalid_name() {
        let mut bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(5000),
            width: 0,
            name: "c:transactions/\0hergus.bergus@none".into(),
            value: BucketValue::Counter(0.into()),
            tags: Default::default(),
            metadata: Default::default(),
        };

        assert!(matches!(
            normalize_bucket(&mut bucket),
            Err(NormalizationError::InvalidMetricName(_))
        ));
    }

    #[test]
    fn test_normalize_invalid_namespace() {
        let mut bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(5000),
            width: 0,
            name: "c:lol/hergus.bergus@none".into(),
            value: BucketValue::Counter(0.into()),
            tags: Default::default(),
            metadata: Default::default(),
        };

        assert!(matches!(
            normalize_bucket(&mut bucket),
            Err(NormalizationError::UnsupportedNamespace)
        ));
    }

    #[test]
    fn test_normalize_name() {
        let mut bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(5000),
            width: 0,
            name: "c:spans/hergus\0\0bergus".into(),
            value: BucketValue::Counter(0.into()),
            tags: Default::default(),
            metadata: Default::default(),
        };

        normalize_bucket(&mut bucket).unwrap();

        assert_eq!(&bucket.name, "c:spans/hergus_bergus@none");
    }

    #[test]
    fn test_normalize_tag_key_chars() {
        let mut bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(5000),
            width: 0,
            name: "c:spans/hergus.bergus".into(),
            value: BucketValue::Counter(0.into()),
            tags: {
                let mut tags = MetricTags::new();
                // There are some SDKs which mess up content encodings, and interpret the raw bytes
                // of an UTF-16 string as UTF-8. Leading to ASCII
                // strings getting null-bytes interleaved.
                //
                // Somehow those values end up as release tag in sessions, while in error events we
                // haven't observed this malformed encoding. We believe it's slightly better to
                // strip out NUL-bytes instead of dropping the tag such that those values line up
                // again across sessions and events. Should that cause too high cardinality we'll
                // have to drop tags.
                //
                // Note that releases are validated separately against much stricter character set,
                // but the above idea should still apply to other tags.
                tags.insert(
                    "is_it_garbage".to_owned(),
                    "a\0b\0s\0o\0l\0u\0t\0e\0l\0y".to_owned(),
                );
                tags.insert("another\0garbage".to_owned(), "bye".to_owned());
                tags
            },
            metadata: Default::default(),
        };

        normalize_bucket(&mut bucket).unwrap();

        assert_json_snapshot!(bucket, @r###"
        {
          "timestamp": 5000,
          "width": 0,
          "name": "c:spans/hergus.bergus@none",
          "type": "c",
          "value": 0.0,
          "tags": {
            "is_it_garbage": "absolutely"
          }
        }
        "###);
    }
}
