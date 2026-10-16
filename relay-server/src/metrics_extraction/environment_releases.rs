//! Environment and release metrics for EAP items.
//!
//! Used by Sentry to create environments and releases for items which never pass through
//! the event pipeline, like logs and trace metrics.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use relay_conventions::attributes::{SENTRY__DIST, SENTRY__ENVIRONMENT, SENTRY__RELEASE};
use relay_event_schema::protocol::Attributes;
use relay_metrics::{Bucket, BucketValue, MetricName, UnixTimestamp};
use relay_protocol::FiniteF64;
use relay_quotas::DataCategory;

use crate::metrics_extraction::ExtractedMetrics;

/// Metric MRI counting items per environment.
const ENVIRONMENT_MRI: &str = "c:environment_releases/environment@none";
/// Metric MRI counting items per release, environment and dist.
const RELEASE_MRI: &str = "c:environment_releases/release@none";

const CATEGORY_TAG: &str = "category";
const ENVIRONMENT_TAG: &str = "environment";
const RELEASE_TAG: &str = "release";
const DIST_TAG: &str = "dist";

/// Distinct combination of environment, release and dist on an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Key<'a> {
    environment: Option<&'a str>,
    release: Option<&'a str>,
    dist: Option<&'a str>,
}

impl<'a> Key<'a> {
    fn from_attributes(attributes: Option<&'a Attributes>) -> Self {
        let get = |key: &str| {
            attributes
                .and_then(|a| a.get_value(key))
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty())
        };

        Self {
            environment: get(SENTRY__ENVIRONMENT),
            release: get(SENTRY__RELEASE),
            dist: get(SENTRY__DIST),
        }
    }
}

/// Extracts environment and release metrics from item attributes.
///
/// Items without an environment are still counted, without the `environment` tag.
/// Items without a release do not emit a release metric.
pub fn extract_metrics<'a, I>(
    attributes: I,
    category: DataCategory,
    timestamp: UnixTimestamp,
) -> ExtractedMetrics
where
    I: IntoIterator<Item = Option<&'a Attributes>>,
{
    static ENVIRONMENT: LazyLock<MetricName> = LazyLock::new(|| ENVIRONMENT_MRI.into());
    static RELEASE: LazyLock<MetricName> = LazyLock::new(|| RELEASE_MRI.into());

    let mut releases = BTreeMap::<Key<'_>, u64>::new();
    for attributes in attributes {
        *releases
            .entry(Key::from_attributes(attributes))
            .or_default() += 1;
    }

    let mut environments = BTreeMap::<Option<&str>, u64>::new();
    for (key, count) in &releases {
        *environments.entry(key.environment).or_default() += count;
    }

    let bucket = |name: &MetricName, count: u64, tags: BTreeMap<String, String>| Bucket {
        name: name.clone(),
        value: BucketValue::Counter(FiniteF64::cast_from_u64(count)),
        timestamp,
        tags,
        width: 0,
        metadata: Default::default(),
    };

    let tags = |pairs: &[(&str, Option<&str>)]| {
        std::iter::once((CATEGORY_TAG, Some(category.name())))
            .chain(pairs.iter().copied())
            .filter_map(|(k, v)| Some((k.to_owned(), v?.to_owned())))
            .collect::<BTreeMap<_, _>>()
    };

    let environment_buckets = environments.into_iter().map(|(environment, count)| {
        bucket(&ENVIRONMENT, count, tags(&[(ENVIRONMENT_TAG, environment)]))
    });

    let release_buckets = releases.into_iter().filter_map(|(key, count)| {
        let tags = tags(&[
            (RELEASE_TAG, Some(key.release?)),
            (ENVIRONMENT_TAG, key.environment),
            (DIST_TAG, key.dist),
        ]);
        Some(bucket(&RELEASE, count, tags))
    });

    ExtractedMetrics(environment_buckets.chain(release_buckets).collect())
}

#[cfg(test)]
mod tests {
    use relay_metrics::MetricNamespace;

    use super::*;

    fn attributes(pairs: &[(&str, &str)]) -> Attributes {
        let mut attributes = Attributes::new();
        for (key, value) in pairs {
            attributes.insert(*key, value.to_string());
        }
        attributes
    }

    #[test]
    fn test_metric_mri_valid() {
        for mri in [ENVIRONMENT_MRI, RELEASE_MRI] {
            let name = MetricName::from(mri);
            assert_eq!(
                name.try_namespace(),
                Some(MetricNamespace::EnvironmentReleases)
            );
        }
    }

    #[test]
    fn test_extract_metrics() {
        let items = [
            attributes(&[(SENTRY__ENVIRONMENT, "prod"), (SENTRY__RELEASE, "1.0")]),
            attributes(&[(SENTRY__ENVIRONMENT, "prod"), (SENTRY__RELEASE, "1.0")]),
            attributes(&[(SENTRY__RELEASE, "1.0"), (SENTRY__DIST, "42")]),
            attributes(&[(SENTRY__ENVIRONMENT, "dev")]),
            attributes(&[(SENTRY__ENVIRONMENT, ""), (SENTRY__RELEASE, "")]),
        ];

        let ExtractedMetrics(buckets) = extract_metrics(
            items.iter().map(Some).chain([None]),
            DataCategory::LogItem,
            UnixTimestamp::from_secs(1000),
        );

        let buckets = buckets
            .iter()
            .map(|b| {
                let tags = b.tags.iter().map(|(k, v)| format!("{k}={v}"));
                format!(
                    "{} {:?} {}",
                    b.name,
                    b.value,
                    tags.collect::<Vec<_>>().join(",")
                )
            })
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!(buckets, @r#"
        [
            "c:environment_releases/environment@none Counter(3.0) category=log_item",
            "c:environment_releases/environment@none Counter(1.0) category=log_item,environment=dev",
            "c:environment_releases/environment@none Counter(2.0) category=log_item,environment=prod",
            "c:environment_releases/release@none Counter(1.0) category=log_item,dist=42,release=1.0",
            "c:environment_releases/release@none Counter(2.0) category=log_item,environment=prod,release=1.0",
        ]
        "#);
    }
}
