use relay_filter::FilterStatKey;
use relay_metrics::{Bucket, MetricNamespace};
use relay_quotas::Scoping;

use crate::metrics::MetricOutcomes;
use crate::services::outcome::Outcome;
use crate::services::processor::BucketSource;

/// Checks if the namespace of the passed bucket is valid.
///
/// This is returns `true` for most namespaces except:
///  - [`MetricNamespace::Unsupported`]: Equal to invalid/unknown namespaces.
///  - [`MetricNamespace::Outcomes`]: Outcomes are only allowed if the `source` is [`BucketSource::Internal`].
pub fn is_valid_namespace(bucket: &Bucket, source: BucketSource) -> bool {
    match bucket.name.namespace() {
        MetricNamespace::Sessions => true,
        MetricNamespace::Spans => true,
        MetricNamespace::Transactions => true,
        MetricNamespace::Outcomes => source == BucketSource::Internal,
        MetricNamespace::Unsupported => false,
    }
}

/// Removes all buckets in disabled or unsupported namespaces.
///
/// Removed buckets are tracked with a [`FilterStatKey::DisabledNamespace`] outcome.
pub fn remove_invalid_namespaces(
    mut buckets: Vec<Bucket>,
    metric_outcomes: &MetricOutcomes,
    scoping: Scoping,
) -> Vec<Bucket> {
    let mut disabled_namespace_buckets = Vec::new();

    buckets = buckets
        .into_iter()
        .filter_map(|bucket| {
            let namespace = bucket.name.namespace();
            if !is_metric_namespace_valid(namespace) {
                relay_log::trace!(
                    mri = &*bucket.name,
                    namespace = %namespace,
                    "dropping metric in disabled namespace"
                );
                disabled_namespace_buckets.push(bucket);
                return None;
            };

            Some(bucket)
        })
        .collect();

    if !disabled_namespace_buckets.is_empty() {
        metric_outcomes.track(
            scoping,
            &disabled_namespace_buckets,
            Outcome::Filtered(FilterStatKey::DisabledNamespace),
        );
    }

    buckets
}

fn is_metric_namespace_valid(namespace: MetricNamespace) -> bool {
    match namespace {
        MetricNamespace::Sessions => true,
        MetricNamespace::Spans => true,
        MetricNamespace::Transactions => true,
        MetricNamespace::Outcomes => true,
        MetricNamespace::Unsupported => false,
    }
}

#[cfg(test)]
mod tests {
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_metrics::{BucketValue, UnixTimestamp};
    use relay_system::Addr;

    use super::*;

    fn create_unsupported_bucket_with_name(name: String) -> Bucket {
        Bucket {
            name: format!("d:unknown/{name}@byte").into(),
            value: BucketValue::Counter(1.into()),
            timestamp: UnixTimestamp::now(),
            tags: Default::default(),
            width: 10,
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_remove_invalid_namespaces() {
        let (outcome_aggregator, _) = Addr::custom();
        let metric_outcomes = MetricOutcomes::new(outcome_aggregator);

        let b1 = create_unsupported_bucket_with_name("cpu_time".into());
        let b2 = create_unsupported_bucket_with_name("memory_usage".into());
        let buckets = vec![b1.clone(), b2.clone()];

        let buckets = remove_invalid_namespaces(
            buckets,
            &metric_outcomes,
            Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        );

        assert!(buckets.is_empty());
    }
}
