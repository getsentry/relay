use relay_dynamic_config::Feature;
use relay_filter::FilterStatKey;
use relay_metrics::{Bucket, MetricNamespace};
use relay_quotas::Scoping;

use crate::metrics::MetricOutcomes;
use crate::services::outcome::Outcome;
use crate::services::projects::project::ProjectInfo;

/// Checks if the namespace of the passed bucket is valid.
///
/// This is returns `true` for most namespaces except:
///  - [`MetricNamespace::Unsupported`]: Equal to invalid/unknown namespaces.
pub fn is_valid_namespace(bucket: &Bucket) -> bool {
    match bucket.name.namespace() {
        MetricNamespace::Sessions => true,
        MetricNamespace::Transactions => true,
        MetricNamespace::Spans => true,
        MetricNamespace::Custom => true,
        MetricNamespace::Unsupported => false,
    }
}

pub fn apply_project_info(
    mut buckets: Vec<Bucket>,
    metric_outcomes: &MetricOutcomes,
    project_info: &ProjectInfo,
    scoping: Scoping,
) -> Vec<Bucket> {
    let mut disabled_namespace_buckets = Vec::new();

    buckets = buckets
        .into_iter()
        .filter_map(|bucket| {
            if !is_metric_namespace_valid(project_info, bucket.name.namespace()) {
                relay_log::trace!(mri = &*bucket.name, "dropping metric in disabled namespace");
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

fn is_metric_namespace_valid(state: &ProjectInfo, namespace: MetricNamespace) -> bool {
    match namespace {
        MetricNamespace::Sessions => true,
        MetricNamespace::Transactions => true,
        MetricNamespace::Spans => true,
        MetricNamespace::Custom => state.has_feature(Feature::CustomMetrics),
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

    fn create_custom_bucket_with_name(name: String) -> Bucket {
        Bucket {
            name: format!("d:custom/{name}@byte").into(),
            value: BucketValue::Counter(1.into()),
            timestamp: UnixTimestamp::now(),
            tags: Default::default(),
            width: 10,
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_apply_project_info_with_disabled_custom_namespace() {
        let (outcome_aggregator, _) = Addr::custom();
        let metric_outcomes = MetricOutcomes::new(outcome_aggregator);

        let b1 = create_custom_bucket_with_name("cpu_time".into());
        let b2 = create_custom_bucket_with_name("memory_usage".into());
        let buckets = vec![b1.clone(), b2.clone()];

        let buckets = apply_project_info(
            buckets,
            &metric_outcomes,
            &ProjectInfo::default(),
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
