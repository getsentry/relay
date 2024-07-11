use relay_dynamic_config::{ErrorBoundary, Feature, Metrics};
use relay_filter::FilterStatKey;
use relay_metrics::{Bucket, MetricNamespace};
use relay_quotas::Scoping;

use crate::metrics::MetricOutcomes;
use crate::services::outcome::Outcome;
use crate::services::project::ProjectState;
use crate::services::project_cache::BucketSource;

pub fn filter_namespaces(mut buckets: Vec<Bucket>, source: BucketSource) -> Vec<Bucket> {
    buckets.retain(|bucket| match bucket.name.namespace() {
        MetricNamespace::Sessions => true,
        MetricNamespace::Transactions => true,
        MetricNamespace::Spans => true,
        MetricNamespace::Profiles => true,
        MetricNamespace::Custom => true,
        MetricNamespace::Stats => source == BucketSource::Internal,
        MetricNamespace::Unsupported => false,
    });

    buckets
}

pub fn apply_project_state(
    mut buckets: Vec<Bucket>,
    metric_outcomes: &MetricOutcomes,
    project_state: &ProjectState,
    scoping: Scoping,
) -> Vec<Bucket> {
    let mut denied_buckets = Vec::new();
    let mut disabled_namespace_buckets = Vec::new();

    buckets = buckets
        .into_iter()
        .filter_map(|mut bucket| {
            if !is_metric_namespace_valid(project_state, bucket.name.namespace()) {
                relay_log::trace!(mri = &*bucket.name, "dropping metric in disabled namespace");
                disabled_namespace_buckets.push(bucket);
                return None;
            };

            if let ErrorBoundary::Ok(ref metric_config) = project_state.config.metrics {
                if metric_config.denied_names.is_match(&*bucket.name) {
                    relay_log::trace!(mri = &*bucket.name, "dropping metrics due to block list");
                    denied_buckets.push(bucket);
                    return None;
                }

                remove_matching_bucket_tags(metric_config, &mut bucket);
            }

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

    if !denied_buckets.is_empty() {
        metric_outcomes.track(
            scoping,
            &denied_buckets,
            Outcome::Filtered(FilterStatKey::DeniedName),
        );
    }

    buckets
}

fn is_metric_namespace_valid(state: &ProjectState, namespace: MetricNamespace) -> bool {
    match namespace {
        MetricNamespace::Sessions => true,
        MetricNamespace::Transactions => true,
        MetricNamespace::Spans => state.config.features.produces_spans(),
        MetricNamespace::Profiles => true,
        MetricNamespace::Custom => state.has_feature(Feature::CustomMetrics),
        MetricNamespace::Stats => true,
        MetricNamespace::Unsupported => false,
    }
}

/// Removes tags based on user configured deny list.
fn remove_matching_bucket_tags(metric_config: &Metrics, bucket: &mut Bucket) {
    for tag_block in &metric_config.denied_tags {
        if tag_block.name.is_match(&*bucket.name) {
            bucket
                .tags
                .retain(|tag_key, _| !tag_block.tags.is_match(tag_key));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_common::glob3::GlobPatterns;
    use relay_dynamic_config::TagBlock;
    use relay_metrics::{BucketValue, UnixTimestamp};
    use relay_system::Addr;

    use crate::metrics::MetricStats;
    use crate::services::metrics::Aggregator;

    use super::*;

    fn get_test_bucket(name: &str, tags: BTreeMap<String, String>) -> Bucket {
        let json = serde_json::json!({
            "timestamp": 1615889440,
            "width": 10,
            "name": name,
            "type": "c",
            "value": 4.0,
            "tags": tags,
        });

        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn test_remove_tags() {
        let mut tags = BTreeMap::default();
        tags.insert("foobazbar".to_string(), "val".to_string());
        tags.insert("foobaz".to_string(), "val".to_string());
        tags.insert("bazbar".to_string(), "val".to_string());

        let mut bucket = get_test_bucket("foobar", tags);

        let tag_block_pattern = "foobaz*";

        let metric_config = Metrics {
            denied_tags: vec![TagBlock {
                name: GlobPatterns::new(vec!["foobar".to_string()]),
                tags: GlobPatterns::new(vec![tag_block_pattern.to_string()]),
            }],
            ..Default::default()
        };

        remove_matching_bucket_tags(&metric_config, &mut bucket);

        // the tag_block_pattern should match on two of the tags.
        assert_eq!(bucket.tags.len(), 1);
    }

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
    fn test_apply_project_state() {
        let (outcome_aggregator, _) = Addr::custom();
        let (metric_stats, mut metric_stats_rx) = MetricStats::test();
        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator);

        let project_state = {
            let mut project_state = ProjectState::allowed();
            project_state.config = serde_json::from_value(serde_json::json!({
                "metrics": { "deniedNames": ["*cpu_time*"] },
                "features": ["organizations:custom-metrics"]
            }))
            .unwrap();
            project_state
        };

        let b1 = create_custom_bucket_with_name("cpu_time".into());
        let b2 = create_custom_bucket_with_name("memory_usage".into());
        let buckets = vec![b1.clone(), b2.clone()];

        let buckets = apply_project_state(
            buckets,
            &metric_outcomes,
            &project_state,
            Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        );

        // We assert that one metric passes all the pre-processing.
        assert_eq!(&*buckets, &[b2]);

        // We assert that one metric is emitted by metric stats.
        let value = metric_stats_rx.blocking_recv().unwrap();
        let Aggregator::MergeBuckets(merge_buckets) = value else {
            panic!();
        };
        assert_eq!(merge_buckets.buckets.len(), 1);
        assert_eq!(
            merge_buckets.buckets[0].tags.get("mri").unwrap().as_str(),
            &*b1.name
        );
    }

    #[test]
    fn test_apply_project_state_with_disabled_custom_namespace() {
        let (outcome_aggregator, _) = Addr::custom();
        let (metric_stats, mut metric_stats_rx) = MetricStats::test();
        let metric_outcomes = MetricOutcomes::new(metric_stats, outcome_aggregator);

        let b1 = create_custom_bucket_with_name("cpu_time".into());
        let b2 = create_custom_bucket_with_name("memory_usage".into());
        let buckets = vec![b1.clone(), b2.clone()];

        let buckets = apply_project_state(
            buckets,
            &metric_outcomes,
            &ProjectState::allowed(),
            Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        );

        assert!(buckets.is_empty());

        // We assert that two metrics are emitted by metric stats.
        for _ in 0..2 {
            let value = metric_stats_rx.blocking_recv().unwrap();
            let Aggregator::MergeBuckets(merge_buckets) = value else {
                panic!();
            };
            assert_eq!(merge_buckets.buckets.len(), 1);
            let BucketValue::Counter(value) = merge_buckets.buckets[0].value else {
                panic!();
            };
            assert_eq!(value.to_f64(), 1.0);
        }
    }
}
