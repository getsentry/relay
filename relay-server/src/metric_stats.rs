use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use relay_config::Config;
use relay_metrics::{
    Aggregator, Bucket, BucketValue, BucketView, MergeBuckets, MetricResourceIdentifier,
    UnixTimestamp,
};
use relay_quotas::Scoping;
use relay_system::Addr;

use crate::services::outcome::Outcome;

fn volume_metric_mri() -> Arc<str> {
    static VOLUME_METRIC_MRI: OnceLock<Arc<str>> = OnceLock::new();

    Arc::clone(VOLUME_METRIC_MRI.get_or_init(|| "c:metric_stats/volume@none".into()))
}

/// Tracks stats about metrics.
///
/// Metric stats are similar to outcomes for envelopes, they record
/// the final "fate" of a metric and its properties.
///
/// Unlike outcomes metric stats are tracked on a per MRI basis
/// and contain additional metadata, like the cardinality of a metric.
#[derive(Clone, Debug)]
pub struct MetricStats {
    config: Arc<Config>,
    aggregator: Addr<Aggregator>,
}

impl MetricStats {
    /// Creates a new [`MetricStats`] instance.
    pub fn new(config: Arc<Config>, aggregator: Addr<Aggregator>) -> Self {
        Self { config, aggregator }
    }

    /// Tracks the metric volume and outcome for the bucket.
    pub fn track(&self, scoping: Scoping, bucket: &BucketView<'_>, outcome: Outcome) {
        if !self.config.processing_enabled() {
            return;
        }

        let Some(volume) = self.to_volume_metric(bucket, &outcome) else {
            return;
        };

        relay_log::trace!(
            "Tracking volume of {} for mri '{}': {}",
            bucket.metadata().merges.get(),
            bucket.name(),
            outcome
        );
        self.aggregator
            .send(MergeBuckets::new(scoping.project_key, vec![volume]));
    }

    fn to_volume_metric(&self, bucket: &BucketView<'_>, outcome: &Outcome) -> Option<Bucket> {
        let volume = bucket.metadata().merges.get();
        if volume == 0 {
            return None;
        }

        let namespace = MetricResourceIdentifier::parse(bucket.name())
            .ok()?
            .namespace;
        if !namespace.has_metric_stats() {
            return None;
        }

        let mut tags = BTreeMap::from([
            ("mri".to_owned(), bucket.name().to_string()),
            ("mri.namespace".to_owned(), namespace.to_string()),
            (
                "outcome.id".to_owned(),
                outcome.to_outcome_id().as_u8().to_string(),
            ),
        ]);

        if let Some(reason) = outcome.to_reason() {
            tags.insert("outcome.reason".to_owned(), reason.into_owned());
        }

        Some(Bucket {
            timestamp: UnixTimestamp::now(),
            width: 0,
            name: volume_metric_mri(),
            value: BucketValue::Counter(volume.into()),
            tags,
            metadata: Default::default(),
        })
    }
}
