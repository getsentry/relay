use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use relay_config::Config;
use relay_metrics::{
    Aggregator, Bucket, BucketValue, BucketView, MergeBuckets, MetricResourceIdentifier,
    UnixTimestamp,
};
use relay_quotas::Scoping;
use relay_system::Addr;

use crate::services::global_config::GlobalConfigHandle;
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
    global_config: GlobalConfigHandle,
    aggregator: Addr<Aggregator>,
}

impl MetricStats {
    /// Creates a new [`MetricStats`] instance.
    pub fn new(
        config: Arc<Config>,
        global_config: GlobalConfigHandle,
        aggregator: Addr<Aggregator>,
    ) -> Self {
        Self {
            config,
            global_config,
            aggregator,
        }
    }

    /// Tracks the metric volume and outcome for the bucket.
    pub fn track(&self, scoping: Scoping, bucket: &BucketView<'_>, outcome: Outcome) {
        if !self.config.processing_enabled() || !self.is_rolled_out(scoping.organization_id) {
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

    fn is_rolled_out(&self, organization_id: u64) -> bool {
        let rate = self
            .global_config
            .current()
            .options
            .metric_stats_rollout_rate;

        ((organization_id % 100000) as f32 / 100000.0f32) <= rate
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

#[cfg(test)]
mod tests {
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_dynamic_config::GlobalConfig;
    use relay_quotas::ReasonCode;
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;

    fn create_metric_stats(rollout_rate: f32) -> (MetricStats, UnboundedReceiver<Aggregator>) {
        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let mut global_config = GlobalConfig::default();
        global_config.options.metric_stats_rollout_rate = rollout_rate;
        let global_config = GlobalConfigHandle::fixed(global_config);

        let (addr, receiver) = Addr::custom();
        let ms = MetricStats::new(Arc::new(config), global_config, addr);

        (ms, receiver)
    }

    fn scoping() -> Scoping {
        Scoping {
            organization_id: 42,
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        }
    }

    macro_rules! tags {
        ($(($key:expr, $value:expr),)*) => {
            BTreeMap::from([
                $(($key.to_owned(), $value.to_owned())),*
            ])
        }
    }

    #[test]
    fn test_metric_stats_volume() {
        let (ms, mut receiver) = create_metric_stats(1.0);

        let scoping = scoping();
        let mut bucket = Bucket::parse(b"rt@millisecond:57|d", UnixTimestamp::now()).unwrap();

        ms.track(scoping, &BucketView::from(&bucket), Outcome::Accepted);

        bucket.metadata.merges = bucket.metadata.merges.saturating_add(41);
        ms.track(
            scoping,
            &BucketView::from(&bucket),
            Outcome::RateLimited(Some(ReasonCode::new("foobar"))),
        );

        drop(ms);

        let Aggregator::MergeBuckets(mb) = receiver.blocking_recv().unwrap() else {
            panic!();
        };
        assert_eq!(mb.project_key(), scoping.project_key);

        let mut buckets = mb.buckets();
        assert_eq!(buckets.len(), 1);
        let bucket = buckets.pop().unwrap();

        assert_eq!(&*bucket.name, "c:metric_stats/volume@none");
        assert_eq!(bucket.value, BucketValue::Counter(1.into()));
        assert_eq!(
            bucket.tags,
            tags!(
                ("mri", "d:custom/rt@millisecond"),
                ("mri.namespace", "custom"),
                ("outcome.id", "0"),
            )
        );

        let Aggregator::MergeBuckets(mb) = receiver.blocking_recv().unwrap() else {
            panic!();
        };
        assert_eq!(mb.project_key(), scoping.project_key);

        let mut buckets = mb.buckets();
        assert_eq!(buckets.len(), 1);
        let bucket = buckets.pop().unwrap();

        assert_eq!(&*bucket.name, "c:metric_stats/volume@none");
        assert_eq!(bucket.value, BucketValue::Counter(42.into()));
        assert_eq!(
            bucket.tags,
            tags!(
                ("mri", "d:custom/rt@millisecond"),
                ("mri.namespace", "custom"),
                ("outcome.id", "2"),
                ("outcome.reason", "foobar"),
            )
        );

        assert!(receiver.blocking_recv().is_none());
    }

    #[test]
    fn test_metric_stats_rollout_rate_disabled() {
        let (ms, mut receiver) = create_metric_stats(0.0);

        let scoping = scoping();
        let bucket = Bucket::parse(b"rt@millisecond:57|d", UnixTimestamp::now()).unwrap();
        ms.track(scoping, &BucketView::from(&bucket), Outcome::Accepted);

        drop(ms);

        assert!(receiver.blocking_recv().is_none());
    }

    #[test]
    fn test_metric_stats_disabled_namespace() {
        let (ms, mut receiver) = create_metric_stats(1.0);

        let scoping = scoping();
        let bucket =
            Bucket::parse(b"transactions/rt@millisecond:57|d", UnixTimestamp::now()).unwrap();
        ms.track(scoping, &BucketView::from(&bucket), Outcome::Accepted);

        drop(ms);

        assert!(receiver.blocking_recv().is_none());
    }
}
