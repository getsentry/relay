use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use relay_cardinality::{CardinalityLimit, CardinalityReport};
use relay_config::Config;
use relay_metrics::{
    Aggregator, Bucket, BucketValue, GaugeValue, MergeBuckets, MetricName, UnixTimestamp,
};
use relay_quotas::Scoping;
use relay_system::Addr;

use crate::services::global_config::GlobalConfigHandle;
use crate::services::outcome::Outcome;
use crate::utils::is_rolled_out;

fn volume_metric_mri() -> MetricName {
    static VOLUME_METRIC_MRI: OnceLock<MetricName> = OnceLock::new();

    VOLUME_METRIC_MRI
        .get_or_init(|| "c:metric_stats/volume@none".into())
        .clone()
}

fn cardinality_metric_mri() -> MetricName {
    static CARDINALITY_METRIC_MRI: OnceLock<MetricName> = OnceLock::new();

    CARDINALITY_METRIC_MRI
        .get_or_init(|| "g:metric_stats/cardinality@none".into())
        .clone()
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
    pub fn track_metric(&self, scoping: Scoping, bucket: Bucket, outcome: Outcome) {
        if !self.is_enabled(scoping) {
            return;
        }

        let Some(volume) = self.to_volume_metric(&bucket, &outcome) else {
            return;
        };

        relay_log::trace!(
            "Tracking volume of {} for mri '{}': {}",
            bucket.metadata.merges.get(),
            bucket.name,
            outcome
        );
        self.aggregator
            .send(MergeBuckets::new(scoping.project_key, vec![volume]));
    }

    /// Tracks the cardinality of a metric.
    pub fn track_cardinality(
        &self,
        scoping: Scoping,
        limit: &CardinalityLimit,
        report: &CardinalityReport,
    ) {
        if !self.is_enabled(scoping) {
            return;
        }

        let Some(cardinality) = self.to_cardinality_metric(limit, report) else {
            return;
        };

        relay_log::trace!(
            "Tracking cardinality '{}' for mri '{}': {}",
            limit.id,
            report.metric_name.as_deref().unwrap_or("-"),
            report.cardinality,
        );
        self.aggregator
            .send(MergeBuckets::new(scoping.project_key, vec![cardinality]));
    }

    fn is_enabled(&self, scoping: Scoping) -> bool {
        self.config.processing_enabled() && self.is_rolled_out(scoping.organization_id)
    }

    fn is_rolled_out(&self, organization_id: u64) -> bool {
        let rate = self
            .global_config
            .current()
            .options
            .metric_stats_rollout_rate;

        is_rolled_out(organization_id, rate)
    }

    fn to_volume_metric(&self, bucket: &Bucket, outcome: &Outcome) -> Option<Bucket> {
        let volume = bucket.metadata.merges.get();
        if volume == 0 {
            return None;
        }

        let namespace = bucket.name.namespace();
        if !namespace.has_metric_stats() {
            return None;
        }

        let mut tags = BTreeMap::from([
            ("mri".to_owned(), bucket.name.to_string()),
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

    fn to_cardinality_metric(
        &self,
        limit: &CardinalityLimit,
        report: &CardinalityReport,
    ) -> Option<Bucket> {
        let cardinality = report.cardinality;
        if cardinality == 0 {
            return None;
        }

        let name = report.metric_name.as_ref()?;
        let namespace = name.namespace();

        if !namespace.has_metric_stats() {
            return None;
        }

        let tags = BTreeMap::from([
            ("mri".to_owned(), name.to_string()),
            ("mri.namespace".to_owned(), namespace.to_string()),
            (
                "cardinality.window".to_owned(),
                limit.window.window_seconds.to_string(),
            ),
        ]);

        Some(Bucket {
            timestamp: UnixTimestamp::now(),
            width: 0,
            name: cardinality_metric_mri(),
            value: BucketValue::Gauge(GaugeValue::single(cardinality.into())),
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

        ms.track_metric(scoping, bucket.clone(), Outcome::Accepted);

        bucket.metadata.merges = bucket.metadata.merges.saturating_add(41);
        ms.track_metric(
            scoping,
            bucket,
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
        ms.track_metric(scoping, bucket, Outcome::Accepted);

        drop(ms);

        assert!(receiver.blocking_recv().is_none());
    }

    #[test]
    fn test_metric_stats_disabled_namespace() {
        let (ms, mut receiver) = create_metric_stats(1.0);

        let scoping = scoping();
        let bucket =
            Bucket::parse(b"transactions/rt@millisecond:57|d", UnixTimestamp::now()).unwrap();
        ms.track_metric(scoping, bucket, Outcome::Accepted);

        drop(ms);

        assert!(receiver.blocking_recv().is_none());
    }
}
