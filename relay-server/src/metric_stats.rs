use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

#[cfg(feature = "processing")]
use relay_cardinality::{CardinalityLimit, CardinalityReport};
use relay_config::Config;
#[cfg(feature = "processing")]
use relay_metrics::GaugeValue;
use relay_metrics::{
    Aggregator, Bucket, BucketValue, BucketView, MergeBuckets, MetricName, UnixTimestamp,
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

#[cfg(feature = "processing")]
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
    pub fn track_metric<'a, T>(&self, scoping: Scoping, bucket: T, outcome: Outcome)
    where
        T: Into<BucketView<'a>>,
    {
        if !self.is_enabled(scoping) {
            return;
        }

        let bucket = bucket.into();
        let Some(volume) = self.to_volume_metric(&bucket, &outcome) else {
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

    /// Tracks the cardinality of a metric.
    #[cfg(feature = "processing")]
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

    fn to_volume_metric(&self, bucket: &BucketView<'_>, outcome: &Outcome) -> Option<Bucket> {
        let volume = bucket.metadata().merges.get();
        if volume == 0 {
            return None;
        }

        let namespace = bucket.name().namespace();
        if !namespace.has_metric_stats() {
            return None;
        }

        let mut tags = BTreeMap::from([
            ("mri".to_owned(), bucket.name().to_string()),
            ("mri.type".to_owned(), bucket.ty().to_string()),
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

    #[cfg(feature = "processing")]
    fn to_cardinality_metric(
        &self,
        limit: &CardinalityLimit,
        report: &CardinalityReport,
    ) -> Option<Bucket> {
        let cardinality = report.cardinality;
        if cardinality == 0 {
            return None;
        }

        let mut tags = BTreeMap::from([
            ("cardinality.limit".to_owned(), limit.id.clone()),
            (
                "cardinality.scope".to_owned(),
                limit.scope.as_str().to_owned(),
            ),
            (
                "cardinality.window".to_owned(),
                limit.window.window_seconds.to_string(),
            ),
        ]);

        if let Some(ref name) = report.metric_name {
            tags.insert("mri".to_owned(), name.to_string());
            tags.insert("mri.namespace".to_owned(), name.namespace().to_string());
            if let Some(t) = name.try_type() {
                tags.insert("mri.type".to_owned(), t.to_string());
            }
        } else {
            if let Some(namespace) = limit.namespace {
                tags.insert("mri.namespace".to_owned(), namespace.to_string());
            }
            if let Some(t) = report.metric_type {
                tags.insert("mri.type".to_owned(), t.to_string());
            }
        }

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
    #[cfg(feature = "processing")]
    use relay_cardinality::{CardinalityScope, SlidingWindow};
    use relay_dynamic_config::GlobalConfig;
    #[cfg(feature = "processing")]
    use relay_metrics::{MetricNamespace, MetricType};
    use relay_quotas::ReasonCode;
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;

    impl MetricStats {
        pub fn test() -> (Self, UnboundedReceiver<Aggregator>) {
            create_metric_stats(1.0)
        }
    }

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

        ms.track_metric(scoping, &bucket.clone(), Outcome::Accepted);

        bucket.metadata.merges = bucket.metadata.merges.saturating_add(41);
        ms.track_metric(
            scoping,
            &bucket,
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
                ("mri.type", "d"),
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
                ("mri.type", "d"),
                ("mri.namespace", "custom"),
                ("outcome.id", "2"),
                ("outcome.reason", "foobar"),
            )
        );

        assert!(receiver.blocking_recv().is_none());
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_metric_stats_cardinality_name() {
        let (ms, mut receiver) = create_metric_stats(1.0);

        let scoping = scoping();
        let limit = CardinalityLimit {
            id: "test".to_owned(),
            passive: false,
            report: true,
            window: SlidingWindow {
                window_seconds: 246,
                granularity_seconds: 123,
            },
            limit: 99,
            scope: CardinalityScope::Name,
            namespace: None,
        };
        let report = CardinalityReport {
            organization_id: Some(scoping.organization_id),
            project_id: Some(scoping.project_id),
            metric_type: None,
            metric_name: Some(MetricName::from("d:custom/rt@millisecond")),
            cardinality: 12,
        };

        ms.track_cardinality(scoping, &limit, &report);

        drop(ms);

        let Aggregator::MergeBuckets(mb) = receiver.blocking_recv().unwrap() else {
            panic!();
        };
        assert_eq!(mb.project_key(), scoping.project_key);

        let mut buckets = mb.buckets();
        assert_eq!(buckets.len(), 1);
        let bucket = buckets.pop().unwrap();

        assert_eq!(&*bucket.name, "g:metric_stats/cardinality@none");
        assert_eq!(
            bucket.value,
            BucketValue::Gauge(GaugeValue {
                last: 12.into(),
                min: 12.into(),
                max: 12.into(),
                sum: 12.into(),
                count: 1,
            })
        );
        assert_eq!(
            bucket.tags,
            tags!(
                ("mri", "d:custom/rt@millisecond"),
                ("mri.type", "d"),
                ("mri.namespace", "custom"),
                ("cardinality.limit", "test"),
                ("cardinality.scope", "name"),
                ("cardinality.window", "246"),
            )
        );

        assert!(receiver.blocking_recv().is_none());
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_metric_stats_cardinality_type() {
        let (ms, mut receiver) = create_metric_stats(1.0);

        let scoping = scoping();
        let limit = CardinalityLimit {
            id: "test".to_owned(),
            passive: false,
            report: true,
            window: SlidingWindow {
                window_seconds: 246,
                granularity_seconds: 123,
            },
            limit: 99,
            scope: CardinalityScope::Type,
            namespace: Some(MetricNamespace::Spans),
        };
        let report = CardinalityReport {
            organization_id: Some(scoping.organization_id),
            project_id: Some(scoping.project_id),
            metric_type: Some(MetricType::Distribution),
            metric_name: None,
            cardinality: 12,
        };

        ms.track_cardinality(scoping, &limit, &report);

        drop(ms);

        let Aggregator::MergeBuckets(mb) = receiver.blocking_recv().unwrap() else {
            panic!();
        };
        assert_eq!(mb.project_key(), scoping.project_key);

        let mut buckets = mb.buckets();
        assert_eq!(buckets.len(), 1);
        let bucket = buckets.pop().unwrap();

        assert_eq!(&*bucket.name, "g:metric_stats/cardinality@none");
        assert_eq!(
            bucket.value,
            BucketValue::Gauge(GaugeValue {
                last: 12.into(),
                min: 12.into(),
                max: 12.into(),
                sum: 12.into(),
                count: 1,
            })
        );
        assert_eq!(
            bucket.tags,
            tags!(
                ("mri.type", "d"),
                ("mri.namespace", "spans"),
                ("cardinality.limit", "test"),
                ("cardinality.scope", "type"),
                ("cardinality.window", "246"),
            )
        );

        assert!(receiver.blocking_recv().is_none());
    }

    #[test]
    fn test_metric_stats_rollout_rate_disabled() {
        let (ms, mut receiver) = create_metric_stats(0.0);

        let scoping = scoping();
        let bucket = Bucket::parse(b"rt@millisecond:57|d", UnixTimestamp::now()).unwrap();
        ms.track_metric(scoping, &bucket, Outcome::Accepted);

        drop(ms);

        assert!(receiver.blocking_recv().is_none());
    }

    #[test]
    fn test_metric_stats_disabled_namespace() {
        let (ms, mut receiver) = create_metric_stats(1.0);

        let scoping = scoping();
        let bucket =
            Bucket::parse(b"transactions/rt@millisecond:57|d", UnixTimestamp::now()).unwrap();
        ms.track_metric(scoping, &bucket, Outcome::Accepted);

        drop(ms);

        assert!(receiver.blocking_recv().is_none());
    }
}
