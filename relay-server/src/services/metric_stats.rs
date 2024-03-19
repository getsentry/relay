use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use relay_config::Config;
use relay_metrics::{
    Aggregator, Bucket, BucketValue, MergeBuckets, MetricResourceIdentifier, UnixTimestamp,
};
use relay_quotas::Scoping;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};

use crate::services::outcome::Outcome;

fn volume_metric_mri() -> Arc<str> {
    static VOLUME_METRIC_MRI: OnceLock<Arc<str>> = OnceLock::new();

    Arc::clone(VOLUME_METRIC_MRI.get_or_init(|| "c:metric_stats/volume@none".into()))
}

/// Tracks an [`Outcome`] of a Metric item.
#[derive(Debug)]
pub struct TrackMetric {
    /// The creation timestamp of the track metric.
    pub timestamp: UnixTimestamp,
    /// Scoping of the metric.
    pub scoping: Scoping,
    /// The MRI, name of the metric.
    pub mri: Arc<str>,
    /// The outcome.
    pub outcome: Outcome,
    /// The amount of individual metric items Relay received for this metric.
    ///
    /// See also: [relay_metrics::BucketMetadata::merges].
    pub volume: u32,
}

impl TrackMetric {
    #[cfg(feature = "processing")]
    pub fn volume(
        scoping: Scoping,
        bucket: &relay_metrics::BucketView<'_>,
        outcome: Outcome,
    ) -> Self {
        Self {
            timestamp: UnixTimestamp::now(),
            scoping,
            mri: bucket.clone_name(),
            outcome,
            volume: bucket.metadata().merges.get(),
        }
    }

    fn to_volume_metric(&self) -> Option<Bucket> {
        if self.volume == 0 {
            return None;
        }

        let namespace = MetricResourceIdentifier::parse(&self.mri).ok()?.namespace;
        if !namespace.has_metric_stats() {
            return None;
        }

        let mut tags = BTreeMap::from([
            ("mri".to_owned(), self.mri.to_string()),
            ("mri.namespace".to_owned(), namespace.to_string()),
            (
                "outcome.id".to_owned(),
                self.outcome.to_outcome_id().as_u8().to_string(),
            ),
        ]);

        if let Some(reason) = self.outcome.to_reason() {
            tags.insert("outcome.reason".to_owned(), reason.into_owned());
        }

        Some(Bucket {
            timestamp: self.timestamp,
            width: 0,
            name: volume_metric_mri(),
            value: BucketValue::Counter(self.volume.into()),
            tags,
            metadata: Default::default(),
        })
    }
}

impl Interface for TrackMetric {}

impl FromMessage<Self> for TrackMetric {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

pub struct MetricStatsService {
    config: Arc<Config>,
    aggregator: Addr<Aggregator>,
}

impl MetricStatsService {
    pub fn new(config: Arc<Config>, aggregator: Addr<Aggregator>) -> Self {
        Self { config, aggregator }
    }

    fn handle_track_metric(&self, tm: TrackMetric) {
        if !self.config.processing_enabled() {
            return;
        }

        let Some(volume) = tm.to_volume_metric() else {
            return;
        };

        relay_log::trace!("Tracking volume of {} for mri '{}'", tm.volume, tm.mri);
        self.aggregator
            .send(MergeBuckets::new(tm.scoping.project_key, vec![volume]));
    }
}

impl Service for MetricStatsService {
    type Interface = TrackMetric;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                self.handle_track_metric(message);
            }
            dbg!("END");
        });
    }
}
