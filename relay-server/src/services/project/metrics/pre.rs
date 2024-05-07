use relay_metrics::{Bucket, MetricNamespace};

use crate::services::project_cache::BucketSource;

use super::{state, Transformer};

/// Applies an initial filter to incoming metrics without knowledge about the project state.
pub struct PreProjectTransform {
    /// The source of the buckets.
    pub source: BucketSource,
}

impl Transformer<()> for PreProjectTransform {
    type NewState = state::PreProject;

    fn transform(self, mut buckets: Vec<Bucket>) -> Vec<Bucket> {
        buckets.retain(|bucket| match bucket.name.namespace() {
            MetricNamespace::Sessions => true,
            MetricNamespace::Transactions => true,
            MetricNamespace::Spans => true,
            MetricNamespace::Profiles => true,
            MetricNamespace::Custom => true,
            MetricNamespace::Stats => self.source == BucketSource::Internal,
            MetricNamespace::Unsupported => false,
        });
        buckets
    }
}
