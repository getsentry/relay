//! COGS related metric utilities.

use relay_cogs::{AppFeature, FeatureWeights};

use crate::{Bucket, BucketView, MetricNamespace};

/// COGS estimator based on the estimated size of each bucket in bytes.
pub struct BySize<T>(pub T);

impl<'a, T: IntoIterator<Item = &'a Bucket>> From<BySize<T>> for FeatureWeights {
    fn from(value: BySize<T>) -> Self {
        metric_app_features(value.0, |b| BucketView::new(b).estimated_size())
    }
}

/// COGS estimator based on the bucket count.
pub struct ByCount<T>(pub T);

impl<'a, T: IntoIterator<Item = &'a Bucket>> From<ByCount<T>> for FeatureWeights {
    fn from(value: ByCount<T>) -> Self {
        metric_app_features(value.0, |_| 1)
    }
}

fn metric_app_features<'a, T>(buckets: T, f: impl Fn(&Bucket) -> usize) -> FeatureWeights
where
    T: IntoIterator<Item = &'a Bucket>,
{
    let mut b = FeatureWeights::builder();

    for bucket in buckets.into_iter() {
        b.add_weight(to_app_feature(bucket.name.namespace()), f(bucket));
    }

    b.build()
}

fn to_app_feature(ns: MetricNamespace) -> AppFeature {
    match ns {
        MetricNamespace::Sessions => AppFeature::MetricsSessions,
        MetricNamespace::Transactions => AppFeature::MetricsTransactions,
        MetricNamespace::Spans => AppFeature::MetricsSpans,
        MetricNamespace::Profiles => AppFeature::MetricsProfiles,
        MetricNamespace::Custom => AppFeature::MetricsCustom,
        MetricNamespace::Stats => AppFeature::MetricsStats,
        MetricNamespace::Unsupported => AppFeature::MetricsUnsupported,
    }
}
