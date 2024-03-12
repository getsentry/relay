//! COGS related metric utilities.

use relay_base_schema::metrics::{MetricNamespace, MetricResourceIdentifier};
use relay_cogs::{AppFeature, FeatureWeights};

use crate::{Bucket, BucketView};

/// COGS estimator based on the estimated size of each bucket in bytes.
pub struct BySize<'a>(pub &'a [Bucket]);

impl<'a> From<BySize<'a>> for FeatureWeights {
    fn from(value: BySize<'a>) -> Self {
        metric_app_features(value.0, |b| BucketView::new(b).estimated_size())
    }
}

/// COGS estimator based on the bucket count.
pub struct ByCount<'a, T: IntoIterator<Item = &'a Bucket>>(pub T);

impl<'a, T: IntoIterator<Item = &'a Bucket>> From<ByCount<'a, T>> for FeatureWeights {
    fn from(value: ByCount<'a, T>) -> Self {
        metric_app_features(value.0, |_| 1)
    }
}

fn metric_app_features<'a, T>(buckets: T, f: impl Fn(&Bucket) -> usize) -> FeatureWeights
where
    T: IntoIterator<Item = &'a Bucket>,
{
    let mut b = FeatureWeights::builder();

    for bucket in buckets.into_iter() {
        b.add_weight(to_app_feature(namespace(bucket)), f(bucket));
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
        MetricNamespace::Unsupported => AppFeature::MetricsUnsupported,
    }
}

fn namespace(bucket: &Bucket) -> MetricNamespace {
    MetricResourceIdentifier::parse(&bucket.name)
        .map(|mri| mri.namespace)
        .unwrap_or(MetricNamespace::Unsupported)
}
