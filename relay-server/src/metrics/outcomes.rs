use chrono::Utc;
use relay_metrics::{
    Bucket, BucketMetadata, BucketView, BucketViewValue, MetricName, MetricNamespace,
    MetricResourceIdentifier, MetricType,
};
use relay_quotas::{DataCategory, Scoping};
use relay_system::Addr;

use crate::envelope::SourceQuantities;
use crate::metrics::MetricStats;
use crate::services::outcome::{Outcome, TrackOutcome};
#[cfg(feature = "processing")]
use relay_cardinality::{CardinalityLimit, CardinalityReport};

pub const PROFILE_TAG: &str = "has_profile";

/// [`MetricOutcomes`] takes care of creating the right outcomes for metrics at the end of their
/// lifecycle.
///
/// It is aware of surrogate metrics like transaction- and span-duration as well as pure metrics
/// like custom.
#[derive(Debug, Clone)]
pub struct MetricOutcomes {
    metric_stats: MetricStats,
    outcomes: Addr<TrackOutcome>,
}

impl MetricOutcomes {
    /// Creates a new [`MetricOutcomes`].
    pub fn new(metric_stats: MetricStats, outcomes: Addr<TrackOutcome>) -> Self {
        Self {
            metric_stats,
            outcomes,
        }
    }

    /// Tracks an outcome for a list of buckets and generates the necessary outcomes.
    pub fn track(&self, scoping: Scoping, buckets: &[impl TrackableBucket], outcome: Outcome) {
        let timestamp = Utc::now();

        // Never emit accepted outcomes for surrogate metrics.
        // These are handled from within Sentry.
        if !matches!(outcome, Outcome::Accepted) {
            let SourceQuantities {
                transactions,
                spans,
                profiles,
                buckets,
            } = extract_quantities(buckets);

            let categories = [
                (DataCategory::Transaction, transactions as u32),
                (DataCategory::Span, spans as u32),
                (DataCategory::Profile, profiles as u32),
                (DataCategory::MetricBucket, buckets as u32),
            ];

            for (category, quantity) in categories {
                if quantity > 0 {
                    self.outcomes.send(TrackOutcome {
                        timestamp,
                        scoping,
                        outcome: outcome.clone(),
                        event_id: None,
                        remote_addr: None,
                        category,
                        quantity,
                    });
                }
            }
        }

        // When rejecting metrics, we need to make sure that the number of merges is correctly handled
        // for buckets views, since if we have a bucket which has 5 merges, and it's split into 2
        // bucket views, we will emit the volume of the rejection as 5 + 5 merges since we still read
        // the underlying metadata for each view, and it points to the same bucket reference.
        // Possible solutions to this problem include emitting the merges only if the bucket view is
        // the first of view or distributing uniformly the metadata between split views.
        for bucket in buckets {
            relay_log::trace!("{:<50} -> {outcome}", bucket.name());
            self.metric_stats.track_metric(scoping, bucket, &outcome)
        }
    }

    /// Tracks the cardinality of a metric.
    #[cfg(feature = "processing")]
    pub fn cardinality(
        &self,
        scoping: Scoping,
        limit: &CardinalityLimit,
        report: &CardinalityReport,
    ) {
        self.metric_stats.track_cardinality(scoping, limit, report)
    }
}

/// The return value of [`TrackableBucket::summary`].
///
/// Contains the count of total transactions or spans that went into this bucket.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BucketSummary {
    Transactions {
        count: usize,
        has_profile: bool,
    },
    Spans(usize),
    #[default]
    None,
}

/// Minimum information required to track outcomes for a metric bucket.
pub trait TrackableBucket {
    /// Full mri of the bucket.
    fn name(&self) -> &MetricName;

    /// Type of the metric bucket.
    fn ty(&self) -> MetricType;

    /// Extracts quota information from the metric bucket.
    ///
    /// If the metric was extracted from one or more transactions or spans, it returns the amount
    /// of datapoints contained in the bucket.
    ///
    /// Additionally tracks whether the transactions also contained profiling information.
    fn summary(&self) -> BucketSummary;

    /// Metric bucket metadata.
    fn metadata(&self) -> BucketMetadata;
}

impl<T: TrackableBucket> TrackableBucket for &T {
    fn name(&self) -> &MetricName {
        (**self).name()
    }

    fn ty(&self) -> MetricType {
        (**self).ty()
    }

    fn summary(&self) -> BucketSummary {
        (**self).summary()
    }

    fn metadata(&self) -> BucketMetadata {
        (**self).metadata()
    }
}

impl TrackableBucket for Bucket {
    fn name(&self) -> &MetricName {
        &self.name
    }

    fn ty(&self) -> MetricType {
        self.value.ty()
    }

    fn summary(&self) -> BucketSummary {
        BucketView::new(self).summary()
    }

    fn metadata(&self) -> BucketMetadata {
        self.metadata
    }
}

impl TrackableBucket for BucketView<'_> {
    fn name(&self) -> &MetricName {
        self.name()
    }

    fn ty(&self) -> MetricType {
        self.ty()
    }

    fn summary(&self) -> BucketSummary {
        if self.metadata().is_sampled {
            return BucketSummary::default();
        }

        let mri = match MetricResourceIdentifier::parse(self.name()) {
            Ok(mri) => mri,
            Err(_) => return BucketSummary::default(),
        };

        match mri.namespace {
            MetricNamespace::Transactions => {
                let count = match self.value() {
                    BucketViewValue::Counter(c) if mri.name == "usage" => c.to_f64() as usize,
                    _ => 0,
                };
                let has_profile = matches!(mri.name.as_ref(), "usage" | "duration")
                    && self.tag(PROFILE_TAG) == Some("true");
                BucketSummary::Transactions { count, has_profile }
            }
            MetricNamespace::Spans => BucketSummary::Spans(match self.value() {
                BucketViewValue::Counter(c) if mri.name == "usage" => c.to_f64() as usize,
                _ => 0,
            }),
            _ => {
                // Nothing to count
                BucketSummary::default()
            }
        }
    }

    fn metadata(&self) -> BucketMetadata {
        self.metadata()
    }
}

/// Extracts quota information from a list of metric buckets.
pub fn extract_quantities<I, T>(buckets: I) -> SourceQuantities
where
    I: IntoIterator<Item = T>,
    T: TrackableBucket,
{
    let mut quantities = SourceQuantities::default();

    for bucket in buckets {
        quantities.buckets += 1;
        let summary = bucket.summary();
        match summary {
            BucketSummary::Transactions { count, has_profile } => {
                quantities.transactions += count;
                if has_profile {
                    quantities.profiles += count;
                }
            }
            BucketSummary::Spans(count) => quantities.spans += count,
            BucketSummary::None => continue,
        };
    }

    quantities
}
