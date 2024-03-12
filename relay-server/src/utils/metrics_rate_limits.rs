//! Quota and rate limiting helpers for metrics and metrics buckets.

use chrono::{DateTime, Utc};
use relay_common::time::UnixTimestamp;
use relay_metrics::{
    Bucket, BucketView, BucketViewValue, MetricNamespace, MetricResourceIdentifier,
};
use relay_quotas::{DataCategory, ItemScoping, Quota, RateLimits, Scoping};
use relay_system::Addr;

use crate::envelope::SourceQuantities;
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};

/// Contains all data necessary to rate limit metrics or metrics buckets.
#[derive(Debug)]
pub struct MetricsLimiter<Q: AsRef<Vec<Quota>> = Vec<Quota>> {
    /// TODO: docs
    buckets: Vec<SummarizedBucket>,

    /// The quotas set on the current project.
    quotas: Q,

    /// Project information.
    scoping: Scoping,

    /// The number of transactions contributing to these metrics.
    counts: TotalEntityCounts,
}

const PROFILE_TAG: &str = "has_profile";

/// Extracts quota information from a metric.
///
/// If the metric was extracted from a or multiple transaction, it returns the amount
/// of datapoints contained in the bucket.
///
/// Additionally tracks whether the transactions also contained profiling information.
///
/// Returns `None` if the metric was not extracted from transactions.
fn count_metric_bucket(metric: BucketView<'_>, mode: ExtractionMode) -> BucketSummary {
    let mri = match MetricResourceIdentifier::parse(metric.name()) {
        Ok(mri) => mri,
        Err(_) => {
            relay_log::error!("invalid MRI: {}", metric.name());
            return BucketSummary::default();
        }
    };

    let count = if mri.namespace == MetricNamespace::Transactions {
        let usage = matches!(mode, ExtractionMode::Usage);
        EntityCount::Transactions(match metric.value() {
            BucketViewValue::Counter(c) if usage && mri.name == "usage" => c.to_f64() as usize,
            BucketViewValue::Distribution(d) if !usage && mri.name == "duration" => d.len(),
            _ => 0,
        })
    } else if mri.namespace == MetricNamespace::Spans {
        EntityCount::Spans(match metric.value() {
            BucketViewValue::Counter(c) if mri.name == "usage" => c.to_f64() as usize,
            _ => 0,
        })
    } else {
        // Nothing to count
        return BucketSummary::default();
    };

    let has_profile = matches!(mri.name.as_ref(), "usage" | "duration")
        && metric.tag(PROFILE_TAG) == Some("true");

    BucketSummary { count, has_profile }
}

/// Extracts quota information from a list of metric buckets.
pub fn extract_metric_quantities<'a, I, V>(buckets: I, mode: ExtractionMode) -> SourceQuantities
where
    I: IntoIterator<Item = V>,
    BucketView<'a>: From<V>,
{
    let mut quantities = SourceQuantities::default();

    for bucket in buckets {
        quantities.buckets += 1;
        let summary = count_metric_bucket(bucket.into(), mode);
        let (count, target) = match summary.count {
            EntityCount::Transactions(count) => (count, &mut quantities.transactions),
            EntityCount::Spans(count) => (count, &mut quantities.spans),
            EntityCount::None => continue,
        };
        *target += count;

        if summary.has_profile {
            quantities.profiles += count;
        }
    }

    quantities
}

pub fn reject_metrics(
    addr: &Addr<TrackOutcome>,
    quantities: SourceQuantities,
    scoping: Scoping,
    outcome: Outcome,
) {
    let timestamp = Utc::now();

    let categories = [
        (DataCategory::Transaction, quantities.transactions as u32),
        (DataCategory::Profile, quantities.profiles as u32),
        (DataCategory::MetricBucket, quantities.buckets as u32),
    ];

    for (category, quantity) in categories {
        if quantity > 0 {
            addr.send(TrackOutcome {
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

/// Wether to extract transaction and profile count based on the usage or duration metric.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtractionMode {
    /// Use the usage count metric.
    Usage,
    /// Use the duration distribution metric.
    Duration,
}

/// The return value of [`count_metric_bucket`].
///
/// Contains the count of total transactions or spans that went into this bucket.
#[derive(Debug, Default, Clone)]
struct BucketSummary {
    /// Number of countable entities.
    pub count: EntityCount,
    /// Whether the countable entities have associated profiles.
    pub has_profile: bool,
}

impl BucketSummary {
    fn to_counts(&self) -> TotalEntityCounts {
        let transactions = match self.count {
            EntityCount::Transactions(count) => count,
            _ => 0,
        };
        let spans = match self.count {
            EntityCount::Spans(count) => count,
            _ => 0,
        };
        let profiles = match self.count {
            EntityCount::Transactions(count) | EntityCount::Spans(count) if self.has_profile => {
                count
            }
            _ => 0,
        };

        TotalEntityCounts {
            transactions,
            spans,
            profiles,
        }
    }
}

#[derive(Debug)]
struct SummarizedBucket {
    bucket: Bucket,
    summary: BucketSummary,
}

impl std::ops::Deref for SummarizedBucket {
    type Target = Bucket;

    fn deref(&self) -> &Self::Target {
        &self.bucket
    }
}

#[derive(Debug, Default, Clone)]
struct TotalEntityCounts {
    transactions: usize,
    spans: usize,
    profiles: usize,
}

impl std::ops::Add for TotalEntityCounts {
    type Output = Self;

    fn add(self, rhs: TotalEntityCounts) -> Self::Output {
        Self {
            transactions: self.transactions + rhs.transactions,
            spans: self.spans + rhs.spans,
            profiles: self.profiles + rhs.profiles,
        }
    }
}

#[derive(Debug, Default, Clone)]
enum EntityCount {
    Transactions(usize),
    Spans(usize),
    #[default]
    None,
}

impl EntityCount {
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl<Q: AsRef<Vec<Quota>>> MetricsLimiter<Q> {
    /// Create a new limiter instance.
    ///
    /// Returns Ok if `metrics` contain relevant metrics, `metrics` otherwise.
    pub fn create(
        buckets: Vec<Bucket>,
        quotas: Q,
        scoping: Scoping,
        mode: ExtractionMode,
    ) -> Result<Self, Vec<Bucket>> {
        let buckets: Vec<_> = buckets
            .into_iter()
            .map(|bucket| {
                let summary = count_metric_bucket(BucketView::new(&bucket), mode);
                SummarizedBucket { bucket, summary }
            })
            .collect();

        // Accumulate the total counts
        let total_counts = buckets
            .iter()
            .map(|b| b.summary.to_counts())
            .reduce(|a, b| a + b);
        if let Some(counts) = total_counts {
            Ok(Self {
                buckets,
                quotas,
                scoping,
                counts,
            })
        } else {
            Err(buckets.into_iter().map(|s| s.bucket).collect())
        }
    }

    // TODO: docs
    pub fn scoping(&self) -> &Scoping {
        &self.scoping
    }

    // TODO: docs
    pub fn quotas(&self) -> &[Quota] {
        self.quotas.as_ref()
    }

    // TODO: docs
    pub fn transaction_count(&self) -> usize {
        self.counts.transactions
    }

    // TODO: docs
    pub fn span_count(&self) -> usize {
        self.counts.spans
    }

    fn drop_with_outcome(&mut self, outcome: Outcome, outcome_aggregator: Addr<TrackOutcome>) {
        // Drop transaction buckets:
        let buckets = std::mem::take(&mut self.buckets);
        let timestamp = Utc::now();

        // Only keep buckets without counts:
        self.buckets = buckets
            .into_iter()
            .filter_map(|b| b.summary.count.is_none().then_some(b))
            .collect();

        // Track outcome for the transaction metrics we dropped:
        if self.counts.transactions > 0 {
            outcome_aggregator.send(TrackOutcome {
                timestamp,
                scoping: self.scoping,
                outcome: outcome.clone(),
                event_id: None,
                remote_addr: None,
                category: DataCategory::Transaction,
                quantity: self.counts.transactions as u32,
            });
        }

        // Track outcome for the span metrics we dropped:
        if self.counts.spans > 0 {
            outcome_aggregator.send(TrackOutcome {
                timestamp,
                scoping: self.scoping,
                outcome: outcome.clone(),
                event_id: None,
                remote_addr: None,
                category: DataCategory::Span,
                quantity: self.counts.spans as u32,
            });
        }

        self.report_profiles(outcome, timestamp, outcome_aggregator);
    }

    fn strip_profiles(&mut self) {
        for SummarizedBucket { bucket, summary } in self.buckets.iter_mut() {
            if summary.has_profile {
                bucket.remove_tag(PROFILE_TAG);
            }
        }
    }

    fn report_profiles(
        &self,
        outcome: Outcome,
        timestamp: DateTime<Utc>,
        outcome_aggregator: Addr<TrackOutcome>,
    ) {
        if self.counts.profiles > 0 {
            outcome_aggregator.send(TrackOutcome {
                timestamp,
                scoping: self.scoping,
                outcome,
                event_id: None,
                remote_addr: None,
                category: DataCategory::Profile,
                quantity: self.counts.profiles as u32,
            });
        }
    }

    // Drop transaction-related metrics and create outcomes for any active rate limits.
    //
    // If rate limits could not be checked for some reason, pass an `Err` to this function.
    // In this case, transaction-related metrics will also be dropped, and an "internal"
    // outcome is generated.
    //
    // Returns true if any metrics were dropped.
    pub fn enforce_limits(
        &mut self,
        rate_limits: Result<&RateLimits, ()>,
        outcome_aggregator: Addr<TrackOutcome>,
    ) -> bool {
        let mut dropped_stuff = false;

        match rate_limits {
            Ok(rate_limits) => {
                for category in [DataCategory::Transaction, DataCategory::Span] {
                    let item_scoping = ItemScoping {
                        category,
                        scoping: &self.scoping,
                        namespace: None,
                    };
                    let active_rate_limits =
                        rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

                    // If a rate limit is active, discard transaction buckets.
                    if let Some(limit) = active_rate_limits.longest() {
                        self.drop_with_outcome(
                            Outcome::RateLimited(limit.reason_code.clone()),
                            outcome_aggregator.clone(),
                        );
                        dropped_stuff = true;
                    } else {
                        // Also check profiles:
                        let item_scoping = ItemScoping {
                            category: DataCategory::Profile,
                            scoping: &self.scoping,
                            namespace: None,
                        };
                        let active_rate_limits =
                            rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

                        if let Some(limit) = active_rate_limits.longest() {
                            self.strip_profiles();
                            self.report_profiles(
                                Outcome::RateLimited(limit.reason_code.clone()),
                                UnixTimestamp::now().as_datetime().unwrap_or_else(Utc::now),
                                outcome_aggregator.clone(),
                            )
                        }
                    }
                }
            }
            Err(_) => {
                // Error from rate limiter, drop transaction buckets.
                self.drop_with_outcome(
                    Outcome::Invalid(DiscardReason::Internal),
                    outcome_aggregator,
                );
                dropped_stuff = true;
            }
        };

        dropped_stuff
    }

    /// Returns a reference to the contained metrics.
    #[cfg(feature = "processing")]
    pub fn buckets(&self) -> impl Iterator<Item = &Bucket> {
        self.buckets.iter().map(|s| &s.bucket)
    }

    /// Consume this struct and return the contained metrics.
    pub fn into_buckets(self) -> Vec<Bucket> {
        self.buckets.into_iter().map(|s| s.bucket).collect()
    }
}

#[cfg(test)]
mod tests {
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_metrics::{Bucket, BucketValue};
    use relay_quotas::{Quota, QuotaScope};
    use smallvec::smallvec;

    use super::*;

    #[test]
    fn profiles_limits_are_reported() {
        let metrics = vec![
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".to_string(),
                tags: Default::default(),
                value: BucketValue::distribution(123.into()),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(456.into()),
            },
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".to_string(),
                tags: Default::default(),
                value: BucketValue::counter(1.into()),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::counter(1.into()),
            },
            Bucket {
                // unrelated metric
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "something_else".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(123.into()),
            },
        ];
        let quotas = vec![Quota {
            id: None,
            categories: smallvec![DataCategory::Transaction],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
        }];
        let (outcome_sink, mut rx) = Addr::custom();

        let mut limiter = MetricsLimiter::create(
            metrics,
            quotas,
            Scoping {
                organization_id: 1,
                project_id: ProjectId::new(1),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            ExtractionMode::Usage,
        )
        .unwrap();

        limiter.enforce_limits(Ok(&RateLimits::new()), outcome_sink);

        rx.close();

        let outcomes: Vec<_> = (0..)
            .map(|_| rx.blocking_recv())
            .take_while(|o| o.is_some())
            .flatten()
            .map(|o| (o.outcome, o.category, o.quantity))
            .collect();

        assert_eq!(
            outcomes,
            vec![
                (Outcome::RateLimited(None), DataCategory::Transaction, 2),
                (Outcome::RateLimited(None), DataCategory::Profile, 1)
            ]
        );
    }

    /// A few different bucket types
    fn mixed_bag() -> Vec<Bucket> {
        vec![
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".to_string(),
                tags: Default::default(),
                value: BucketValue::counter(12.into()),
            },
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:spans/usage@none".to_string(),
                tags: Default::default(),
                value: BucketValue::counter(34.into()),
            },
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:spans/usage@none".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(56.into()),
            },
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:spans/exclusive_time@millisecond".to_string(),
                tags: Default::default(),
                value: BucketValue::distribution(78.into()),
            },
            Bucket {
                // Unrelated metric with has_profile tag
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:custom/something@millisecond".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(78.into()),
            },
        ]
    }

    fn deny(category: DataCategory) -> Vec<Quota> {
        vec![Quota {
            id: None,
            categories: smallvec![category],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
        }]
    }

    /// Applies rate limits and returns the remaining buckets and generated outcomes.
    fn run_limiter(
        metrics: Vec<Bucket>,
        quotas: Vec<Quota>,
    ) -> (Vec<Bucket>, Vec<(Outcome, DataCategory, u32)>) {
        let (outcome_sink, mut rx) = Addr::custom();

        let mut limiter = MetricsLimiter::create(
            metrics,
            quotas,
            Scoping {
                organization_id: 1,
                project_id: ProjectId::new(1),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            ExtractionMode::Usage,
        )
        .unwrap();

        limiter.enforce_limits(Ok(&RateLimits::new()), outcome_sink);
        let metrics = limiter.into_buckets();

        rx.close();

        let outcomes: Vec<_> = (0..)
            .map(|_| rx.blocking_recv())
            .take_while(|o| o.is_some())
            .flatten()
            .map(|o| (o.outcome, o.category, o.quantity))
            .collect();

        (metrics, outcomes)
    }

    #[test]
    fn span_quota_enforced() {
        let (metrics, outcomes) = run_limiter(mixed_bag(), deny(DataCategory::Span));

        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "c:transactions/usage@none");
        assert_eq!(metrics[1].name, "d:custom/something@millisecond");

        assert_eq!(
            outcomes,
            vec![(Outcome::RateLimited(None), DataCategory::Profile, 1)]
        );
    }
}
