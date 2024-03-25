//! Quota and rate limiting helpers for metrics and metrics buckets.

use chrono::{DateTime, Utc};
use relay_common::time::UnixTimestamp;
use relay_metrics::{
    Bucket, BucketView, BucketViewValue, MetricNamespace, MetricResourceIdentifier,
};
use relay_quotas::{DataCategory, ItemScoping, Quota, RateLimits, Scoping};
use relay_system::Addr;

use crate::envelope::SourceQuantities;
use crate::services::outcome::{Outcome, TrackOutcome};

/// Contains all data necessary to rate limit metrics or metrics buckets.
#[derive(Debug)]
pub struct MetricsLimiter<Q: AsRef<Vec<Quota>> = Vec<Quota>> {
    /// A list of aggregated metric buckets with some counters.
    buckets: Vec<SummarizedBucket>,

    /// The quotas set on the current project.
    quotas: Q,

    /// Project information.
    scoping: Scoping,

    /// The number of performance items (transactions, spans, profiles) contributing to these metrics.
    counts: EntityCounts,
}

const PROFILE_TAG: &str = "has_profile";

/// Extracts quota information from a metric.
///
/// If the metric was extracted from one or more transactions or spans, it returns the amount
/// of datapoints contained in the bucket.
///
/// Additionally tracks whether the transactions also contained profiling information.
fn summarize_bucket(metric: BucketView<'_>, mode: ExtractionMode) -> BucketSummary {
    let mri = match MetricResourceIdentifier::parse(metric.name()) {
        Ok(mri) => mri,
        Err(_) => {
            relay_log::error!("invalid MRI: {}", metric.name());
            return BucketSummary::default();
        }
    };

    match mri.namespace {
        MetricNamespace::Transactions => {
            let usage = matches!(mode, ExtractionMode::Usage);
            let count = match metric.value() {
                BucketViewValue::Counter(c) if usage && mri.name == "usage" => c.to_f64() as usize,
                BucketViewValue::Distribution(d) if !usage && mri.name == "duration" => d.len(),
                _ => 0,
            };
            let has_profile = matches!(mri.name.as_ref(), "usage" | "duration")
                && metric.tag(PROFILE_TAG) == Some("true");
            BucketSummary::Transactions { count, has_profile }
        }
        MetricNamespace::Spans => BucketSummary::Spans(match metric.value() {
            BucketViewValue::Counter(c) if mri.name == "usage" => c.to_f64() as usize,
            _ => 0,
        }),
        _ => {
            // Nothing to count
            BucketSummary::default()
        }
    }
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
        let summary = summarize_bucket(bucket.into(), mode);
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

/// The return value of [`summarize_bucket`].
///
/// Contains the count of total transactions or spans that went into this bucket.
#[derive(Debug, Default, Clone)]
pub enum BucketSummary {
    Transactions {
        count: usize,
        has_profile: bool,
    },
    Spans(usize),
    #[default]
    None,
}

impl BucketSummary {
    fn to_counts(&self) -> EntityCounts {
        match *self {
            BucketSummary::Transactions { count, has_profile } => EntityCounts {
                transactions: Some(count),
                spans: None,
                profiles: if has_profile { count } else { 0 },
            },
            BucketSummary::Spans(count) => EntityCounts {
                transactions: None,
                spans: Some(count),
                profiles: 0,
            },
            BucketSummary::None => EntityCounts::default(),
        }
    }
}

#[derive(Debug)]
struct SummarizedBucket {
    bucket: Bucket,
    summary: BucketSummary,
}

/// Contains the total counts of limitable entities represented by a batch of metrics.
#[derive(Debug, Default, Clone)]
struct EntityCounts {
    /// The number of transactions represented in the current batch.
    ///
    /// - `None` if the batch does not contain any transaction metrics.
    /// - `Some(0)` if the batch contains transaction metrics, but no `usage` count.
    /// - `Some(n > 0)` if the batch contains a `usage` count for transactions.
    ///
    /// The distinction between `None` and `Some(0)` is needed to decide whether or not a rate limit
    /// must be checked.
    transactions: Option<usize>,
    /// The number of spans represented in the current batch.
    ///
    /// - `None` if the batch does not contain any transaction metrics.
    /// - `Some(0)` if the batch contains transaction metrics, but no `usage` count.
    /// - `Some(n > 0)` if the batch contains a `usage` count for spans.
    ///
    /// The distinction between `None` and `Some(0)` is needed to decide whether or not a rate limit
    /// must be checked.
    spans: Option<usize>,
    /// The number of profiles represented in the current batch.
    ///
    /// We do not explicitly check the rate limiter for profiles, so there is no need to
    /// distinguish between `None` and `Some(0)`.
    profiles: usize,
}

impl std::ops::Add for EntityCounts {
    type Output = Self;

    fn add(self, rhs: EntityCounts) -> Self::Output {
        Self {
            transactions: add_some(self.transactions, rhs.transactions),
            spans: add_some(self.spans, rhs.spans),
            profiles: self.profiles + rhs.profiles,
        }
    }
}

fn add_some<T>(a: Option<T>, b: Option<T>) -> Option<T>
where
    T: std::ops::Add<Output = T>,
{
    match (a, b) {
        (None, None) => None,
        (None, Some(c)) | (Some(c), None) => Some(c),
        (Some(a), Some(b)) => Some(a + b),
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
                let summary = summarize_bucket(BucketView::new(&bucket), mode);
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

    /// Returns a reference to the scoping information.
    #[cfg(feature = "processing")]
    pub fn scoping(&self) -> &Scoping {
        &self.scoping
    }

    /// Returns a reference to the list of quotas.
    #[cfg(feature = "processing")]
    pub fn quotas(&self) -> &[Quota] {
        self.quotas.as_ref()
    }

    /// Counts the number of transactions/spans represented in this batch.
    ///
    /// Returns
    /// - `None` if the batch does not contain metrics related to the data category.
    /// - `Some(0)` if the batch contains metrics related to the data category, but no `usage` count.
    /// - `Some(n > 0)` if the batch contains a `usage` count for the given data category.
    ///
    /// The distinction between `None` and `Some(0)` is needed to decide whether or not a rate limit
    /// must be checked.
    #[cfg(feature = "processing")]
    pub fn count(&self, category: DataCategory) -> Option<usize> {
        match category {
            DataCategory::Transaction => self.counts.transactions,
            DataCategory::Span => self.counts.spans,
            _ => None,
        }
    }

    fn drop_with_outcome(
        &mut self,
        category: DataCategory,
        outcome: Outcome,
        outcome_aggregator: Addr<TrackOutcome>,
    ) {
        // Drop transaction buckets:
        let buckets = std::mem::take(&mut self.buckets);
        let timestamp = Utc::now();

        // Only keep buckets without counts:
        self.buckets = buckets
            .into_iter()
            .filter_map(|b| match b.summary {
                BucketSummary::Transactions { .. } if category == DataCategory::Transaction => None,
                BucketSummary::Spans(_) if category == DataCategory::Span => None,
                _ => Some(b),
            })
            .collect();

        // Track outcome for the transaction metrics we dropped:
        match (category, &self.counts) {
            (
                DataCategory::Transaction,
                EntityCounts {
                    transactions: Some(count),
                    ..
                },
            ) => {
                if *count > 0 {
                    outcome_aggregator.send(TrackOutcome {
                        timestamp,
                        scoping: self.scoping,
                        outcome: outcome.clone(),
                        event_id: None,
                        remote_addr: None,
                        category: DataCategory::Transaction,
                        quantity: *count as u32,
                    });
                }

                self.report_profiles(outcome.clone(), timestamp, outcome_aggregator.clone());
            }

            // Track outcome for the span metrics we dropped:
            (
                DataCategory::Span,
                EntityCounts {
                    spans: Some(count), ..
                },
            ) if *count > 0 => {
                outcome_aggregator.send(TrackOutcome {
                    timestamp,
                    scoping: self.scoping,
                    outcome: outcome.clone(),
                    event_id: None,
                    remote_addr: None,
                    category: DataCategory::Span,
                    quantity: *count as u32,
                });
            }
            _ => {}
        }
    }

    fn strip_profiles(&mut self) {
        for SummarizedBucket { bucket, summary } in self.buckets.iter_mut() {
            if let BucketSummary::Transactions { has_profile, .. } = summary {
                if *has_profile {
                    bucket.remove_tag(PROFILE_TAG);
                }
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
    // Returns true if any metrics were dropped.
    pub fn enforce_limits(
        &mut self,
        rate_limits: &RateLimits,
        outcome_aggregator: Addr<TrackOutcome>,
    ) -> bool {
        for category in [DataCategory::Transaction, DataCategory::Span] {
            let item_scoping = ItemScoping {
                category,
                scoping: &self.scoping,
                namespace: None,
            };
            let active_rate_limits =
                rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

            // If a rate limit is active, discard relevant buckets.
            if let Some(limit) = active_rate_limits.longest() {
                self.drop_with_outcome(
                    category,
                    Outcome::RateLimited(limit.reason_code.clone()),
                    outcome_aggregator.clone(),
                );

                return true;
            } else if category == DataCategory::Transaction {
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

        false
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
    use relay_metrics::{BucketMetadata, BucketValue};
    use relay_quotas::QuotaScope;
    use smallvec::smallvec;

    use super::*;

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

        limiter.enforce_limits(&RateLimits::new(), outcome_sink);
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
    fn profiles_limits_are_reported() {
        let metrics = vec![
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".into(),
                tags: Default::default(),
                value: BucketValue::distribution(123.into()),
                metadata: Default::default(),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".into(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(456.into()),
                metadata: Default::default(),
            },
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".into(),
                tags: Default::default(),
                value: BucketValue::counter(1.into()),
                metadata: Default::default(),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".into(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::counter(1.into()),
                metadata: Default::default(),
            },
            Bucket {
                // unrelated metric
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "something_else".into(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(123.into()),
                metadata: Default::default(),
            },
        ];
        let (metrics, outcomes) = run_limiter(metrics, deny(DataCategory::Transaction));

        assert_eq!(metrics.len(), 1);
        assert_eq!(&*metrics[0].name, "something_else");

        assert_eq!(
            outcomes,
            vec![
                (Outcome::RateLimited(None), DataCategory::Transaction, 2),
                (Outcome::RateLimited(None), DataCategory::Profile, 1)
            ]
        )
    }

    #[test]
    fn profiles_quota_is_enforced() {
        let metrics = vec![
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".into(),
                tags: Default::default(),
                value: BucketValue::distribution(123.into()),
                metadata: Default::default(),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".into(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(456.into()),
                metadata: Default::default(),
            },
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".into(),
                tags: Default::default(),
                value: BucketValue::counter(1.into()),
                metadata: Default::default(),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".into(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::counter(1.into()),
                metadata: Default::default(),
            },
            Bucket {
                // unrelated metric
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "something_else".into(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(123.into()),
                metadata: Default::default(),
            },
        ];

        let (metrics, outcomes) = run_limiter(metrics, deny(DataCategory::Profile));

        // All metrics have been preserved:
        assert_eq!(metrics.len(), 5);

        // Profile tag has been removed:
        assert!(metrics[0].tags.is_empty());
        assert!(metrics[1].tags.is_empty());
        assert!(metrics[2].tags.is_empty());
        assert!(metrics[3].tags.is_empty());
        assert!(!metrics[4].tags.is_empty()); // unrelated metric still has it

        assert_eq!(
            outcomes,
            vec![(Outcome::RateLimited(None), DataCategory::Profile, 1)]
        );
    }

    /// A few different bucket types
    fn mixed_bag() -> Vec<Bucket> {
        vec![
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".into(),
                tags: Default::default(),
                value: BucketValue::counter(12.into()),
                metadata: BucketMetadata::default(),
            },
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:spans/usage@none".into(),
                tags: Default::default(),
                value: BucketValue::counter(34.into()),
                metadata: BucketMetadata::default(),
            },
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:spans/usage@none".into(),
                tags: Default::default(),
                value: BucketValue::counter(56.into()),
                metadata: BucketMetadata::default(),
            },
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:spans/exclusive_time@millisecond".into(),
                tags: Default::default(),
                value: BucketValue::distribution(78.into()),
                metadata: BucketMetadata::default(),
            },
            Bucket {
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:custom/something@millisecond".into(),
                tags: Default::default(),
                value: BucketValue::distribution(78.into()),
                metadata: BucketMetadata::default(),
            },
        ]
    }

    #[test]
    fn mixed_with_span_quota() {
        let (metrics, outcomes) = run_limiter(mixed_bag(), deny(DataCategory::Span));

        assert_eq!(metrics.len(), 2);
        assert_eq!(&*metrics[0].name, "c:transactions/usage@none");
        assert_eq!(&*metrics[1].name, "d:custom/something@millisecond");

        assert_eq!(
            outcomes,
            vec![(Outcome::RateLimited(None), DataCategory::Span, 90)]
        );
    }

    #[test]
    fn mixed_with_transaction_quota() {
        let (metrics, outcomes) = run_limiter(mixed_bag(), deny(DataCategory::Transaction));

        assert_eq!(metrics.len(), 4);
        assert_eq!(&*metrics[0].name, "c:spans/usage@none");
        assert_eq!(&*metrics[1].name, "c:spans/usage@none");
        assert_eq!(&*metrics[2].name, "d:spans/exclusive_time@millisecond");
        assert_eq!(&*metrics[3].name, "d:custom/something@millisecond");

        assert_eq!(
            outcomes,
            vec![(Outcome::RateLimited(None), DataCategory::Transaction, 12)]
        );
    }
}
