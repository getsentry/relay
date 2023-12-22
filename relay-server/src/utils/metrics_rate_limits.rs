//! Quota and rate limiting helpers for metrics and metrics buckets.
use chrono::{DateTime, Utc};
use relay_common::time::UnixTimestamp;
use relay_metrics::{
    Bucket, BucketView, BucketViewValue, MetricNamespace, MetricResourceIdentifier,
};
use relay_quotas::{DataCategory, ItemScoping, Quota, RateLimits, Scoping};
use relay_system::Addr;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::envelope::SourceQuantities;

/// Contains all data necessary to rate limit metrics or metrics buckets.
#[derive(Debug)]
pub struct MetricsLimiter<Q: AsRef<Vec<Quota>> = Vec<Quota>> {
    /// A list of aggregated metric buckets.
    metrics: Vec<Bucket>,

    /// The quotas set on the current project.
    quotas: Q,

    /// Project information.
    scoping: Scoping,

    /// Binary index of metrics/buckets in the transaction namespace (used to retain).
    transaction_buckets: Vec<bool>,

    /// Binary index of metrics/buckets that encode processed profiles.
    profile_buckets: Vec<bool>,

    /// The number of transactions contributing to these metrics.
    transaction_count: usize,

    /// The number of profiles contained in these metrics.
    profile_count: usize,
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
fn count_metric_bucket(metric: BucketView<'_>, mode: ExtractionMode) -> Option<TransactionCount> {
    let mri = match MetricResourceIdentifier::parse(metric.name()) {
        Ok(mri) => mri,
        Err(_) => {
            relay_log::error!("invalid MRI: {}", metric.name());
            return None;
        }
    };

    if mri.namespace != MetricNamespace::Transactions {
        return None;
    }

    let usage = matches!(mode, ExtractionMode::Usage);
    let count = match metric.value() {
        BucketViewValue::Counter(c) if usage && mri.name == "usage" => c as usize,
        BucketViewValue::Distribution(d) if !usage && mri.name == "duration" => d.len(),
        _ => 0,
    };

    let has_profile = matches!(mri.name.as_ref(), "usage" | "duration")
        && metric.tag(PROFILE_TAG) == Some("true");

    Some(TransactionCount { count, has_profile })
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
        if let Some(count) = count_metric_bucket(bucket.into(), mode) {
            quantities.transactions += count.count;
            if count.has_profile {
                quantities.profiles += count.count;
            }
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

/// Return value of [`count_metric_bucket`], containing the extracted
/// count of transactions and wether they have associated profiles.
#[derive(Debug, Clone, Copy)]
struct TransactionCount {
    /// Number of transactions.
    pub count: usize,
    /// Whether the transactions have associated profiles.
    pub has_profile: bool,
}

impl<Q: AsRef<Vec<Quota>>> MetricsLimiter<Q> {
    /// Create a new limiter instance.
    ///
    /// Returns Ok if `metrics` contain transaction metrics, `metrics` otherwise.
    pub fn create(
        buckets: Vec<Bucket>,
        quotas: Q,
        scoping: Scoping,
        mode: ExtractionMode,
    ) -> Result<Self, Vec<Bucket>> {
        let counts: Vec<_> = buckets
            .iter()
            .map(|metric| count_metric_bucket(BucketView::new(metric), mode))
            .collect();

        // Accumulate the total transaction count and profile count
        let total_counts = counts
            .iter()
            .filter_map(Option::as_ref)
            .map(|c| {
                let profile_count = if c.has_profile { c.count } else { 0 };
                (c.count, profile_count)
            })
            .reduce(|a, b| (a.0 + b.0, a.1 + b.1));

        if let Some((transaction_count, profile_count)) = total_counts {
            let transaction_buckets = counts.iter().map(Option::is_some).collect();
            let profile_buckets = counts
                .iter()
                .map(|o| match o {
                    Some(c) => c.has_profile,
                    None => false,
                })
                .collect();
            Ok(Self {
                metrics: buckets,
                quotas,
                scoping,
                transaction_buckets,
                profile_buckets,
                transaction_count,
                profile_count,
            })
        } else {
            Err(buckets)
        }
    }

    #[allow(dead_code)]
    pub fn scoping(&self) -> &Scoping {
        &self.scoping
    }

    #[allow(dead_code)]
    pub fn quotas(&self) -> &[Quota] {
        self.quotas.as_ref()
    }

    #[allow(dead_code)]
    pub fn transaction_count(&self) -> usize {
        self.transaction_count
    }

    fn drop_with_outcome(&mut self, outcome: Outcome, outcome_aggregator: Addr<TrackOutcome>) {
        // Drop transaction buckets:
        let metrics = std::mem::take(&mut self.metrics);
        let timestamp = Utc::now();

        self.metrics = metrics
            .into_iter()
            .zip(self.transaction_buckets.iter())
            .filter_map(|(bucket, is_transaction_bucket)| {
                (!is_transaction_bucket).then_some(bucket)
            })
            .collect();

        // Track outcome for the transaction metrics we dropped here:
        if self.transaction_count > 0 {
            outcome_aggregator.send(TrackOutcome {
                timestamp,
                scoping: self.scoping,
                outcome: outcome.clone(),
                event_id: None,
                remote_addr: None,
                category: DataCategory::Transaction,
                quantity: self.transaction_count as u32,
            });

            self.report_profiles(outcome, timestamp, outcome_aggregator);
        }
    }

    fn strip_profiles(&mut self) {
        for (has_profile, bucket) in self.profile_buckets.iter().zip(self.metrics.iter_mut()) {
            if *has_profile {
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
        if self.profile_count > 0 {
            outcome_aggregator.send(TrackOutcome {
                timestamp,
                scoping: self.scoping,
                outcome,
                event_id: None,
                remote_addr: None,
                category: DataCategory::Profile,
                quantity: self.profile_count as u32,
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
                let item_scoping = ItemScoping {
                    category: DataCategory::Transaction,
                    scoping: &self.scoping,
                };
                let active_rate_limits =
                    rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

                // If a rate limit is active, discard transaction buckets.
                if let Some(limit) = active_rate_limits.longest() {
                    self.drop_with_outcome(
                        Outcome::RateLimited(limit.reason_code.clone()),
                        outcome_aggregator,
                    );
                    dropped_stuff = true;
                } else {
                    // Also check profiles:
                    let item_scoping = ItemScoping {
                        category: DataCategory::Profile,
                        scoping: &self.scoping,
                    };
                    let active_rate_limits =
                        rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

                    if let Some(limit) = active_rate_limits.longest() {
                        self.strip_profiles();
                        self.report_profiles(
                            Outcome::RateLimited(limit.reason_code.clone()),
                            UnixTimestamp::now().as_datetime().unwrap_or_else(Utc::now),
                            outcome_aggregator,
                        )
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

    /// Consume this struct and return the contained metrics.
    pub fn into_metrics(self) -> Vec<Bucket> {
        self.metrics
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
                value: BucketValue::distribution(123.0),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(456.0),
            },
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".to_string(),
                tags: Default::default(),
                value: BucketValue::counter(1.0),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::counter(1.0),
            },
            Bucket {
                // unrelated metric
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "something_else".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(123.0),
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

    #[test]
    fn profiles_quota_is_enforced() {
        let metrics = vec![
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".to_string(),
                tags: Default::default(),
                value: BucketValue::distribution(123.0),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "d:transactions/duration@millisecond".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(456.0),
            },
            Bucket {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".to_string(),
                tags: Default::default(),
                value: BucketValue::counter(1.0),
            },
            Bucket {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "c:transactions/usage@none".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::counter(1.0),
            },
            Bucket {
                // unrelated metric
                timestamp: UnixTimestamp::now(),
                width: 0,
                name: "something_else".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: BucketValue::distribution(123.0),
            },
        ];
        let quotas = vec![Quota {
            id: None,
            categories: smallvec![DataCategory::Profile],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
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
        let metrics = limiter.into_metrics();

        // All metrics have been preserved:
        assert_eq!(metrics.len(), 5);

        // Profile tag has been removed:
        assert!(metrics[0].tags.is_empty());
        assert!(metrics[1].tags.is_empty());
        assert!(metrics[2].tags.is_empty());
        assert!(metrics[3].tags.is_empty());
        assert!(!metrics[4].tags.is_empty()); // unrelated metric still has it

        rx.close();

        let outcomes: Vec<_> = (0..)
            .map(|_| rx.blocking_recv())
            .take_while(|o| o.is_some())
            .flatten()
            .map(|o| (o.outcome, o.category, o.quantity))
            .collect();

        assert_eq!(
            outcomes,
            vec![(Outcome::RateLimited(None), DataCategory::Profile, 1)]
        );
    }
}
