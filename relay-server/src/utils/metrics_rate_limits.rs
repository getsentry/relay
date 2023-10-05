//! Quota and rate limiting helpers for metrics and metrics buckets.
use chrono::{DateTime, Utc};
use relay_common::time::UnixTimestamp;
use relay_metrics::{Bucket, BucketValue, MetricNamespace, MetricResourceIdentifier};
use relay_quotas::{DataCategory, ItemScoping, Quota, RateLimits, Scoping};
use relay_system::Addr;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};

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

impl<Q: AsRef<Vec<Quota>>> MetricsLimiter<Q> {
    /// Create a new limiter instance.
    ///
    /// Returns Ok if `metrics` contain transaction metrics, `metrics` otherwise.
    pub fn create(buckets: Vec<Bucket>, quotas: Q, scoping: Scoping) -> Result<Self, Vec<Bucket>> {
        let counts: Vec<_> = buckets
            .iter()
            .map(|metric| {
                let mri = match MetricResourceIdentifier::parse(&metric.name) {
                    Ok(mri) => mri,
                    Err(_) => {
                        relay_log::error!("invalid MRI: {}", metric.name);
                        return None;
                    }
                };

                // Keep all metrics that are not transaction related:
                if mri.namespace != MetricNamespace::Transactions {
                    return None;
                }

                let usage = true; // TODO: Feature flag
                let count = match &metric.value {
                    // The "usage" counter directly tracks the number of processed transactions.
                    BucketValue::Counter(count) if usage && mri.name == "usage" => *count as usize,

                    // Fallback to the legacy "duration" metric, which is extracted exactly once for
                    // every processed transaction and was originally used to count transactions.
                    BucketValue::Distribution(dist) if mri.name == "duration" => dist.len(),

                    // For any other metric in the transaction namespace, we check the limit with
                    // quantity=0 so transactions are not double counted against the quota.
                    _ => return Some((0, false)),
                };

                let has_profile = metric.tag(PROFILE_TAG) == Some("true");
                Some((count, has_profile))
            })
            .collect();

        // Accumulate the total transaction count:
        let mut total_counts: Option<(usize, usize)> = None;
        for (tx_count, has_profile) in counts.iter().flatten() {
            let (total_txs, total_profiles) = total_counts.get_or_insert((0, 0));
            *total_txs += tx_count;
            if *has_profile {
                *total_profiles += tx_count;
            }
        }

        if let Some((transaction_count, profile_count)) = total_counts {
            let transaction_buckets = counts.iter().map(Option::is_some).collect();
            let profile_buckets = counts
                .iter()
                .map(|o| match o {
                    Some((_, has_profile)) => *has_profile,
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

        self.metrics = metrics
            .into_iter()
            .zip(self.transaction_buckets.iter())
            .filter_map(|(bucket, is_transaction_bucket)| {
                (!is_transaction_bucket).then_some(bucket)
            })
            .collect();

        // Track outcome for the transaction metrics we dropped here:
        if self.transaction_count > 0 {
            let timestamp = UnixTimestamp::now().as_datetime().unwrap_or_else(Utc::now);
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
        )
        .unwrap();

        limiter.enforce_limits(Ok(&RateLimits::new()), outcome_sink);
        let metrics = limiter.into_metrics();

        // All metrics have been preserved:
        assert_eq!(metrics.len(), 3);

        // Profile tag has been removed:
        assert!(metrics[0].tags.is_empty());
        assert!(metrics[1].tags.is_empty());
        assert!(!metrics[2].tags.is_empty());

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
