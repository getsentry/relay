//! Quota and rate limiting helpers for metrics and metrics buckets.
use chrono::Utc;
use relay_common::{DataCategory, UnixTimestamp};
use relay_metrics::{MetricNamespace, MetricResourceIdentifier, MetricsContainer};
use relay_quotas::{ItemScoping, Quota, RateLimits, Scoping};
use relay_system::Addr;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};

/// Contains all data necessary to rate limit metrics or metrics buckets.
#[derive(Debug)]
pub struct MetricsLimiter<M: MetricsContainer, Q: AsRef<Vec<Quota>> = Vec<Quota>> {
    /// A list of metrics or buckets.
    metrics: Vec<M>,

    /// The quotas set on the current project.
    quotas: Q,

    /// Project information.
    scoping: Scoping,

    /// Binary index of metrics/buckets in the transaction namespace (used to retain).
    transaction_buckets: Vec<bool>,

    /// Binary index of metrics/buckets in the transaction namespace that have profiles.
    profile_buckets: Vec<bool>, // TODO: remove for simplicity?

    /// The number of transactions contributing to these metrics.
    transaction_count: usize,

    /// The number of profiles contributing to these metrics.
    profile_count: usize,
}

impl<M: MetricsContainer, Q: AsRef<Vec<Quota>>> MetricsLimiter<M, Q> {
    /// Create a new limiter instance.
    ///
    /// Returns Ok if `metrics` contain transaction metrics, `metrics` otherwise.
    pub fn create(buckets: Vec<M>, quotas: Q, scoping: Scoping) -> Result<Self, Vec<M>> {
        let counts: Vec<_> = buckets
            .iter()
            .map(|metric| {
                let mri = match MetricResourceIdentifier::parse(metric.name()) {
                    Ok(mri) => mri,
                    Err(_) => {
                        relay_log::error!("invalid MRI: {}", metric.name());
                        return None;
                    }
                };

                // Keep all metrics that are not transaction related:
                if mri.namespace != MetricNamespace::Transactions {
                    return None;
                }

                if mri.name == "duration" {
                    // The "duration" metric is extracted exactly once for every processed
                    // transaction, so we can use it to count the number of transactions.
                    let count = metric.len();
                    let has_profile = metric.tag("has_profile") == Some("true");
                    Some((count, has_profile))
                } else {
                    // For any other metric in the transaction namespace, we check the limit with
                    // quantity=0 so transactions are not double counted against the quota.
                    Some((0, false))
                }
            })
            .collect();

        // Accumulate the total counts
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
                .map(|b| b.map_or(false, |(_, has_profile)| has_profile))
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

    fn drop_transactions_with_outcome(
        &mut self,
        outcome: Outcome,
        outcome_aggregator: Addr<TrackOutcome>,
    ) {
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
                outcome,
                event_id: None,
                remote_addr: None,
                category: DataCategory::Transaction,
                quantity: self.transaction_count as u32,
            });
        }
    }

    fn strip_profiles_with_outcome(
        &mut self,
        outcome: Outcome,
        outcome_aggregator: Addr<TrackOutcome>,
    ) {
        let mut count = 0;

        for (bucket, is_profile_bucket) in self.metrics.iter_mut().zip(self.profile_buckets.iter())
        {
            if *is_profile_bucket && bucket.remove_tag("has_profile").as_deref() == Some("true") {
                count += bucket.len();
            }
        }
        debug_assert_eq!(count, self.profile_count);

        // Track outcome for the transaction metrics we dropped here:
        if count > 0 {
            let timestamp = UnixTimestamp::now().as_datetime().unwrap_or_else(Utc::now);
            outcome_aggregator.send(TrackOutcome {
                timestamp,
                scoping: self.scoping,
                outcome,
                event_id: None,
                remote_addr: None,
                category: DataCategory::Profile,
                quantity: count as u32,
            });
        }
    }

    // Drop transaction-related metrics and create outcomes for any active rate limits.
    //
    // If rate limits could not be checked for some reason, pass an `Err` to this function.
    // In this case, transaction-related metrics will also be dropped, and an "internal"
    // outcome is generated.
    //
    // Returns true if any rate limit was enforced.
    pub fn enforce_limits(
        &mut self,
        rate_limits: Result<&RateLimits, ()>,
        outcome_aggregator: Addr<TrackOutcome>,
    ) -> bool {
        let mut enforced_anything = false;
        match rate_limits {
            Ok(rate_limits) => {
                {
                    let item_scoping = ItemScoping {
                        category: DataCategory::Transaction,
                        scoping: &self.scoping,
                    };
                    let active_rate_limits =
                        rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

                    // If a rate limit is active, discard transaction buckets.
                    if let Some(limit) = active_rate_limits.longest() {
                        self.strip_profiles_with_outcome(
                            Outcome::RateLimited(limit.reason_code.clone()),
                            outcome_aggregator.clone(),
                        );
                        self.drop_transactions_with_outcome(
                            Outcome::RateLimited(limit.reason_code.clone()),
                            outcome_aggregator,
                        );
                        enforced_anything = true;
                    } else {
                        let item_scoping = ItemScoping {
                            category: DataCategory::Profile,
                            scoping: &self.scoping,
                        };
                        let active_rate_limits =
                            rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

                        // If a rate limit is active, discard transaction buckets.
                        if let Some(limit) = active_rate_limits.longest() {
                            self.strip_profiles_with_outcome(
                                Outcome::RateLimited(limit.reason_code.clone()),
                                outcome_aggregator,
                            );
                            enforced_anything = true;
                        }
                    }
                }
            }
            Err(_) => {
                // Error from rate limiter, drop transaction buckets.
                self.strip_profiles_with_outcome(
                    Outcome::Invalid(DiscardReason::Internal),
                    outcome_aggregator.clone(),
                );
                self.drop_transactions_with_outcome(
                    Outcome::Invalid(DiscardReason::Internal),
                    outcome_aggregator,
                );
            }
        };

        enforced_anything
    }

    /// Consume this struct and return the contained metrics.
    pub fn into_metrics(self) -> Vec<M> {
        self.metrics
    }
}

#[cfg(test)]
mod tests {
    use relay_common::{ProjectId, ProjectKey};
    use relay_metrics::{Metric, MetricValue};
    use relay_quotas::QuotaScope;
    use smallvec::{smallvec, SmallVec};

    use super::*;

    fn run_limiter(
        metric_name: &str,
        has_profile: bool,
        categories: SmallVec<[DataCategory; 8]>,
        limit: u64,
    ) -> (usize, Vec<DataCategory>) {
        let (outcome_sink, mut rx) = Addr::custom();

        let metrics = vec![Metric {
            timestamp: UnixTimestamp::now(),
            name: metric_name.to_string(),
            tags: if has_profile {
                [("has_profile".to_string(), "true".to_string())].into()
            } else {
                Default::default()
            },
            value: MetricValue::Distribution(123.0),
        }];
        let quotas = vec![Quota {
            id: None,
            categories,
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(limit),
            window: None,
            reason_code: None,
        }];
        let metrics = match MetricsLimiter::create(
            metrics,
            quotas,
            Scoping {
                organization_id: 1,
                project_id: ProjectId::new(1),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
        ) {
            Ok(mut limiter) => {
                limiter.enforce_limits(Ok(&RateLimits::new()), outcome_sink);
                limiter.into_metrics()
            }
            Err(metrics) => metrics,
        };

        rx.close();

        let outcomes = (0..)
            .map(|_| rx.blocking_recv())
            .take_while(|o| o.is_some())
            .map(|o| o.unwrap().category);
        (metrics.len(), outcomes.collect())
    }

    #[test]
    fn test_rate_limits_applied() {
        assert_eq!(
            run_limiter(
                "d:transactions/duration@millisecond",
                false,
                smallvec!(DataCategory::Transaction),
                0,
            ),
            // generate outcomes for one category:
            (0, vec![DataCategory::Transaction])
        );
    }

    #[test]
    fn test_rate_limits_applied_with_profile() {
        assert_eq!(
            run_limiter(
                "d:transactions/duration@millisecond",
                true,
                smallvec!(DataCategory::Transaction),
                0,
            ),
            // generate outcomes for both categories:
            (0, vec![DataCategory::Profile, DataCategory::Transaction])
        );
    }

    #[test]
    fn test_rate_limits_still_room() {
        assert_eq!(
            run_limiter(
                "d:transactions/duration@millisecond",
                true,
                smallvec!(DataCategory::Transaction),
                1,
            ),
            (1, vec![])
        );
    }

    #[test]
    fn test_rate_limits_other_metric() {
        assert_eq!(
            run_limiter("foo", false, smallvec!(DataCategory::Transaction), 0,),
            (1, vec![])
        );
    }

    #[test]
    fn test_rate_limits_profile_indexed() {
        assert_eq!(
            run_limiter(
                "d:transactions/duration@millisecond",
                true,
                smallvec!(DataCategory::ProfileIndexed),
                0,
            ),
            (1, vec![])
        );
    }

    #[test]
    fn test_rate_limits_profile() {
        assert_eq!(
            run_limiter(
                "d:transactions/duration@millisecond",
                true,
                smallvec!(DataCategory::Profile),
                0,
            ),
            (1, vec![DataCategory::Profile]) // TODO: test tag deleted
        );
    }

    #[test]
    fn test_rate_limits_both() {
        assert_eq!(
            run_limiter(
                "d:transactions/duration@millisecond",
                true,
                smallvec![DataCategory::Profile, DataCategory::Transaction],
                0,
            ),
            (0, vec![DataCategory::Profile, DataCategory::Transaction]) // TODO: test tag deleted
        );
    }
}
