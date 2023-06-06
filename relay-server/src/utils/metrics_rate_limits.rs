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

    /// The number of transactions contributing to these metrics.
    transaction_count: usize,
}

impl<M: MetricsContainer, Q: AsRef<Vec<Quota>>> MetricsLimiter<M, Q> {
    /// Create a new limiter instance.
    ///
    /// Returns Ok if `metrics` contain transaction metrics, `metrics` otherwise.
    pub fn create(buckets: Vec<M>, quotas: Q, scoping: Scoping) -> Result<Self, Vec<M>> {
        let transaction_counts: Vec<_> = buckets
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
                    Some(count)
                } else {
                    // For any other metric in the transaction namespace, we check the limit with
                    // quantity=0 so transactions are not double counted against the quota.
                    Some(0)
                }
            })
            .collect();

        // Accumulate the total transaction count:
        let transaction_count = transaction_counts
            .iter()
            .fold(None, |acc, transaction_count| match transaction_count {
                Some(count) => Some(acc.unwrap_or(0) + count),
                None => acc,
            });

        if let Some(transaction_count) = transaction_count {
            let transaction_buckets = transaction_counts.iter().map(Option::is_some).collect();
            Ok(Self {
                metrics: buckets,
                quotas,
                scoping,
                transaction_buckets,
                transaction_count,
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
                outcome,
                event_id: None,
                remote_addr: None,
                category: DataCategory::Transaction,
                quantity: self.transaction_count as u32,
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
    pub fn into_metrics(self) -> Vec<M> {
        self.metrics
    }
}

#[cfg(test)]
mod tests {
    use relay_common::{ProjectId, ProjectKey};
    use relay_metrics::{Metric, MetricValue};
    use relay_quotas::{Quota, QuotaScope};
    use smallvec::smallvec;

    use super::*;

    #[test]
    fn profiles_limits_are_reported() {
        let metrics = vec![
            Metric {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                name: "d:transactions/duration@millisecond".to_string(),
                tags: Default::default(),
                value: MetricValue::Distribution(123.0),
            },
            Metric {
                // transaction without profile
                timestamp: UnixTimestamp::now(),
                name: "d:transactions/duration@millisecond".to_string(),
                tags: Default::default(),
                value: MetricValue::Distribution(123.0),
            },
            Metric {
                // transaction with profile
                timestamp: UnixTimestamp::now(),
                name: "d:transactions/duration@millisecond".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: MetricValue::Distribution(456.0),
            },
            Metric {
                // unrelated metric
                timestamp: UnixTimestamp::now(),
                name: "something_else".to_string(),
                tags: [("has_profile".to_string(), "true".to_string())].into(),
                value: MetricValue::Distribution(123.0),
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
            .map(|o| (o.category, o.quantity))
            .collect();

        assert_eq!(
            outcomes,
            vec![(DataCategory::Transaction, 3), (DataCategory::Profile, 1)]
        );
    }
}
