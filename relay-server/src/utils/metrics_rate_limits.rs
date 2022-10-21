//! Quota and rate limiting helpers for metrics buckets.

use relay_common::{DataCategory, UnixTimestamp};
use relay_metrics::{Bucket, MetricNamespace, MetricResourceIdentifier};
use relay_quotas::{ItemScoping, Quota, RateLimits, Scoping};

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};

/// Holds metrics buckets with some information about their contents.
#[derive(Debug)]
pub struct BucketLimiter<Q: AsRef<Vec<Quota>> = Vec<Quota>> {
    /// A list of metrics buckets.
    buckets: Vec<Bucket>,

    /// The quotas set on the current project.
    quotas: Q,

    /// Project information.
    scoping: Scoping,

    /// Binary index of buckets in the transaction namespace (used to retain).
    transaction_buckets: Vec<bool>,

    /// The number of transactions contributing to these buckets.
    transaction_count: usize,
}

impl<Q: AsRef<Vec<Quota>>> BucketLimiter<Q> {
    /// Create a new limiter instance.
    ///
    /// Returns Ok if `buckets` contain transaction metrics, `buckets` otherwise.
    pub fn create(buckets: Vec<Bucket>, quotas: Q, scoping: Scoping) -> Result<Self, Vec<Bucket>> {
        let transaction_counts: Vec<_> = buckets
            .iter()
            .map(|bucket| {
                let mri = match MetricResourceIdentifier::parse(bucket.name.as_str()) {
                    Ok(mri) => mri,
                    Err(_) => {
                        relay_log::error!("Invalid MRI: {}", bucket.name);
                        return None;
                    }
                };

                // Keep all metrics that are not transaction related:
                if mri.namespace != MetricNamespace::Transactions {
                    return None;
                }

                if mri.name == "duration" {
                    // The "duration" metric is extracted exactly once for every processed transaction,
                    // so we can use it to count the number of transactions.
                    let count = bucket.value.len();
                    Some(count)
                } else {
                    // For any other metric in the transaction namespace, we check the limit with quantity=0
                    // so transactions are not double counted against the quota.
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
                buckets,
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

    fn drop_with_outcome(&mut self, outcome: Outcome) {
        // Drop transaction buckets:
        let buckets = std::mem::take(&mut self.buckets);

        self.buckets = buckets
            .into_iter()
            .zip(self.transaction_buckets.iter())
            .filter_map(|(bucket, is_transaction_bucket)| {
                (!is_transaction_bucket).then_some(bucket)
            })
            .collect();

        // Track outcome for the processed transactions we dropped here:
        if self.transaction_count > 0 {
            TrackOutcome::from_registry().send(TrackOutcome {
                timestamp: UnixTimestamp::now().as_datetime(), // as good as any timestamp
                scoping: self.scoping,
                outcome,
                event_id: None,
                remote_addr: None,
                category: DataCategory::TransactionProcessed,
                quantity: self.transaction_count as u32,
            });
        }
    }

    // Drop transaction-related buckets and create outcomes for any active rate limits.
    //
    // If rate limits could not be checked for some reason, pass an `Err` to this function.
    // In this case, transaction-related metrics buckets will also be dropped, and an "internal"
    // outcome is generated.
    //
    // Returns true if any buckets were dropped.
    pub fn enforce_limits(&mut self, rate_limits: Result<&RateLimits, ()>) -> bool {
        let mut dropped_stuff = false;
        match rate_limits {
            Ok(rate_limits) => {
                let item_scoping = ItemScoping {
                    category: DataCategory::TransactionProcessed,
                    scoping: &self.scoping,
                };
                let applied_rate_limits =
                    rate_limits.check_with_quotas(self.quotas.as_ref(), item_scoping);

                // If a rate limit is active, discard transaction buckets.
                if let Some(limit) = applied_rate_limits.longest() {
                    self.drop_with_outcome(Outcome::RateLimited(limit.reason_code.clone()));
                    dropped_stuff = true;
                }
            }
            Err(_) => {
                // Error from rate limiter, drop transaction buckets.
                self.drop_with_outcome(Outcome::Invalid(DiscardReason::Internal));
                dropped_stuff = true;
            }
        };

        dropped_stuff
    }

    /// Consume this struct and return its buckets.
    pub fn into_buckets(self) -> Vec<Bucket> {
        self.buckets
    }
}
