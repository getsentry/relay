use std::hash::Hash;

use relay_metrics::{Bucket, MetricNamespace, MetricResourceIdentifier};

use hash32::{FnvHasher, Hasher as _};

use crate::{OrganizationId, Result};

// TODO: give this a better name
/// Limiter responsible to enforce limits.
pub trait Limiter {
    /// TODO docs
    // TODO: figure out why I cant return impl Trait here, it's 1.74 already!
    fn check_cardinality_limits<I>(
        &self,
        organization: OrganizationId,
        entries: I,
    ) -> Result<Box<dyn Iterator<Item = Entry>>>
    where
        I: IntoIterator<Item = Entry>;
}

// TODO: can we remove this, everything is operated on a per org basis anyways
/// Scope for which cardinality will be tracked.
///
/// Currently this includes the organization and [namespace](`MetricNamespace`).
/// Eventually we want to track cardinality also on more fine grained namespaces,
/// e.g. on individual metric basis.
#[derive(Clone, Copy, Debug, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct Scope {
    /// The organization.
    pub organization: OrganizationId,
    /// The metric namespace, extracted from the metric bucket name.
    pub namespace: MetricNamespace,
}

/// TODO doc me and maybe rename me
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    /// Opaque entry Id, used to keep track of indices and buckets.
    pub id: EntryId,

    pub namespace: MetricNamespace,
    pub hash: u32,

    /// Implementation detail to optimize the implementation.
    ///
    /// Tracking the status directly on the entry means we do
    /// not have to temporarily allocated a second data structure
    /// to keep track of the status.
    status: EntryStatus,
}

// TODO: visibility, these types should be visible in the crate but not outside of it
/// Represents a unique Id for a bucket within one invocation
/// of the cardinality limiter.
#[derive(Clone, Copy, Debug)]
pub struct EntryId(usize);

/// TODO: docs
/// Status of an entry.
#[derive(Clone, Copy, Debug)]
enum EntryStatus {
    /// Entry is rejected.
    Rejected,
    /// Entry is accepted.
    Accepted,
}

impl Entry {
    /// Creates a new entry.
    pub fn new(id: EntryId, namespace: MetricNamespace, hash: u32) -> Self {
        Self {
            id,
            namespace,
            hash,
            status: EntryStatus::Rejected,
        }
    }

    /// Marks the entry as accepted.
    ///
    /// Implementation detail, not exposed outside of the crate.
    pub(crate) fn accept(&mut self) {
        self.status = EntryStatus::Accepted;
    }

    /// Marks the entry as rejected.
    ///
    /// Implementation detail, not exposed outside of the crate.
    pub(crate) fn reject(&mut self) {
        self.status = EntryStatus::Rejected;
    }

    /// Whether the entry is accepted.
    ///
    /// Implementation detail, not exposed outside of the crate.
    pub(crate) fn is_accepted(&self) -> bool {
        matches!(self.status, EntryStatus::Accepted)
    }
}

pub struct Config {
    /// Global sample rate.
    ///
    /// Sampling is done on an organizational level.
    pub sample_rate: f32,
}

/// Cardinality Limiter enforcing cardinality limits on buckets.
///
/// Delegates enforcement to a [`Limiter`], see for example the
pub struct CardinalityLimiter<T: Limiter> {
    limiter: T,
    config: Config,
}

impl<T: Limiter> CardinalityLimiter<T> {
    pub fn new(limiter: T, config: Config) -> Self {
        Self { limiter, config }
    }

    // TODO: replace with BucketsView after BucketsView PR is merged
    pub fn check_cardinality_limits<'a>(
        &'a self,
        organization: OrganizationId,
        buckets: &'a [Bucket],
    ) -> Result<Box<dyn Iterator<Item = &'a Bucket> + '_>> {
        if matches!(self.sample_by_org(organization), SampleDecision::Ignore) {
            return Ok(Box::new(vec![].into_iter()));
        }

        let entries = buckets
            .iter()
            .enumerate()
            .filter_map(|(id, bucket)| self.parse_bucket(EntryId(id), bucket));

        let accepted = self
            .limiter
            .check_cardinality_limits(organization, entries)?
            .map(|entry| &buckets[entry.id.0]);

        Ok(Box::new(accepted))
    }
}

impl<T: Limiter> CardinalityLimiter<T> {
    /// Currently to explore cardinality limiter implementation limits
    /// and performance we want to sample to gather information.
    ///
    /// Sampling is currently purely done on an organizational level,
    /// which may lead to an unequal amount of sampling (large orgs vs small orgs),
    /// but sampling on metrics themselfs is hard to achieve, since we need to know
    /// previously accepted buckets.
    ///
    /// TODO: verify sampling by bucket is feasible and works? The same hash should always
    /// be sampled and hence accepted.
    ///
    /// This should eventually removed if we have the cardinality fully in production.
    fn sample_by_org(&self, org: OrganizationId) -> SampleDecision {
        let keep = (org % 100) < (100.0 * self.config.sample_rate) as u64;

        if keep {
            SampleDecision::Keep
        } else {
            SampleDecision::Ignore
        }
    }

    fn parse_bucket(&self, id: EntryId, bucket: &Bucket) -> Option<Entry> {
        let mri = match MetricResourceIdentifier::parse(&bucket.name) {
            Ok(mri) if !matches!(mri.namespace, MetricNamespace::Unsupported) => {
                // TODO: we could actually implement limiting for unsupported,
                // we dont have to reject here.
                relay_log::trace!(
                    metric = bucket.name,
                    "rejecting metric with unsupported namespace"
                );
                return None;
            }
            Err(error) => {
                // TODO: what level do we log here?
                relay_log::trace!(
                    error = &error as &dyn std::error::Error,
                    metric = bucket.name,
                    "rejecting metric with invalid MRI"
                );
                // TODO: REJECT BUCKET
                // TODO: actually use a whitelist?
                return None;
            }
            Ok(mri) => mri,
        };

        Some(Entry::new(id, mri.namespace, cardinality_hash(bucket)))
    }
}

enum SampleDecision {
    Ignore,
    Keep,
}

fn cardinality_hash(bucket: &Bucket) -> u32 {
    let mut hasher = FnvHasher::default();
    bucket.name.hash(&mut hasher);
    bucket.tags.hash(&mut hasher);
    hasher.finish32()
}
