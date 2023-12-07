use std::collections::BTreeMap;
use std::hash::Hash;

use relay_metrics::{Bucket, MetricNamespace, MetricResourceIdentifier, UnixTimestamp};

use hash32::{FnvHasher, Hasher as _};

use crate::{OrganizationId, Result};

/// Limiter responsible to enforce limits.
pub trait Limiter {
    /// Verifies cardinality limits.
    ///
    /// Returns an iterator containing only accepted entries.
    fn check_cardinality_limits<I>(
        &self,
        organization: OrganizationId,
        entries: I,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Entry>>>
    where
        I: IntoIterator<Item = Entry>;
}

/// A single entry to check cardinality for.
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    /// Opaque entry Id, used to keep track of indices and buckets.
    pub id: EntryId,

    /// Metric namespace.
    pub namespace: MetricNamespace,
    /// Hash of the metric name and tags.
    pub hash: u32,

    /// Implementation detail to optimize the implementation.
    ///
    /// Tracking the status directly on the entry means we do
    /// not have to temporarily allocate a second data structure
    /// to keep track of the status.
    status: EntryStatus,
}

/// Represents a unique Id for a bucket within one invocation
/// of the cardinality limiter.
///
/// Opaque data structure used by [`CardinalityLimiter`] to track
/// which buckets have been accepted and rejected.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct EntryId(pub(crate) usize);

/// Status wether an entry/bucket is accepted or rejected by the cardinality limiter.
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

/// CardinalityLimiter configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// The cardinality limit to enforce.
    pub cardinality_limit: usize,
}

/// Cardinality Limiter enforcing cardinality limits on buckets.
///
/// Delegates enforcement to a [`Limiter`].
pub struct CardinalityLimiter<T: Limiter> {
    limiter: T,
    config: Config,
}

impl<T: Limiter> CardinalityLimiter<T> {
    pub fn new(limiter: T, config: Config) -> Self {
        Self { limiter, config }
    }

    /// Checks cardinality limits of a list of buckets.
    ///
    /// Returns an iterator of all buckets that have been accepted.
    pub fn check_cardinality_limits(
        &self,
        organization: OrganizationId,
        mut buckets: Vec<Bucket>,
    ) -> Result<impl Iterator<Item = Bucket>> {
        let entries = buckets
            .iter()
            .enumerate()
            .filter_map(|(id, bucket)| self.parse_bucket(EntryId(id), bucket));

        const EMPTY: Bucket = Bucket {
            name: String::new(),
            timestamp: UnixTimestamp::from_secs(0),
            tags: BTreeMap::new(),
            value: relay_metrics::BucketValue::Counter(0.0),
            width: 0,
        };

        let accepted = self
            .limiter
            .check_cardinality_limits(organization, entries, self.config.cardinality_limit)?
            .map(move |entry| std::mem::replace(&mut buckets[entry.id.0], EMPTY));

        Ok(accepted)
    }
}

impl<T: Limiter> CardinalityLimiter<T> {
    fn parse_bucket(&self, id: EntryId, bucket: &Bucket) -> Option<Entry> {
        // Parsing in practice will never fail here, all buckets have been validated before.
        let mri = match MetricResourceIdentifier::parse(&bucket.name) {
            Err(error) => {
                relay_log::debug!(
                    error = &error as &dyn std::error::Error,
                    metric = bucket.name,
                    "rejecting metric with invalid MRI"
                );
                return None;
            }
            Ok(mri) => mri,
        };

        Some(Entry::new(id, mri.namespace, cardinality_hash(bucket)))
    }
}

/// Hashes a bucket to use for the cardinality limiter.
fn cardinality_hash(bucket: &Bucket) -> u32 {
    let mut hasher = FnvHasher::default();
    bucket.name.hash(&mut hasher);
    bucket.tags.hash(&mut hasher);
    hasher.finish32()
}
