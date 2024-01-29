//! Relay Cardinality Limiter

use hashbrown::HashSet;
use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::project::ProjectId;
use relay_statsd::metric;

use crate::statsd::CardinalityLimiterTimers;
use crate::{CardinalityLimit, Error, OrganizationId, Result};

/// Data scoping information.
///
/// This structure holds information of all scopes required for attributing entries to limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Scoping {
    /// The organization id.
    pub organization_id: OrganizationId,
    /// The project id.
    pub project_id: ProjectId,
}

/// Accumulator of all cardinality limiter rejections.
pub trait Rejections {
    /// Called for ever [`Entry`] which was rejected from the [`Limiter`].
    fn reject(&mut self, entry_id: EntryId);
}

/// Limiter responsible to enforce limits.
pub trait Limiter {
    /// Verifies cardinality limits.
    ///
    /// Returns an iterator containing only accepted entries.
    fn check_cardinality_limits<E, R>(
        &self,
        scoping: Scoping,
        limits: &[CardinalityLimit],
        entries: E,
        rejections: &mut R,
    ) -> Result<()>
    where
        E: IntoIterator<Item = Entry>,
        R: Rejections;
}

/// Unit of operation for the cardinality limiter.
pub trait CardinalityItem {
    /// Transforms this item into a consistent hash.
    fn to_hash(&self) -> u32;

    /// Metric namespace of the item.
    ///
    /// If this method returns `None` the item is automatically rejected.
    fn namespace(&self) -> Option<MetricNamespace>;
}

/// A single entry to check cardinality for.
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    /// Opaque entry Id, used to keep track of indices and buckets.
    pub id: EntryId,

    /// Metric namespace to which the cardinality limit is scoped.
    pub namespace: MetricNamespace,
    /// Hash of the metric name and tags.
    pub hash: u32,
}

/// Represents a unique Id for a bucket within one invocation
/// of the cardinality limiter.
///
/// Opaque data structure used by [`CardinalityLimiter`] to track
/// which buckets have been accepted and rejected.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct EntryId(pub usize);

impl Entry {
    /// Creates a new entry.
    pub fn new(id: EntryId, namespace: MetricNamespace, hash: u32) -> Self {
        Self {
            id,
            namespace,
            hash,
        }
    }
}

/// Cardinality Limiter enforcing cardinality limits on buckets.
///
/// Delegates enforcement to a [`Limiter`].
pub struct CardinalityLimiter<T: Limiter> {
    limiter: T,
}

impl<T: Limiter> CardinalityLimiter<T> {
    /// Creates a new cardinality limiter.
    pub fn new(limiter: T) -> Self {
        Self { limiter }
    }

    /// Checks cardinality limits of a list of buckets.
    ///
    /// Returns an iterator of all buckets that have been accepted.
    pub fn check_cardinality_limits<I: CardinalityItem>(
        &self,
        scoping: Scoping,
        limits: &[CardinalityLimit],
        items: Vec<I>,
    ) -> Result<CardinalityLimits<I>, (Vec<I>, Error)> {
        metric!(timer(CardinalityLimiterTimers::CardinalityLimiter), {
            let entries = items.iter().enumerate().filter_map(|(id, item)| {
                Some(Entry::new(EntryId(id), item.namespace()?, item.to_hash()))
            });

            let mut rejections = RejectedIds::default();
            if let Err(err) =
                self.limiter
                    .check_cardinality_limits(scoping, limits, entries, &mut rejections)
            {
                return Err((items, err));
            }

            if !rejections.0.is_empty() {
                relay_log::debug!(
                    scoping = ?scoping,
                    "rejected {} metrics due to cardinality limit",
                    rejections.0.len(),
                );
            }

            Ok(CardinalityLimits::new(items, rejections))
        })
    }
}

/// Internal outcome accumulator tracking the raw value from an [`EntryId`].
///
/// The result can be used directly by [`CardinalityLimits`].
#[derive(Debug, Default)]
struct RejectedIds(HashSet<usize>);

impl Rejections for RejectedIds {
    #[inline(always)]
    fn reject(&mut self, entry_id: EntryId) {
        self.0.insert(entry_id.0);
    }
}

/// Result of [`CardinalityLimiter::check_cardinality_limits`].
#[derive(Debug)]
pub struct CardinalityLimits<T> {
    source: Vec<T>,
    rejections: HashSet<usize>,
}

impl<T> CardinalityLimits<T> {
    fn new(source: Vec<T>, rejections: RejectedIds) -> Self {
        Self {
            source,
            rejections: rejections.0,
        }
    }

    /// Returns an iterator yielding only rejected items.
    pub fn rejected(&self) -> impl Iterator<Item = &T> {
        self.rejections.iter().filter_map(|&i| self.source.get(i))
    }

    /// Consumes the result and returns an iterator over all accepted items.
    pub fn into_accepted(self) -> Vec<T> {
        if self.rejections.is_empty() {
            return self.source;
        } else if self.source.len() == self.rejections.len() {
            return Vec::new();
        }

        self.source
            .into_iter()
            .enumerate()
            .filter_map(|(i, t)| {
                if self.rejections.contains(&i) {
                    None
                } else {
                    Some(t)
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::{CardinalityScope, SlidingWindow};

    use super::*;

    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
    struct Item {
        hash: u32,
        namespace: Option<MetricNamespace>,
    }

    impl Item {
        fn new(hash: u32, namespace: impl Into<Option<MetricNamespace>>) -> Self {
            Self {
                hash,
                namespace: namespace.into(),
            }
        }
    }

    impl CardinalityItem for Item {
        fn to_hash(&self) -> u32 {
            self.hash
        }

        fn namespace(&self) -> Option<MetricNamespace> {
            self.namespace
        }
    }

    fn build_limits() -> [CardinalityLimit; 1] {
        [CardinalityLimit {
            id: "limit".to_owned(),
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 10_000,
            scope: CardinalityScope::Organization,
            namespace: None,
        }]
    }

    fn build_scoping() -> Scoping {
        Scoping {
            organization_id: 1,
            project_id: ProjectId::new(1),
        }
    }

    #[test]
    fn test_accepted() {
        // Workaround for windows which requires an absurd amount of type annotations here.
        fn assert_rejected(
            limits: &CardinalityLimits<char>,
            expected: impl IntoIterator<Item = char>,
        ) {
            assert_eq!(
                limits.rejected().copied().collect::<HashSet<char>>(),
                expected.into_iter().collect::<HashSet<char>>(),
            );
        }

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashSet::from([0, 1, 3]),
        };
        assert_rejected(&limits, ['a', 'b', 'd']);
        assert_eq!(limits.into_accepted(), vec!['c', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashSet::from([]),
        };
        assert_rejected(&limits, []);
        assert_eq!(limits.into_accepted(), vec!['a', 'b', 'c', 'd', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashSet::from([0, 1, 2, 3, 4]),
        };
        assert_rejected(&limits, ['a', 'b', 'c', 'd', 'e']);
        assert!(limits.into_accepted().is_empty());
    }

    #[test]
    fn test_limiter_reject_all() {
        struct RejectAllLimiter;

        impl Limiter for RejectAllLimiter {
            fn check_cardinality_limits<I, T>(
                &self,
                _scoping: Scoping,
                _limits: &[CardinalityLimit],
                entries: I,
                outcomes: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry>,
                T: Rejections,
            {
                for entry in entries {
                    outcomes.reject(entry.id);
                }

                Ok(())
            }
        }

        let limiter = CardinalityLimiter::new(RejectAllLimiter);

        let result = limiter
            .check_cardinality_limits(
                build_scoping(),
                &build_limits(),
                vec![
                    Item::new(0, MetricNamespace::Transactions),
                    Item::new(1, MetricNamespace::Transactions),
                ],
            )
            .unwrap();

        assert!(result.into_accepted().is_empty());
    }

    #[test]
    fn test_limiter_accept_all() {
        struct AcceptAllLimiter;

        impl Limiter for AcceptAllLimiter {
            fn check_cardinality_limits<I, T>(
                &self,
                _scoping: Scoping,
                _limits: &[CardinalityLimit],
                _entries: I,
                _outcomes: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry>,
                T: Rejections,
            {
                Ok(())
            }
        }

        let limiter = CardinalityLimiter::new(AcceptAllLimiter);

        let items = vec![
            Item::new(0, MetricNamespace::Transactions),
            Item::new(1, MetricNamespace::Spans),
        ];
        let result = limiter
            .check_cardinality_limits(build_scoping(), &build_limits(), items.clone())
            .unwrap();

        assert_eq!(result.into_accepted(), items);
    }

    #[test]
    fn test_limiter_accept_odd_reject_even() {
        struct RejectEvenLimiter;

        impl Limiter for RejectEvenLimiter {
            fn check_cardinality_limits<I, T>(
                &self,
                scoping: Scoping,
                limits: &[CardinalityLimit],
                entries: I,
                outcomes: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry>,
                T: Rejections,
            {
                assert_eq!(scoping, build_scoping());
                assert_eq!(limits, &build_limits());

                for entry in entries {
                    if entry.id.0 % 2 == 0 {
                        outcomes.reject(entry.id);
                    }
                }

                Ok(())
            }
        }

        let limiter = CardinalityLimiter::new(RejectEvenLimiter);

        let items = vec![
            Item::new(0, MetricNamespace::Sessions),
            Item::new(1, MetricNamespace::Transactions),
            Item::new(2, MetricNamespace::Spans),
            Item::new(3, MetricNamespace::Custom),
            Item::new(4, MetricNamespace::Custom),
            Item::new(5, MetricNamespace::Transactions),
            Item::new(6, MetricNamespace::Spans),
        ];
        let accepted = limiter
            .check_cardinality_limits(build_scoping(), &build_limits(), items)
            .unwrap()
            .into_accepted();

        assert_eq!(
            accepted,
            vec![
                Item::new(1, MetricNamespace::Transactions),
                Item::new(3, MetricNamespace::Custom),
                Item::new(5, MetricNamespace::Transactions),
            ]
        );
    }
}
