//! Relay Cardinality Limiter

use std::collections::BTreeSet;
use std::fmt::{Debug, Display};
use std::hash::Hash;

use relay_statsd::metric;

use crate::statsd::CardinalityLimiterTimers;
use crate::{Error, OrganizationId, Result};

/// Limiter responsible to enforce limits.
pub trait Limiter {
    /// Verifies cardinality limits.
    ///
    /// Returns an iterator containing only accepted entries.
    fn check_cardinality_limits<I, T>(
        &self,
        organization: OrganizationId,
        entries: I,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Rejection>>>
    where
        I: IntoIterator<Item = Entry<T>>,
        T: CardinalityScope;
}

/// The scope of a [`CardinalityItem`].
pub trait CardinalityScope: Debug + Display + Hash + PartialEq + Eq + 'static {}

impl<T> CardinalityScope for T where T: Debug + Display + Hash + PartialEq + Eq + 'static {}

/// Unit of operation for the cardinality limiter.
pub trait CardinalityItem {
    /// The cardinality limit scope.
    type Scope: CardinalityScope;

    /// Transforms this item into a consistent hash.
    fn to_hash(&self) -> u32;

    /// Extracts a scope to check cardinality limits for from this item.
    fn to_scope(&self) -> Option<Self::Scope>;
}

/// A single entry to check cardinality for.
#[derive(Clone, Copy, Debug)]
pub struct Entry<T: CardinalityScope> {
    /// Opaque entry Id, used to keep track of indices and buckets.
    pub id: EntryId,

    /// Cardinality scope.
    pub scope: T,
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

impl<T: CardinalityScope> Entry<T> {
    /// Creates a new entry.
    pub fn new(id: EntryId, scope: T, hash: u32) -> Self {
        Self { id, scope, hash }
    }
}

/// Represents a from the cardinality limiter rejected [`Entry`].
#[derive(Debug)]
pub struct Rejection {
    pub(crate) id: EntryId,
}

impl Rejection {
    /// Creates a new rejection.
    pub fn new(id: EntryId) -> Self {
        Self { id }
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
    /// Creates a new cardinality limiter.
    pub fn new(limiter: T, config: Config) -> Self {
        Self { limiter, config }
    }

    /// Checks cardinality limits of a list of buckets.
    ///
    /// Returns an iterator of all buckets that have been accepted.
    pub fn check_cardinality_limits<I: CardinalityItem>(
        &self,
        organization: OrganizationId,
        items: Vec<I>,
    ) -> Result<CardinalityLimits<I>, (Vec<I>, Error)> {
        metric!(timer(CardinalityLimiterTimers::CardinalityLimiter), {
            let entries = items.iter().enumerate().filter_map(|(id, item)| {
                Some(Entry::new(EntryId(id), item.to_scope()?, item.to_hash()))
            });

            let result = self.limiter.check_cardinality_limits(
                organization,
                entries,
                self.config.cardinality_limit,
            );

            let rejections = match result {
                Ok(rejections) => rejections.map(|rejection| rejection.id.0).collect(),
                Err(err) => return Err((items, err)),
            };

            Ok(CardinalityLimits {
                source: items,
                rejections,
            })
        })
    }
}

/// Result of [`CardinalityLimiter::check_cardinality_limits`].
#[derive(Debug)]
pub struct CardinalityLimits<T> {
    source: Vec<T>,
    rejections: BTreeSet<usize>,
}

impl<T> CardinalityLimits<T> {
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
    use super::*;

    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
    struct Item {
        hash: u32,
        scope: Option<&'static str>,
    }

    impl Item {
        fn new(hash: u32, scope: impl Into<Option<&'static str>>) -> Self {
            Self {
                hash,
                scope: scope.into(),
            }
        }
    }

    impl CardinalityItem for Item {
        type Scope = &'static str;

        fn to_hash(&self) -> u32 {
            self.hash
        }

        fn to_scope(&self) -> Option<Self::Scope> {
            self.scope
        }
    }

    #[test]
    fn test_accepted() {
        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: BTreeSet::from([0, 1, 3]),
        };
        dbg!(limits.rejected().collect::<Vec<_>>());
        assert!(limits.rejected().eq(['a', 'b', 'd'].iter()));
        assert_eq!(limits.into_accepted(), vec!['c', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: BTreeSet::from([]),
        };
        assert!(limits.rejected().eq([].iter()));
        assert_eq!(limits.into_accepted(), vec!['a', 'b', 'c', 'd', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: BTreeSet::from([0, 1, 2, 3, 4]),
        };
        assert!(limits.rejected().eq(['a', 'b', 'c', 'd', 'e'].iter()));
        assert!(limits.into_accepted().is_empty());
    }

    #[test]
    fn test_limiter_reject_all() {
        struct RejectAllLimiter;

        impl Limiter for RejectAllLimiter {
            fn check_cardinality_limits<I, T>(
                &self,
                _organization: OrganizationId,
                entries: I,
                limit: usize,
            ) -> Result<Box<dyn Iterator<Item = Rejection>>>
            where
                I: IntoIterator<Item = Entry<T>>,
                T: CardinalityScope,
            {
                assert_eq!(limit, 1000);
                let rejections = entries
                    .into_iter()
                    .map(|entry| Rejection::new(entry.id))
                    .collect::<Vec<_>>();

                Ok(Box::new(rejections.into_iter()))
            }
        }

        let limiter = CardinalityLimiter::new(
            RejectAllLimiter,
            Config {
                cardinality_limit: 1000,
            },
        );

        let result = limiter
            .check_cardinality_limits(1, vec![Item::new(0, "a"), Item::new(1, "b")])
            .unwrap();

        assert!(result.into_accepted().is_empty());
    }

    #[test]
    fn test_limiter_accept_all() {
        struct AcceptAllLimiter;

        impl Limiter for AcceptAllLimiter {
            fn check_cardinality_limits<I, T>(
                &self,
                _organization: OrganizationId,
                _entries: I,
                limit: usize,
            ) -> Result<Box<dyn Iterator<Item = Rejection>>>
            where
                I: IntoIterator<Item = Entry<T>>,
                T: CardinalityScope,
            {
                assert_eq!(limit, 100);
                Ok(Box::new(Vec::new().into_iter()))
            }
        }

        let limiter = CardinalityLimiter::new(
            AcceptAllLimiter,
            Config {
                cardinality_limit: 100,
            },
        );

        let items = vec![Item::new(0, "a"), Item::new(1, "b")];
        let result = limiter.check_cardinality_limits(1, items.clone()).unwrap();

        assert_eq!(result.into_accepted(), items);
    }

    #[test]
    fn test_limiter_accept_odd_reject_even() {
        struct RejectEvenLimiter;

        impl Limiter for RejectEvenLimiter {
            fn check_cardinality_limits<I, T>(
                &self,
                organization: OrganizationId,
                entries: I,
                limit: usize,
            ) -> Result<Box<dyn Iterator<Item = Rejection>>>
            where
                I: IntoIterator<Item = Entry<T>>,
                T: CardinalityScope,
            {
                assert_eq!(limit, 100);
                assert_eq!(organization, 3);

                let rejections = entries
                    .into_iter()
                    .filter_map(|entry| (entry.id.0 % 2 == 0).then_some(Rejection::new(entry.id)))
                    .collect::<Vec<_>>();

                Ok(Box::new(rejections.into_iter()))
            }
        }

        let limiter = CardinalityLimiter::new(
            RejectEvenLimiter,
            Config {
                cardinality_limit: 100,
            },
        );

        let items = vec![
            Item::new(0, "a"),
            Item::new(1, "b"),
            Item::new(2, "c"),
            Item::new(3, "d"),
            Item::new(4, "e"),
            Item::new(5, "f"),
            Item::new(6, "g"),
        ];
        let accepted = limiter
            .check_cardinality_limits(3, items)
            .unwrap()
            .into_accepted();

        assert_eq!(
            accepted,
            vec![Item::new(1, "b"), Item::new(3, "d"), Item::new(5, "f"),]
        );
    }
}
