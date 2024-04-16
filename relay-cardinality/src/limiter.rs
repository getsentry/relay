//! Relay Cardinality Limiter

use std::collections::BTreeMap;

use hashbrown::HashSet;
use relay_base_schema::metrics::{MetricName, MetricNamespace};
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

/// Cardinality report for a specific limit.
///
/// Contains scoping information for the enforced limit and the current cardinality.
/// If all of the scoping information is `None`, the limit is a global cardinality limit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CardinalityReport {
    /// Organization id for which the cardinality limit was applied.
    ///
    /// Only available if the the limit was at least scoped to
    /// [`CardinalityScope::Organization`](crate::CardinalityScope::Organization).
    pub organization_id: Option<OrganizationId>,
    /// Project id for which the cardinality limit was applied.
    ///
    /// Only available if the the limit was at least scoped to
    /// [`CardinalityScope::Project`](crate::CardinalityScope::Project).
    pub project_id: Option<ProjectId>,
    /// Project id for which the cardinality limit was applied.
    ///
    /// Only available if the the limit was at least scoped to
    /// [`CardinalityScope::Name`](crate::CardinalityScope::Name).
    pub name: Option<MetricName>,

    /// The current cardinality.
    pub cardinality: u32,
}

/// Accumulator of all cardinality limiter decisions.
pub trait Reporter<'a> {
    /// Called for ever [`Entry`] which was rejected from the [`Limiter`].
    fn reject(&mut self, limit: &'a CardinalityLimit, entry_id: EntryId);

    /// Called for every individual limit applied.
    ///
    /// The callback can be called multiple times with different reports
    /// for the same `limit` or not at all if there was no change in cardinality.
    ///
    /// For example, with a name scoped limit can be called once for every
    /// metric name matching the limit.
    fn report_cardinality(&mut self, limit: &'a CardinalityLimit, report: CardinalityReport);
}

/// Limiter responsible to enforce limits.
pub trait Limiter {
    /// Verifies cardinality limits.
    ///
    /// Returns an iterator containing only accepted entries.
    fn check_cardinality_limits<'a, 'b, E, R>(
        &self,
        scoping: Scoping,
        limits: &'a [CardinalityLimit],
        entries: E,
        reporter: &mut R,
    ) -> Result<()>
    where
        E: IntoIterator<Item = Entry<'b>>,
        R: Reporter<'a>;
}

/// Unit of operation for the cardinality limiter.
pub trait CardinalityItem {
    /// Transforms this item into a consistent hash.
    fn to_hash(&self) -> u32;

    /// Metric namespace of the item.
    ///
    /// If this method returns `None` the item is automatically rejected.
    fn namespace(&self) -> Option<MetricNamespace>;

    /// Name of the item.
    fn name(&self) -> &MetricName;
}

/// A single entry to check cardinality for.
#[derive(Clone, Copy, Debug)]
pub struct Entry<'a> {
    /// Opaque entry Id, used to keep track of indices and buckets.
    pub id: EntryId,

    /// Metric namespace to which the cardinality limit can be scoped.
    pub namespace: MetricNamespace,
    /// Name to which the cardinality limit can be scoped.
    pub name: &'a MetricName,
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

impl<'a> Entry<'a> {
    /// Creates a new entry.
    pub fn new(id: EntryId, namespace: MetricNamespace, name: &'a MetricName, hash: u32) -> Self {
        Self {
            id,
            namespace,
            name,
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
    pub fn check_cardinality_limits<'a, I: CardinalityItem>(
        &self,
        scoping: Scoping,
        limits: &'a [CardinalityLimit],
        items: Vec<I>,
    ) -> Result<CardinalityLimits<'a, I>, (Vec<I>, Error)> {
        if limits.is_empty() {
            return Ok(CardinalityLimits::new(items, Default::default()));
        }

        metric!(timer(CardinalityLimiterTimers::CardinalityLimiter), {
            let entries = items.iter().enumerate().filter_map(|(id, item)| {
                Some(Entry::new(
                    EntryId(id),
                    item.namespace()?,
                    item.name(),
                    item.to_hash(),
                ))
            });

            let mut rejections = DefaultReporter::default();
            if let Err(err) =
                self.limiter
                    .check_cardinality_limits(scoping, limits, entries, &mut rejections)
            {
                return Err((items, err));
            }

            if !rejections.entries.is_empty() {
                relay_log::debug!(
                    scoping = ?scoping,
                    "rejected {} metrics due to cardinality limit",
                    rejections.entries.len(),
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
struct DefaultReporter<'a> {
    exceeded_limits: HashSet<&'a CardinalityLimit>,
    entries: HashSet<usize>,
    reports: BTreeMap<&'a CardinalityLimit, Vec<CardinalityReport>>,
}

impl<'a> Reporter<'a> for DefaultReporter<'a> {
    #[inline(always)]
    fn reject(&mut self, limit: &'a CardinalityLimit, entry_id: EntryId) {
        self.exceeded_limits.insert(limit);
        if !limit.passive {
            self.entries.insert(entry_id.0);
        }
    }

    #[inline(always)]
    fn report_cardinality(&mut self, limit: &'a CardinalityLimit, report: CardinalityReport) {
        if !limit.report {
            return;
        }
        self.reports.entry(limit).or_default().push(report);
    }
}

/// Split of the original source containing accepted and rejected source elements.
#[derive(Debug)]
pub struct CardinalityLimitsSplit<T> {
    /// The list of accepted elements of the source.
    pub accepted: Vec<T>,
    /// The list of rejected elements of the source.
    pub rejected: Vec<T>,
}

/// Result of [`CardinalityLimiter::check_cardinality_limits`].
#[derive(Debug)]
pub struct CardinalityLimits<'a, T> {
    /// The source.
    source: Vec<T>,
    /// List of rejected item indices pointing into `source`.
    rejections: HashSet<usize>,
    /// All non-passive exceeded limits.
    exceeded_limits: HashSet<&'a CardinalityLimit>,
    /// Generated cardinality reports.
    reports: BTreeMap<&'a CardinalityLimit, Vec<CardinalityReport>>,
}

impl<'a, T> CardinalityLimits<'a, T> {
    fn new(source: Vec<T>, reporter: DefaultReporter<'a>) -> Self {
        Self {
            source,
            rejections: reporter.entries,
            exceeded_limits: reporter.exceeded_limits,
            reports: reporter.reports,
        }
    }

    /// Returns `true` if any items have been rejected.
    pub fn has_rejections(&self) -> bool {
        !self.rejections.is_empty()
    }

    /// Returns all id's of cardinality limits which were exceeded.
    ///
    /// This includes passive limits.
    pub fn exceeded_limits(&self) -> &HashSet<&'a CardinalityLimit> {
        &self.exceeded_limits
    }

    /// Returns all cardinality reports grouped by the cardinality limit.
    ///
    /// Cardinality reports are generated for all cardinality limits with reporting enabled
    /// and the current cardinality changed.
    pub fn cardinality_reports(&self) -> &BTreeMap<&'a CardinalityLimit, Vec<CardinalityReport>> {
        &self.reports
    }

    /// Recovers the original list of items passed to the cardinality limiter.
    pub fn into_source(self) -> Vec<T> {
        self.source
    }

    /// Returns an iterator yielding only rejected items.
    pub fn rejected(&self) -> impl Iterator<Item = &T> {
        self.rejections.iter().filter_map(|&i| self.source.get(i))
    }

    /// Consumes the result and returns one iterator for all accepted items and one for all rejected
    /// items.
    pub fn into_split(self) -> CardinalityLimitsSplit<T> {
        if self.rejections.is_empty() {
            return CardinalityLimitsSplit {
                accepted: self.source,
                rejected: Vec::new(),
            };
        } else if self.source.len() == self.rejections.len() {
            return CardinalityLimitsSplit {
                accepted: Vec::new(),
                rejected: self.source,
            };
        }

        let mut accepted = vec![];
        let mut rejected = vec![];
        self.source.into_iter().enumerate().for_each(|(i, t)| {
            if self.rejections.contains(&i) {
                rejected.push(t);
            } else {
                accepted.push(t);
            }
        });

        CardinalityLimitsSplit { accepted, rejected }
    }
}

#[cfg(test)]
mod tests {
    use crate::{CardinalityScope, SlidingWindow};

    use super::*;

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    struct Item {
        hash: u32,
        namespace: Option<MetricNamespace>,
        name: MetricName,
    }

    impl Item {
        fn new(hash: u32, namespace: impl Into<Option<MetricNamespace>>) -> Self {
            Self {
                hash,
                namespace: namespace.into(),
                name: MetricName::from("foobar"),
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

        fn name(&self) -> &MetricName {
            &self.name
        }
    }

    fn build_limits() -> [CardinalityLimit; 1] {
        [CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
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
            exceeded_limits: HashSet::new(),
            reports: BTreeMap::new(),
        };
        assert_rejected(&limits, ['a', 'b', 'd']);
        assert!(limits.has_rejections());
        assert_eq!(limits.into_split().accepted, vec!['c', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashSet::from([]),
            exceeded_limits: HashSet::new(),
            reports: BTreeMap::new(),
        };
        assert_rejected(&limits, []);
        assert!(!limits.has_rejections());
        assert_eq!(limits.into_split().accepted, vec!['a', 'b', 'c', 'd', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashSet::from([0, 1, 2, 3, 4]),
            exceeded_limits: HashSet::new(),
            reports: BTreeMap::new(),
        };
        assert!(limits.has_rejections());
        assert_rejected(&limits, ['a', 'b', 'c', 'd', 'e']);
        assert!(limits.into_split().accepted.is_empty());
    }

    #[test]
    fn test_limiter_reject_all() {
        struct RejectAllLimiter;

        impl Limiter for RejectAllLimiter {
            fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                _scoping: Scoping,
                limits: &'a [CardinalityLimit],
                entries: I,
                rejections: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>>,
                T: Reporter<'a>,
            {
                for entry in entries {
                    rejections.reject(&limits[0], entry.id);
                }

                Ok(())
            }
        }

        let limiter = CardinalityLimiter::new(RejectAllLimiter);

        let limits = build_limits();
        let result = limiter
            .check_cardinality_limits(
                build_scoping(),
                &limits,
                vec![
                    Item::new(0, MetricNamespace::Transactions),
                    Item::new(1, MetricNamespace::Transactions),
                ],
            )
            .unwrap();

        assert_eq!(result.exceeded_limits(), &HashSet::from([&limits[0]]));
        assert!(result.into_split().accepted.is_empty());
    }

    #[test]
    fn test_limiter_accept_all() {
        struct AcceptAllLimiter;

        impl Limiter for AcceptAllLimiter {
            fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                _scoping: Scoping,
                _limits: &'a [CardinalityLimit],
                _entries: I,
                _reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>>,
                T: Reporter<'a>,
            {
                Ok(())
            }
        }

        let limiter = CardinalityLimiter::new(AcceptAllLimiter);

        let items = vec![
            Item::new(0, MetricNamespace::Transactions),
            Item::new(1, MetricNamespace::Spans),
        ];
        let limits = build_limits();
        let result = limiter
            .check_cardinality_limits(build_scoping(), &limits, items.clone())
            .unwrap();

        assert_eq!(result.into_split().accepted, items);
    }

    #[test]
    fn test_limiter_accept_odd_reject_even() {
        struct RejectEvenLimiter;

        impl Limiter for RejectEvenLimiter {
            fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                scoping: Scoping,
                limits: &'a [CardinalityLimit],
                entries: I,
                reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>>,
                T: Reporter<'a>,
            {
                assert_eq!(scoping, build_scoping());
                assert_eq!(limits, &build_limits());

                for entry in entries {
                    if entry.id.0 % 2 == 0 {
                        reporter.reject(&limits[0], entry.id);
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
            .into_split()
            .accepted;

        assert_eq!(
            accepted,
            vec![
                Item::new(1, MetricNamespace::Transactions),
                Item::new(3, MetricNamespace::Custom),
                Item::new(5, MetricNamespace::Transactions),
            ]
        );
    }

    #[test]
    fn test_limiter_passive() {
        struct RejectLimits;

        impl Limiter for RejectLimits {
            fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                _scoping: Scoping,
                limits: &'a [CardinalityLimit],
                entries: I,
                reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>>,
                T: Reporter<'a>,
            {
                for entry in entries {
                    reporter.reject(&limits[entry.id.0 % limits.len()], entry.id);
                }
                Ok(())
            }
        }

        let limiter = CardinalityLimiter::new(RejectLimits);
        let limits = &[
            CardinalityLimit {
                id: "limit_passive".to_owned(),
                passive: false,
                report: false,
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 10_000,
                scope: CardinalityScope::Organization,
                namespace: None,
            },
            CardinalityLimit {
                id: "limit_enforced".to_owned(),
                passive: true,
                report: false,
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 10_000,
                scope: CardinalityScope::Organization,
                namespace: None,
            },
        ];

        let items = vec![
            Item::new(0, MetricNamespace::Custom),
            Item::new(1, MetricNamespace::Custom),
            Item::new(2, MetricNamespace::Custom),
            Item::new(3, MetricNamespace::Custom),
            Item::new(4, MetricNamespace::Custom),
            Item::new(5, MetricNamespace::Custom),
        ];
        let limited = limiter
            .check_cardinality_limits(build_scoping(), limits, items)
            .unwrap();

        assert!(limited.has_rejections());
        assert_eq!(limited.exceeded_limits(), &limits.iter().collect());

        // All passive items and no enforced (passive = False) should be accepted.
        let rejected = limited.rejected().collect::<HashSet<_>>();
        assert_eq!(
            rejected,
            HashSet::from([
                &Item::new(0, MetricNamespace::Custom),
                &Item::new(2, MetricNamespace::Custom),
                &Item::new(4, MetricNamespace::Custom),
            ])
        );
        drop(rejected); // NLL are broken here without the explicit drop
        assert_eq!(
            limited.into_split().accepted,
            vec![
                Item::new(1, MetricNamespace::Custom),
                Item::new(3, MetricNamespace::Custom),
                Item::new(5, MetricNamespace::Custom),
            ]
        );
    }

    #[test]
    fn test_cardinality_report() {
        struct CreateReports;

        impl Limiter for CreateReports {
            fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                scoping: Scoping,
                limits: &'a [CardinalityLimit],
                _entries: I,
                reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>>,
                T: Reporter<'a>,
            {
                reporter.report_cardinality(
                    &limits[0],
                    CardinalityReport {
                        organization_id: Some(scoping.organization_id),
                        project_id: Some(scoping.project_id),
                        name: Some(MetricName::from("foo")),
                        cardinality: 1,
                    },
                );

                reporter.report_cardinality(
                    &limits[0],
                    CardinalityReport {
                        organization_id: Some(scoping.organization_id),
                        project_id: Some(scoping.project_id),
                        name: Some(MetricName::from("bar")),
                        cardinality: 2,
                    },
                );

                reporter.report_cardinality(
                    &limits[2],
                    CardinalityReport {
                        organization_id: Some(scoping.organization_id),
                        project_id: Some(scoping.project_id),
                        name: None,
                        cardinality: 3,
                    },
                );

                Ok(())
            }
        }

        let window = SlidingWindow {
            window_seconds: 3600,
            granularity_seconds: 360,
        };

        let limits = &[
            CardinalityLimit {
                id: "report".to_owned(),
                passive: false,
                report: true,
                window,
                limit: 10_000,
                scope: CardinalityScope::Organization,
                namespace: None,
            },
            CardinalityLimit {
                id: "no_report".to_owned(),
                passive: false,
                report: false,
                window,
                limit: 10_000,
                scope: CardinalityScope::Organization,
                namespace: None,
            },
            CardinalityLimit {
                id: "report_again".to_owned(),
                passive: true,
                report: true,
                window,
                limit: 10_000,
                scope: CardinalityScope::Organization,
                namespace: None,
            },
        ];
        let scoping = build_scoping();
        let items = vec![Item::new(0, MetricNamespace::Custom)];

        let limiter = CardinalityLimiter::new(CreateReports);
        let limited = limiter
            .check_cardinality_limits(scoping, limits, items)
            .unwrap();

        let reports = limited.cardinality_reports();
        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports.get(&limits[0]).unwrap(),
            &[
                CardinalityReport {
                    organization_id: Some(scoping.organization_id),
                    project_id: Some(scoping.project_id),
                    name: Some(MetricName::from("foo")),
                    cardinality: 1
                },
                CardinalityReport {
                    organization_id: Some(scoping.organization_id),
                    project_id: Some(scoping.project_id),
                    name: Some(MetricName::from("bar")),
                    cardinality: 2
                }
            ]
        );
        assert_eq!(
            reports.get(&limits[2]).unwrap(),
            &[CardinalityReport {
                organization_id: Some(scoping.organization_id),
                project_id: Some(scoping.project_id),
                name: None,
                cardinality: 3
            }]
        );
    }
}
