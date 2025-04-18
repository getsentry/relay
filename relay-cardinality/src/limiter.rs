//! Relay Cardinality Limiter

use std::cmp::Reverse;
use std::collections::BTreeMap;

use async_trait::async_trait;
use hashbrown::{HashMap, HashSet};
use relay_base_schema::metrics::{MetricName, MetricNamespace, MetricType};
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_statsd::metric;

use crate::statsd::CardinalityLimiterTimers;
use crate::{CardinalityLimit, Error, Result};

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
    /// Time for which the cardinality limit was enforced.
    pub timestamp: UnixTimestamp,

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
    /// Metric type for which the cardinality limit was applied.
    ///
    /// Only available if the the limit was scoped to
    /// [`CardinalityScope::Type`](crate::CardinalityScope::Type).
    pub metric_type: Option<MetricType>,
    /// Metric name for which the cardinality limit was applied.
    ///
    /// Only available if the limit was scoped to
    /// [`CardinalityScope::Name`](crate::CardinalityScope::Name).
    pub metric_name: Option<MetricName>,

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
#[async_trait]
pub trait Limiter {
    /// Verifies cardinality limits.
    ///
    /// Returns an iterator containing only accepted entries.
    async fn check_cardinality_limits<'a, 'b, E, R>(
        &self,
        scoping: Scoping,
        limits: &'a [CardinalityLimit],
        entries: E,
        reporter: &mut R,
    ) -> Result<()>
    where
        E: IntoIterator<Item = Entry<'b>> + Send,
        R: Reporter<'a> + Send;
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
    /// Opaque entry id, used to keep track of indices and buckets.
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
    pub async fn check_cardinality_limits<'a, I: CardinalityItem>(
        &self,
        scoping: Scoping,
        limits: &'a [CardinalityLimit],
        items: Vec<I>,
    ) -> Result<CardinalityLimits<'a, I>, (Vec<I>, Error)> {
        if limits.is_empty() {
            return Ok(CardinalityLimits::new(items, Default::default()));
        }

        metric!(timer(CardinalityLimiterTimers::CardinalityLimiter), {
            let entries = items
                .iter()
                .enumerate()
                .filter_map(|(id, item)| {
                    Some(Entry::new(
                        EntryId(id),
                        item.namespace()?,
                        item.name(),
                        item.to_hash(),
                    ))
                })
                .collect::<Vec<_>>();

            let mut rejections = DefaultReporter::default();
            if let Err(err) = self
                .limiter
                .check_cardinality_limits(scoping, limits, entries, &mut rejections)
                .await
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
    /// All limits that have been exceeded.
    exceeded_limits: HashSet<&'a CardinalityLimit>,
    /// A map from entries that have been rejected to the most
    /// specific non-passive limit that they exceeded.
    ///
    /// "Specificity" is determined by scope and limit, in that order.
    entries: HashMap<usize, &'a CardinalityLimit>,
    reports: BTreeMap<&'a CardinalityLimit, Vec<CardinalityReport>>,
}

impl<'a> Reporter<'a> for DefaultReporter<'a> {
    #[inline(always)]
    fn reject(&mut self, limit: &'a CardinalityLimit, entry_id: EntryId) {
        self.exceeded_limits.insert(limit);
        if !limit.passive {
            // Write `limit` into the entry if it's more specific than the existing limit
            // (or if there isn't one)
            self.entries
                .entry(entry_id.0)
                .and_modify(|existing_limit| {
                    // Scopes are ordered by reverse specificity (org is the smallest), so we use `Reverse` here
                    if (Reverse(limit.scope), limit.limit)
                        < (Reverse(existing_limit.scope), existing_limit.limit)
                    {
                        *existing_limit = limit;
                    }
                })
                .or_insert(limit);
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
pub struct CardinalityLimitsSplit<'a, T> {
    /// The list of accepted elements of the source.
    pub accepted: Vec<T>,
    /// The list of rejected elements of the source, together
    /// with the most specific limit they exceeded.
    pub rejected: Vec<(T, &'a CardinalityLimit)>,
}

impl<T> CardinalityLimitsSplit<'_, T> {
    /// Creates a new cardinality limits split with a given capacity for `accepted` and `rejected`
    /// elements.
    fn with_capacity(accepted_capacity: usize, rejected_capacity: usize) -> Self {
        CardinalityLimitsSplit {
            accepted: Vec::with_capacity(accepted_capacity),
            rejected: Vec::with_capacity(rejected_capacity),
        }
    }
}

/// Result of [`CardinalityLimiter::check_cardinality_limits`].
#[derive(Debug)]
pub struct CardinalityLimits<'a, T> {
    /// The source.
    source: Vec<T>,
    /// List of rejected item indices pointing into `source`.
    rejections: HashMap<usize, &'a CardinalityLimit>,
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
        self.rejections.keys().filter_map(|&i| self.source.get(i))
    }

    /// Consumes the result and returns [`CardinalityLimitsSplit`] containing all accepted and rejected items.
    pub fn into_split(mut self) -> CardinalityLimitsSplit<'a, T> {
        if self.rejections.is_empty() {
            return CardinalityLimitsSplit {
                accepted: self.source,
                rejected: Vec::new(),
            };
        }
        // TODO: we might want to optimize this method later, by reusing one of the arrays and
        // swap removing elements from it.
        let source_len = self.source.len();
        let rejections_len = self.rejections.len();
        self.source.into_iter().enumerate().fold(
            CardinalityLimitsSplit::with_capacity(source_len - rejections_len, rejections_len),
            |mut split, (i, item)| {
                if let Some(exceeded) = self.rejections.remove(&i) {
                    split.rejected.push((item, exceeded));
                } else {
                    split.accepted.push(item);
                };

                split
            },
        )
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
            organization_id: OrganizationId::new(1),
            project_id: ProjectId::new(1),
        }
    }

    #[tokio::test]
    async fn test_accepted() {
        // HACK: we need to make Windows happy.
        fn assert_eq(value: Vec<char>, expected_value: Vec<char>) {
            assert_eq!(value, expected_value)
        }

        let limit = CardinalityLimit {
            id: "dummy_limit".to_owned(),
            passive: false,
            report: false,
            window: SlidingWindow {
                window_seconds: 0,
                granularity_seconds: 0,
            },
            limit: 0,
            scope: CardinalityScope::Organization,
            namespace: None,
        };

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashMap::from([(0, &limit), (1, &limit), (3, &limit)]),
            exceeded_limits: HashSet::new(),
            reports: BTreeMap::new(),
        };
        assert!(limits.has_rejections());
        let split = limits.into_split();
        assert_eq!(
            split.rejected,
            vec![('a', &limit), ('b', &limit), ('d', &limit)]
        );
        assert_eq!(split.accepted, vec!['c', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashMap::from([]),
            exceeded_limits: HashSet::new(),
            reports: BTreeMap::new(),
        };
        assert!(!limits.has_rejections());
        let split = limits.into_split();
        assert!(split.rejected.is_empty());
        assert_eq!(split.accepted, vec!['a', 'b', 'c', 'd', 'e']);

        let limits = CardinalityLimits {
            source: vec!['a', 'b', 'c', 'd', 'e'],
            rejections: HashMap::from([
                (0, &limit),
                (1, &limit),
                (2, &limit),
                (3, &limit),
                (4, &limit),
            ]),
            exceeded_limits: HashSet::new(),
            reports: BTreeMap::new(),
        };
        assert!(limits.has_rejections());
        let split = limits.into_split();
        assert_eq!(
            split.rejected,
            vec![
                ('a', &limit),
                ('b', &limit),
                ('c', &limit),
                ('d', &limit),
                ('e', &limit)
            ]
        );
        assert_eq(split.accepted, vec![]);
    }

    #[tokio::test]
    async fn test_limiter_reject_all() {
        struct RejectAllLimiter;

        #[async_trait]
        impl Limiter for RejectAllLimiter {
            async fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                _scoping: Scoping,
                limits: &'a [CardinalityLimit],
                entries: I,
                rejections: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>> + Send,
                T: Reporter<'a> + Send,
            {
                for entry in entries {
                    rejections.reject(&limits[0], entry.id);
                }

                Ok(())
            }
        }

        let limiter = CardinalityLimiter::new(RejectAllLimiter);

        let items = vec![
            Item::new(0, MetricNamespace::Transactions),
            Item::new(1, MetricNamespace::Transactions),
        ];
        let limits = build_limits();
        let result = limiter
            .check_cardinality_limits(build_scoping(), &limits, items.clone())
            .await
            .unwrap();

        let expected_items = items
            .into_iter()
            .zip(std::iter::repeat(&limits[0]))
            .collect::<Vec<_>>();

        assert_eq!(result.exceeded_limits(), &HashSet::from([&limits[0]]));
        let split = result.into_split();
        assert_eq!(split.rejected, expected_items);
        assert!(split.accepted.is_empty());
    }

    #[tokio::test]
    async fn test_limiter_accept_all() {
        struct AcceptAllLimiter;

        #[async_trait]
        impl Limiter for AcceptAllLimiter {
            async fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                _scoping: Scoping,
                _limits: &'a [CardinalityLimit],
                _entries: I,
                _reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>> + Send,
                T: Reporter<'a> + Send,
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
            .await
            .unwrap();

        let split = result.into_split();
        assert!(split.rejected.is_empty());
        assert_eq!(split.accepted, items);
    }

    #[tokio::test]
    async fn test_limiter_accept_odd_reject_even() {
        struct RejectEvenLimiter;

        #[async_trait]
        impl Limiter for RejectEvenLimiter {
            async fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                scoping: Scoping,
                limits: &'a [CardinalityLimit],
                entries: I,
                reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>> + Send,
                T: Reporter<'a> + Send,
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
        let limits = build_limits();
        let split = limiter
            .check_cardinality_limits(build_scoping(), &limits, items)
            .await
            .unwrap()
            .into_split();

        assert_eq!(
            split.rejected,
            vec![
                (Item::new(0, MetricNamespace::Sessions), &limits[0]),
                (Item::new(2, MetricNamespace::Spans), &limits[0]),
                (Item::new(4, MetricNamespace::Custom), &limits[0]),
                (Item::new(6, MetricNamespace::Spans), &limits[0]),
            ]
        );
        assert_eq!(
            split.accepted,
            vec![
                Item::new(1, MetricNamespace::Transactions),
                Item::new(3, MetricNamespace::Custom),
                Item::new(5, MetricNamespace::Transactions),
            ]
        );
    }

    #[tokio::test]
    async fn test_limiter_passive() {
        struct RejectLimits;

        #[async_trait]
        impl Limiter for RejectLimits {
            async fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                _scoping: Scoping,
                limits: &'a [CardinalityLimit],
                entries: I,
                reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>> + Send,
                T: Reporter<'a> + Send,
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
            .await
            .unwrap();

        assert!(limited.has_rejections());
        assert_eq!(limited.exceeded_limits(), &limits.iter().collect());

        let split = limited.into_split();
        assert_eq!(
            split.rejected,
            vec![
                (Item::new(0, MetricNamespace::Custom), &limits[0]),
                (Item::new(2, MetricNamespace::Custom), &limits[0]),
                (Item::new(4, MetricNamespace::Custom), &limits[0]),
            ]
        );
        assert_eq!(
            split.accepted,
            vec![
                Item::new(1, MetricNamespace::Custom),
                Item::new(3, MetricNamespace::Custom),
                Item::new(5, MetricNamespace::Custom),
            ]
        );
    }

    #[tokio::test]
    async fn test_cardinality_report() {
        struct CreateReports;

        #[async_trait]
        impl Limiter for CreateReports {
            async fn check_cardinality_limits<'a, 'b, I, T>(
                &self,
                scoping: Scoping,
                limits: &'a [CardinalityLimit],
                _entries: I,
                reporter: &mut T,
            ) -> Result<()>
            where
                I: IntoIterator<Item = Entry<'b>> + Send,
                T: Reporter<'a> + Send,
            {
                reporter.report_cardinality(
                    &limits[0],
                    CardinalityReport {
                        timestamp: UnixTimestamp::from_secs(5000),
                        organization_id: Some(scoping.organization_id),
                        project_id: Some(scoping.project_id),
                        metric_type: None,
                        metric_name: Some(MetricName::from("foo")),
                        cardinality: 1,
                    },
                );

                reporter.report_cardinality(
                    &limits[0],
                    CardinalityReport {
                        timestamp: UnixTimestamp::from_secs(5001),
                        organization_id: Some(scoping.organization_id),
                        project_id: Some(scoping.project_id),
                        metric_type: None,
                        metric_name: Some(MetricName::from("bar")),
                        cardinality: 2,
                    },
                );

                reporter.report_cardinality(
                    &limits[2],
                    CardinalityReport {
                        timestamp: UnixTimestamp::from_secs(5002),
                        organization_id: Some(scoping.organization_id),
                        project_id: Some(scoping.project_id),
                        metric_type: None,
                        metric_name: None,
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
            .await
            .unwrap();

        let reports = limited.cardinality_reports();
        assert_eq!(reports.len(), 2);
        assert_eq!(
            reports.get(&limits[0]).unwrap(),
            &[
                CardinalityReport {
                    timestamp: UnixTimestamp::from_secs(5000),
                    organization_id: Some(scoping.organization_id),
                    project_id: Some(scoping.project_id),
                    metric_type: None,
                    metric_name: Some(MetricName::from("foo")),
                    cardinality: 1
                },
                CardinalityReport {
                    timestamp: UnixTimestamp::from_secs(5001),
                    organization_id: Some(scoping.organization_id),
                    project_id: Some(scoping.project_id),
                    metric_type: None,
                    metric_name: Some(MetricName::from("bar")),
                    cardinality: 2
                }
            ]
        );
        assert_eq!(
            reports.get(&limits[2]).unwrap(),
            &[CardinalityReport {
                timestamp: UnixTimestamp::from_secs(5002),
                organization_id: Some(scoping.organization_id),
                project_id: Some(scoping.project_id),
                metric_type: None,
                metric_name: None,
                cardinality: 3
            }]
        );
    }
}
