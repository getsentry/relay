use hash32::Hasher;
use std::fmt::{self, Write};
use std::hash::Hash;

use relay_base_schema::metrics::{MetricName, MetricNamespace, MetricType};
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;

use crate::limiter::Entry;
use crate::redis::{KEY_PREFIX, KEY_VERSION};
use crate::window::Slot;
use crate::{CardinalityLimit, CardinalityScope, OrganizationId, Scoping, SlidingWindow};

/// A quota scoping extracted from a [`CardinalityLimit`] and a [`Scoping`].
///
/// The partial quota scoping can be used to select/match on cardinality entries
/// but it needs to be completed into a [`QuotaScoping`] by using
/// [`PartialQuotaScoping::complete`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartialQuotaScoping {
    pub organization_id: Option<OrganizationId>,
    pub project_id: Option<ProjectId>,
    pub namespace: Option<MetricNamespace>,
    window: SlidingWindow,
    scope: CardinalityScope,
}

impl PartialQuotaScoping {
    /// Creates a new [`PartialQuotaScoping`] from a [`Scoping`] and [`CardinalityLimit`].
    ///
    /// Returns `None` for limits with scope [`CardinalityScope::Unknown`].
    pub fn new(scoping: Scoping, limit: &CardinalityLimit) -> Option<Self> {
        let (organization_id, project_id) = match limit.scope {
            CardinalityScope::Organization => (Some(scoping.organization_id), None),
            CardinalityScope::Project => (Some(scoping.organization_id), Some(scoping.project_id)),
            CardinalityScope::Type => (Some(scoping.organization_id), Some(scoping.project_id)),
            CardinalityScope::Name => (Some(scoping.organization_id), Some(scoping.project_id)),
            // Invalid/unknown scope -> ignore the limit.
            CardinalityScope::Unknown => return None,
        };

        Some(Self {
            organization_id,
            project_id,
            namespace: limit.namespace,
            window: limit.window,
            scope: limit.scope,
        })
    }

    /// Wether the scoping applies to the passed entry.
    pub fn matches(&self, entry: &Entry) -> bool {
        self.namespace.is_none() || self.namespace == Some(entry.namespace)
    }

    /// Returns the currently active slot.
    pub fn active_slot(&self, timestamp: UnixTimestamp) -> Slot {
        self.window.active_slot(self.shifted(timestamp))
    }

    /// Returns all slots of the sliding window for a specific timestamp.
    pub fn slots(&self, timestamp: UnixTimestamp) -> impl Iterator<Item = Slot> {
        self.window.iter(self.shifted(timestamp))
    }

    /// Applies a timeshift based on the granularity of the sliding window to the passed timestamp.
    ///
    /// The shift is used to evenly distribute cache and Redis operations across
    /// the sliding window's granule.
    fn shifted(&self, timestamp: UnixTimestamp) -> UnixTimestamp {
        let shift = self
            .organization_id
            .map(|o| o % self.window.granularity_seconds)
            .unwrap_or(0);

        UnixTimestamp::from_secs(timestamp.as_secs() + shift)
    }

    /// Creates a [`QuotaScoping`] from the partial scoping and the passed [`Entry`].
    ///
    /// This unconditionally creates a quota scoping from the passed entry and
    /// does not check whether the scoping even applies to the entry. The caller
    /// needs to ensure this by calling [`Self::matches`] prior to calling `complete`.
    pub fn complete(self, entry: Entry<'_>) -> QuotaScoping {
        let metric_name = match self.scope {
            CardinalityScope::Name => Some(entry.name.clone()),
            _ => None,
        };
        let metric_type = match self.scope {
            CardinalityScope::Type => entry.name.try_type(),
            _ => None,
        };

        QuotaScoping {
            parent: self,
            metric_type,
            metric_name,
        }
    }
}

/// A quota scoping extracted from a [`CardinalityLimit`], a [`Scoping`]
/// and completed with a [`CardinalityItem`](crate::CardinalityItem).
///
/// The scoping must be created using [`PartialQuotaScoping::complete`].
/// and a [`CardinalityItem`](crate::CardinalityItem).
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct QuotaScoping {
    parent: PartialQuotaScoping,
    pub metric_type: Option<MetricType>,
    pub metric_name: Option<MetricName>,
}

impl QuotaScoping {
    /// Returns the minimum TTL for a Redis key created by [`Self::to_redis_key`].
    pub fn redis_key_ttl(&self) -> u64 {
        self.window.window_seconds
    }

    /// Turns the scoping into a Redis key for the passed slot.
    pub fn to_redis_key(&self, slot: Slot) -> String {
        let organization_id = self.organization_id.unwrap_or(0);
        let project_id = self.project_id.map(|p| p.value()).unwrap_or(0);
        let namespace = self.namespace.map(|ns| ns.as_str()).unwrap_or("");
        let metric_type = DisplayOptMinus(self.metric_type);
        let metric_name = DisplayOptMinus(self.metric_name.as_deref().map(fnv32));

        // Use a pre-allocated buffer instead of `format!()`, benchmarks have shown
        // this does have quite a big impact when cardinality limiting a high amount
        // of different metric names.
        let mut result = String::with_capacity(200);
        write!(
            &mut result,
            "{KEY_PREFIX}:{KEY_VERSION}:scope-{{{organization_id}-{project_id}-{namespace}}}-{metric_type}{metric_name}{slot}"
        )
        .expect("formatting into a string never fails");

        result
    }
}

impl std::ops::Deref for QuotaScoping {
    type Target = PartialQuotaScoping;

    fn deref(&self) -> &Self::Target {
        &self.parent
    }
}

// Required for hashbrown's `entry_ref`.
impl From<&QuotaScoping> for QuotaScoping {
    fn from(value: &QuotaScoping) -> Self {
        value.clone()
    }
}

impl fmt::Debug for QuotaScoping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let PartialQuotaScoping {
            organization_id,
            project_id,
            namespace,
            window,
            scope,
        } = &self.parent;

        f.debug_struct("QuotaScoping")
            .field("organization_id", organization_id)
            .field("project_id", project_id)
            .field("namespace", namespace)
            .field("window", window)
            .field("scope", scope)
            .field("metric_type", &self.metric_type)
            .field("metric_name", &self.metric_name)
            .finish()
    }
}

struct DisplayOptMinus<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for DisplayOptMinus<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(inner) = self.0.as_ref() {
            write!(f, "{inner}-")
        } else {
            Ok(())
        }
    }
}

fn fnv32(s: &str) -> u32 {
    let mut hasher = hash32::FnvHasher::default();
    s.hash(&mut hasher);
    hasher.finish32()
}
