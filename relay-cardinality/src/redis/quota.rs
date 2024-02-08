use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;

use crate::limiter::Entry;
use crate::redis::{KEY_PREFIX, KEY_VERSION};
use crate::window::Slot;
use crate::{CardinalityLimit, CardinalityScope, OrganizationId, Scoping, SlidingWindow};

/// A quota scoping extracted from a [`CardinalityLimit`] and a [`Scoping`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct QuotaScoping {
    pub namespace: Option<MetricNamespace>,
    pub organization_id: Option<OrganizationId>,
    pub project_id: Option<ProjectId>,
    window: SlidingWindow,
}

impl QuotaScoping {
    /// Creates a new [`QuotaScoping`] from a [`Scoping`] and [`CardinalityLimit`].
    ///
    /// Returns `None` for limits with scope [`CardinalityScope::Unknown`].
    pub fn new(scoping: Scoping, limit: &CardinalityLimit) -> Option<Self> {
        let (organization_id, project_id) = match limit.scope {
            CardinalityScope::Organization => (Some(scoping.organization_id), None),
            CardinalityScope::Project => (Some(scoping.organization_id), Some(scoping.project_id)),
            // Invalid/unknown scope -> ignore the limit.
            CardinalityScope::Unknown => return None,
        };

        Some(Self {
            window: limit.window,
            namespace: limit.namespace,
            organization_id,
            project_id,
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

    /// Returns the minimum TTL for a Redis key created by [`Self::into_redis_key`].
    pub fn redis_key_ttl(&self) -> u64 {
        self.window.window_seconds
    }

    /// Turns the scoping into a Redis key for the passed slot.
    pub fn into_redis_key(self, slot: Slot) -> String {
        let organization_id = self.organization_id.unwrap_or(0);
        let project_id = self.project_id.map(|p| p.value()).unwrap_or(0);
        let namespace = self.namespace.map(|ns| ns.as_str()).unwrap_or("");

        format!("{KEY_PREFIX}:{KEY_VERSION}:scope-{{{organization_id}-{project_id}-{namespace}}}-{slot}")
    }
}
