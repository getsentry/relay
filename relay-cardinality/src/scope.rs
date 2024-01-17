use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::project::ProjectId;

use crate::limiter::{Entry, Scoping};
use crate::{CardinalityLimit, CardinalityScope, OrganizationId, SlidingWindow};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct FooScope {
    pub window: SlidingWindow,
    pub namespace: Option<MetricNamespace>,
    pub organization_id: Option<OrganizationId>,
    pub project_id: Option<ProjectId>,
}

impl FooScope {
    pub fn new(scoping: Scoping, limit: &CardinalityLimit) -> Option<Self> {
        let (organization_id, project_id) = match limit.scope {
            CardinalityScope::Organization => (Some(scoping.organization_id), None),
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

    pub fn matches(&self, entry: &Entry) -> bool {
        return self.namespace.is_none() || self.namespace == Some(entry.namespace);
    }
}
