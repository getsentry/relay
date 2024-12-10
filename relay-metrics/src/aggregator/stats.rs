use hashbrown::HashMap;
use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::project::ProjectKey;

use crate::aggregator::AggregateMetricsError;
use crate::utils::ByNamespace;

/// Total stats tracked for the aggregator.
#[derive(Default, Debug)]
pub struct Total {
    /// Total amount of buckets in the aggregator.
    pub count: u64,
    /// Total amount of buckets in the aggregator by namespace.
    pub count_by_namespace: ByNamespace<u64>,

    /// Total cost of buckets in the aggregator.
    pub cost: u64,
    /// Total cost of buckets in the aggregator by namespace.
    pub cost_by_namespace: ByNamespace<u64>,
}

impl Total {
    pub fn remove_slot(&mut self, slot: &Slot) {
        let Self {
            count,
            count_by_namespace,
            cost,
            cost_by_namespace,
        } = self;

        *count -= slot.count;
        *count_by_namespace -= slot.count_by_namespace;
        *cost -= slot.cost;
        *cost_by_namespace -= slot.cost_by_namespace;
    }
}

/// Stats tracked by slot in the aggregator.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Slot {
    /// Amount of buckets created in this slot.
    pub count: u64,
    /// Amount of buckets created in this slot by namespace.
    pub count_by_namespace: ByNamespace<u64>,
    /// Amount of merges happened in this slot.
    pub merges: u64,
    /// Amount of merges happened in this slot by namespace.
    pub merges_by_namespace: ByNamespace<u64>,

    /// Cost of buckets in this slot.
    pub cost: u64,
    /// Cost of buckets in this slot by namespace.
    pub cost_by_namespace: ByNamespace<u64>,
    /// Cost of buckets in this slot by project.
    pub cost_by_project: HashMap<ProjectKey, u64>,
}

impl Slot {
    /// Resets the slot to its initial empty state.
    pub fn reset(&mut self) {
        let Self {
            count,
            count_by_namespace,
            merges,
            merges_by_namespace,
            cost,
            cost_by_namespace,
            cost_by_project,
        } = self;

        *count = 0;
        *count_by_namespace = Default::default();
        *merges = 0;
        *merges_by_namespace = Default::default();

        *cost = 0;
        *cost_by_namespace = Default::default();
        // Keep the allocation around but at the same time make it possible for it to shrink.
        cost_by_project.shrink_to_fit();
        cost_by_project.clear();
    }

    /// Increments the count by one.
    pub fn incr_count(&mut self, total: &mut Total, namespace: MetricNamespace) {
        self.count += 1;
        *self.count_by_namespace.get_mut(namespace) += 1;
        total.count += 1;
        *total.count_by_namespace.get_mut(namespace) += 1;
    }

    /// Increments the amount of merges in the slot by one.
    pub fn incr_merges(&mut self, namespace: MetricNamespace) {
        self.merges += 1;
        *self.merges_by_namespace.get_mut(namespace) += 1;
    }

    /// Tries to reserve a certain amount of cost.
    ///
    /// Returns an error if there is not enough budget left.
    pub fn reserve<'a>(
        &'a mut self,
        total: &'a mut Total,
        project_key: ProjectKey,
        namespace: MetricNamespace,
        cost: u64,
        limits: &Limits,
    ) -> Result<Reservation<'a>, AggregateMetricsError> {
        if total.cost + cost > limits.max_total {
            return Err(AggregateMetricsError::TotalLimitExceeded);
        }
        let project = self.cost_by_project.entry(project_key).or_insert(0);
        if *project + cost > limits.max_partition_project {
            return Err(AggregateMetricsError::ProjectLimitExceeded);
        }

        Ok(Reservation {
            total,
            cost_partition: &mut self.cost,
            cost_partition_by_namespace: &mut self.cost_by_namespace,
            cost_partition_project: project,
            namespace,
            reserved: cost,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_total: u64,
    pub max_partition_project: u64,
}

#[must_use = "a reservation does nothing if it is not used"]
pub struct Reservation<'a> {
    total: &'a mut Total,

    cost_partition: &'a mut u64,
    cost_partition_by_namespace: &'a mut ByNamespace<u64>,
    cost_partition_project: &'a mut u64,

    namespace: MetricNamespace,

    reserved: u64,
}

impl Reservation<'_> {
    pub fn consume(self) {
        let reserved = self.reserved;
        self.consume_with(reserved);
    }

    pub fn consume_with(self, cost: u64) {
        debug_assert!(cost <= self.reserved, "less reserved than used");
        // Update total costs.
        self.total.cost += cost;
        *self.total.cost_by_namespace.get_mut(self.namespace) += cost;

        // Update all partition costs.
        *self.cost_partition += cost;
        *self.cost_partition_by_namespace.get_mut(self.namespace) += cost;
        *self.cost_partition_project += cost;
    }
}
