use std::collections::BTreeMap;
use std::fmt;

use relay_base_schema::project::ProjectKey;

use crate::aggregator::AggregateMetricsError;
use crate::MetricNamespace;
use hashbrown::HashMap;

/// Estimates the number of bytes needed to encode the tags.
///
/// Note that this does not necessarily match the exact memory footprint of the tags,
/// because data structures or their serialization have overheads.
pub fn tags_cost(tags: &BTreeMap<String, String>) -> usize {
    tags.iter().map(|(k, v)| k.len() + v.len()).sum()
}

#[derive(Default)]
pub struct Tracker {
    total_cost: usize,
    cost_per_project_key: HashMap<ProjectKey, usize>,
    cost_per_namespace: BTreeMap<MetricNamespace, usize>,
}

impl Tracker {
    #[cfg(test)]
    pub fn total_cost(&self) -> usize {
        self.total_cost
    }

    pub fn cost_per_namespace(&self) -> impl Iterator<Item = (MetricNamespace, usize)> + use<'_> {
        self.cost_per_namespace.iter().map(|(k, v)| (*k, *v))
    }

    pub fn totals_cost_exceeded(&self, max_total_cost: Option<usize>) -> bool {
        if let Some(max_total_cost) = max_total_cost {
            if self.total_cost >= max_total_cost {
                return true;
            }
        }

        false
    }

    pub fn check_limits_exceeded(
        &self,
        project_key: ProjectKey,
        max_total_cost: Option<usize>,
        max_project_cost: Option<usize>,
    ) -> Result<(), AggregateMetricsError> {
        if self.totals_cost_exceeded(max_total_cost) {
            relay_log::configure_scope(|scope| {
                scope.set_extra("bucket.project_key", project_key.as_str().to_owned().into());
            });
            return Err(AggregateMetricsError::TotalLimitExceeded);
        }

        if let Some(max_project_cost) = max_project_cost {
            let project_cost = self
                .cost_per_project_key
                .get(&project_key)
                .cloned()
                .unwrap_or(0);
            if project_cost >= max_project_cost {
                relay_log::configure_scope(|scope| {
                    scope.set_extra("bucket.project_key", project_key.as_str().to_owned().into());
                });
                return Err(AggregateMetricsError::ProjectLimitExceeded);
            }
        }

        Ok(())
    }

    pub fn add_cost(&mut self, namespace: MetricNamespace, project_key: ProjectKey, cost: usize) {
        *self.cost_per_project_key.entry(project_key).or_insert(0) += cost;
        *self.cost_per_namespace.entry(namespace).or_insert(0) += cost;
        self.total_cost += cost;
    }

    pub fn subtract_cost(
        &mut self,
        namespace: MetricNamespace,
        project_key: ProjectKey,
        cost: usize,
    ) {
        let project_cost = self.cost_per_project_key.get_mut(&project_key);
        let namespace_cost = self.cost_per_namespace.get_mut(&namespace);
        match (project_cost, namespace_cost) {
            (Some(project_cost), Some(namespace_cost))
                if *project_cost >= cost && *namespace_cost >= cost =>
            {
                *project_cost -= cost;
                if *project_cost == 0 {
                    self.cost_per_project_key.remove(&project_key);
                }
                *namespace_cost -= cost;
                if *namespace_cost == 0 {
                    self.cost_per_namespace.remove(&namespace);
                }
                self.total_cost -= cost;
            }
            _ => {
                relay_log::error!(
                    namespace = namespace.as_str(),
                    project_key = project_key.to_string(),
                    cost = cost.to_string(),
                    "Cost mismatch",
                );
            }
        }
    }
}

impl fmt::Debug for Tracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CostTracker")
            .field("total_cost", &self.total_cost)
            // Convert HashMap to BTreeMap to guarantee order:
            .field(
                "cost_per_project_key",
                &BTreeMap::from_iter(self.cost_per_project_key.iter()),
            )
            .field("cost_per_namespace", &self.cost_per_namespace)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_tracker() {
        let namespace = MetricNamespace::Custom;
        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let project_key3 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();
        let mut cost_tracker = Tracker::default();
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 0,
            cost_per_project_key: {},
            cost_per_namespace: {},
        }
        "#);
        cost_tracker.add_cost(MetricNamespace::Custom, project_key1, 50);
        cost_tracker.add_cost(MetricNamespace::Profiles, project_key1, 50);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 100,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 50,
            },
        }
        "#);
        cost_tracker.add_cost(namespace, project_key2, 200);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 250,
            },
        }
        "#);
        // Unknown project: bail
        cost_tracker.subtract_cost(namespace, project_key3, 666);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 250,
            },
        }
        "#);
        // Subtract too much: bail
        cost_tracker.subtract_cost(namespace, project_key1, 666);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 250,
            },
        }
        "#);
        cost_tracker.subtract_cost(namespace, project_key2, 20);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 280,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 180,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 230,
            },
        }
        "#);

        // Subtract all
        cost_tracker.subtract_cost(MetricNamespace::Profiles, project_key1, 50);
        cost_tracker.subtract_cost(MetricNamespace::Custom, project_key1, 50);
        cost_tracker.subtract_cost(MetricNamespace::Custom, project_key2, 180);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 0,
            cost_per_project_key: {},
            cost_per_namespace: {},
        }
        "#);
    }
}
