use std::collections::BTreeSet;

use relay_dynamic_config::TaggingRule;
use relay_sampling::RuleCondition;
use serde::{Deserialize, Serialize};
use {relay_general::protocol::Event, relay_metrics::Metric};

pub fn run_conditional_tagging(event: &Event, config: &[TaggingRule], metrics: &mut [Metric]) {
    for rule in config {
        if !rule.condition.supported()
            || rule.target_metrics.is_empty()
            || !rule.condition.matches(event, None)
        {
            continue;
        }

        // XXX(slow): this is a double-for-loop, but we extract like 6 metrics per transaction
        for metric in &mut *metrics {
            if !rule.target_metrics.contains(&metric.name) {
                // this metric should not be updated as part of this rule
                continue;
            }

            if metric.tags.contains_key(&rule.target_tag) {
                // the metric tag already exists, and we are supposed to skip over rules if the tag
                // is already set. This behavior helps with building specific rules and fallback
                // rules.
                continue;
            }

            metric
                .tags
                .insert(rule.target_tag.clone(), rule.tag_value.clone());
        }
    }
}
