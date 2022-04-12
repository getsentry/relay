use std::collections::BTreeSet;

use relay_sampling::RuleCondition;
use serde::{Deserialize, Serialize};
#[cfg(feature = "processing")]
use {relay_general::protocol::Event, relay_metrics::Metric};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaggingRule {
    // note: could add relay_sampling::RuleType here, but right now we only support transaction
    // events
    pub condition: RuleCondition,
    pub target_metrics: BTreeSet<String>,
    pub target_tag: String,
    pub tag_value: String,
}

#[cfg(feature = "processing")]
pub fn run_conditional_tagging(event: &Event, config: &[TaggingRule], metrics: &mut [Metric]) {
    for rule in config {
        if !rule.condition.supported()
            || rule.target_metrics.is_empty()
            || !rule.condition.matches_event(event, None)
        {
            continue;
        }

        // XXX(slow): this is a double-for-loop, but we extract like 6 metrics per transaction
        for metric in &mut *metrics {
            if !rule.target_metrics.contains(&metric.name)
                || metric.tags.contains_key(&rule.target_tag)
            {
                continue;
            }

            metric
                .tags
                .insert(rule.target_tag.clone(), rule.tag_value.clone());
        }
    }
}
