use relay_dynamic_config::TaggingRule;
use relay_general::protocol::Event;
use relay_metrics::Metric;

pub fn run_conditional_tagging(event: &Event, config: &[TaggingRule], metrics: &mut [Metric]) {
    for rule in config {
        if !rule.condition.supported()
            || rule.target_metrics.is_empty()
            || !rule.condition.matches(event)
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_common::UnixTimestamp;
    use relay_dynamic_config::TaggingRule;
    use relay_general::types::Annotated;
    use relay_metrics::MetricValue;

    use super::*;

    #[test]
    fn test_conditional_tagging() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:02+0100",
            "measurements": {
                "lcp": {"value": 41, "unit": "millisecond"}
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![
            Metric {
                name: "d:transactions/measurements.lcp@millisecond".to_owned(),
                value: MetricValue::Distribution(41.0),
                timestamp: UnixTimestamp::from_secs(1619420402),
                tags: Default::default(),
            },
            Metric {
                name: "d:transactions/duration@millisecond".to_owned(),
                value: MetricValue::Distribution(2000.0),
                timestamp: UnixTimestamp::from_secs(1619420402),
                tags: Default::default(),
            },
        ];

        let tagging_config: Vec<TaggingRule> = serde_json::from_str(
            r#"
        [
            {
                "condition": {"op": "gte", "name": "event.duration", "value": 9001},
                "targetMetrics": ["d:transactions/duration@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "frustrated"
            },
            {
                "condition": {"op": "gte", "name": "event.duration", "value": 666},
                "targetMetrics": ["d:transactions/duration@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "tolerated"
            },
            {
                "condition": {"op": "and", "inner": []},
                "targetMetrics": ["d:transactions/duration@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "satisfied"
            }
        ]
        "#,
        )
        .unwrap();

        run_conditional_tagging(event.value().unwrap(), &tagging_config, &mut metrics);

        insta::assert_debug_snapshot!(metrics, @r#"
        [
            Metric {
                name: "d:transactions/measurements.lcp@millisecond",
                value: Distribution(
                    41.0,
                ),
                timestamp: UnixTimestamp(1619420402),
                tags: {},
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420402),
                tags: {
                    "satisfaction": "tolerated",
                },
            },
        ]
        "#);
    }

    #[test]
    fn test_conditional_tagging_lcp() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:02+0100",
            "measurements": {
                "lcp": {"value": 41, "unit": "millisecond"}
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![Metric {
            name: "d:transactions/measurements.lcp@millisecond".to_owned(),
            value: MetricValue::Distribution(41.0),
            timestamp: UnixTimestamp::from_secs(1619420402),
            tags: Default::default(),
        }];

        let tagging_config: Vec<TaggingRule> = serde_json::from_str(
            r#"
        [
            {
                "condition": {"op": "gte", "name": "event.measurements.lcp.value", "value": 41},
                "targetMetrics": ["d:transactions/measurements.lcp@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "frustrated"
            },
            {
                "condition": {"op": "gte", "name": "event.measurements.lcp.value", "value": 20},
                "targetMetrics": ["d:transactions/measurements.lcp@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "tolerated"
            },
            {
                "condition": {"op": "and", "inner": []},
                "targetMetrics": ["d:transactions/measurements.lcp@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "satisfied"
            }
        ]
        "#,
        )
        .unwrap();

        run_conditional_tagging(event.value().unwrap(), &tagging_config, &mut metrics);

        let mut expected_tags = BTreeMap::new();
        expected_tags.insert("satisfaction".to_owned(), "frustrated".to_owned());
        assert_eq!(metrics[0].tags, expected_tags);
    }
}
