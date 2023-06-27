use std::collections::BTreeMap;

use relay_common::{DataCategory, UnixTimestamp};
use relay_dynamic_config::{MetricExtractionConfig, TagSource, TagSpec};
use relay_general::protocol::Event;
use relay_metrics::{Metric, MetricResourceIdentifier, MetricType, MetricValue};
use relay_sampling::FieldValueProvider;

/// Extract metrics from an [`Event`].
///
/// The event must have a valid timestamp; if the timestamp is missing or invalid, no metrics are
/// extracted. Timestamp and clock drift correction should occur before metrics extraction to ensure
/// valid timestamps.
pub fn extract_event_metrics(event: &Event, config: &MetricExtractionConfig) -> Vec<Metric> {
    let mut metrics = Vec::new();

    let event_ts = event.timestamp.value();
    let Some(timestamp) = event_ts.and_then(|d| UnixTimestamp::from_datetime(d.0)) else {
        relay_log::error!(timestamp = ?event_ts, "invalid event timestamp for metric extraction");
        return metrics
    };

    // Obtain the event's data category, but treat default events as error events for the purpose of
    // metric tagging.
    let category = match DataCategory::from(event.ty.value().copied().unwrap_or_default()) {
        DataCategory::Default => DataCategory::Error,
        category => category,
    };

    for metric_spec in &config.metrics {
        if metric_spec.category != category {
            continue;
        }

        if let Some(ref condition) = metric_spec.condition {
            if !condition.matches(event) {
                continue;
            }
        }

        // Parse the MRI so that we can obtain the type, but subsequently re-serialize it into the
        // generated metric to ensure the MRI is normalized.
        let Ok(mri) = MetricResourceIdentifier::parse(&metric_spec.mri) else {
            relay_log::error!(mri=metric_spec.mri, "invalid MRI for metric extraction");
            continue;
        };

        let Some(value) = read_metric_value(event, metric_spec.field.as_deref(), mri.ty) else {
            continue;
        };

        metrics.push(Metric {
            name: mri.to_string(),
            value,
            timestamp,
            tags: extract_event_tags(event, &metric_spec.tags),
        });
    }

    metrics
}

fn extract_event_tags(event: &Event, tags: &[TagSpec]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();

    for tag_spec in tags {
        if let Some(ref condition) = tag_spec.condition {
            if !condition.matches(event) {
                continue;
            }
        }

        let value_opt = match tag_spec.source() {
            TagSource::Literal(value) => Some(value.to_owned()),
            TagSource::Field(field) => event.get_value(field).as_str().map(str::to_owned),
            TagSource::Unknown => None,
        };

        if let Some(value) = value_opt {
            map.insert(tag_spec.key.clone(), value);
        }
    }

    map
}

fn read_metric_value(event: &Event, field: Option<&str>, ty: MetricType) -> Option<MetricValue> {
    Some(match ty {
        MetricType::Counter => MetricValue::Counter(match field {
            Some(field) => event.get_value(field).as_f64()?,
            None => 1.0,
        }),
        MetricType::Distribution => MetricValue::Distribution(event.get_value(field?).as_f64()?),
        MetricType::Set => MetricValue::set_from_str(event.get_value(field?).as_str()?),
        MetricType::Gauge => MetricValue::Gauge(event.get_value(field?).as_f64()?),
    })
}
