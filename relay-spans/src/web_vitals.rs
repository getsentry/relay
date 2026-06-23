use relay_event_schema::protocol::{Attributes, SpanV2, TraceMetric};
use relay_metrics::MetricUnit;
use relay_protocol::Value;
use std::collections::BTreeMap;

const WEB_VITAL_SPAN_NAMES: [&'static str; 7] = [
    "pageload",
    "ui.webvital.lcp",
    "ui.webvital.cls",
    "ui.interaction.click",
    "ui.interaction.hover",
    "ui.interaction.drag",
    "ui.interaction.press",
];

const WEB_VITAL_LOOKUPS: [WebVital; 5] = [
    WebVital {
        attribute_value: "browser.web_vital.lcp.value",
        name: "browser.web_vital.lcp.value",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            "browser.web_vital.lcp.value",
            "browser.web_vital.lcp.element",
            "browser.web_vital.lcp.id",
            "browser.web_vital.lcp.url",
            "browser.web_vital.lcp.size",
            "browser.web_vital.lcp.load_time",
            "browser.web_vital.lcp.render_time",
        ],
    },
    WebVital {
        attribute_value: "browser.web_vital.cls.value",
        name: "browser.web_vital.cls.value",
        unit: MetricUnit::None,
        attribute_keys: &[
            "browser.web_vital.cls.value",
            "browser.web_vital.cls.source.1",
            "browser.web_vital.cls.source.2",
        ],
    },
    WebVital {
        attribute_value: "browser.web_vital.inp.value",
        name: "browser.web_vital.inp.value",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &["browser.web_vital.inp.value"],
    },
    WebVital {
        attribute_value: "browser.web_vital.fcp.value",
        name: "browser.web_vital.fcp.value",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &["browser.web_vital.fcp.value"],
    },
    WebVital {
        attribute_value: "browser.web_vital.ttfb.value",
        name: "browser.web_vital.ttfb.value",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            "browser.web_vital.ttfb.value",
            "browser.web_vital.ttfb.request_time",
        ],
    },
];

struct WebVital {
    name: &'static str,
    attribute_value: &'static str,
    unit: MetricUnit,
    attribute_keys: &'static [&'static str],
}

/// Extract any web vitals metrics for the supplied v2 span.  Bad or missing metrics will be
/// silently dropped.
pub fn extract_web_vital_metrics(span: &SpanV2) -> Option<Vec<TraceMetric>> {
    if let Some(name) = span.name.value() {
        if !WEB_VITAL_SPAN_NAMES.contains(&name.as_str()) {
            return None;
        }
    }

    let Some(attrs) = &span.attributes.0 else {
        return None;
    };

    let mut results = vec![];

    for web_vital in &WEB_VITAL_LOOKUPS {
        let Some(value) = attrs.get_value(web_vital.attribute_value) else {
            continue;
        };

        let Some(value) = value.as_f64() else {
            continue;
        };

        let mut attributes = Attributes::new();
        for attr_key in web_vital.attribute_keys {
            if let Some(v) = attrs.get_attribute(*attr_key) {
                attributes.insert(*attr_key, v.value.clone());
            }
        }
        let trace_metric = TraceMetric {
            timestamp: span.start_timestamp.clone(),
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            name: web_vital.name.to_owned().into(),
            ty: relay_event_schema::protocol::MetricType::Distribution.into(),
            unit: web_vital.unit.into(),
            value: Value::F64(value).into(),
            attributes: attributes.into(),
            other: BTreeMap::default(),
        };

        results.push(trace_metric);
    }

    if results.is_empty() {
        None
    } else {
        Some(results)
    }
}
