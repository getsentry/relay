use relay_event_schema::protocol::{Attributes, SpanV2, TraceMetric};
use relay_metrics::MetricUnit;
use relay_protocol::Value;
use std::collections::BTreeMap;

const MAX_CLS_SOURCES: u32 = 128;

const WEB_VITAL_SPAN_NAMES: [&str; 7] = [
    "pageload",
    "ui.webvital.lcp",
    "ui.webvital.cls",
    "ui.interaction.click",
    "ui.interaction.hover",
    "ui.interaction.drag",
    "ui.interaction.press",
];

const COMMON_ATTRIBUTES: [&str; 9] = [
    "sentry.pageload.span_id",
    "sentry.origin",
    "sentry.transaction",
    "user_agent.original",
    "sentry.release",
    "sentry.environment",
    "sentry.sdk.name",
    "sentry.sdk.version",
    "sentry.platform",
];

const WEB_VITAL_LOOKUPS: [WebVital; 5] = [
    WebVital {
        attribute_value: "browser.web_vital.lcp.value",
        name: "browser.web_vital.lcp",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            "browser.web_vital.lcp.element",
            "browser.web_vital.lcp.id",
            "browser.web_vital.lcp.url",
            "browser.web_vital.lcp.size",
            "browser.web_vital.lcp.load_time",
            "browser.web_vital.lcp.render_time",
            "score.lcp",
            "score.weight.lcp",
            "score.ratio.lcp",
        ],
    },
    WebVital {
        attribute_value: "browser.web_vital.cls.value",
        name: "browser.web_vital.cls",
        unit: MetricUnit::None,
        attribute_keys: &["score.cls", "score.weight.cls", "score.ratio.cls"],
    },
    WebVital {
        attribute_value: "browser.web_vital.inp.value",
        name: "browser.web_vital.inp",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            "browser.web_vital.inp.target",
            "browser.web_vital.inp.type",
            "score.inp",
            "score.weight.inp",
            "score.ratio.inp",
        ],
    },
    WebVital {
        attribute_value: "browser.web_vital.fcp.value",
        name: "browser.web_vital.fcp",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &["score.fcp", "score.weight.fcp", "score.ratio.fcp"],
    },
    WebVital {
        attribute_value: "browser.web_vital.ttfb.value",
        name: "browser.web_vital.ttfb",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            "browser.web_vital.ttfb.request_time",
            "score.ttfb",
            "score.ratio.ttfb",
            "score.weight.ttfb",
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
    let Some(attrs) = &span.attributes.0 else {
        return None;
    };

    let op_name = attrs.get_value("sentry.op")?;

    if !WEB_VITAL_SPAN_NAMES.contains(&op_name.as_str().unwrap_or_default()) {
        return None;
    }

    let mut results = vec![];

    for web_vital in &WEB_VITAL_LOOKUPS {
        let Some(value) = attrs.get_value(web_vital.attribute_value) else {
            continue;
        };

        let Some(value) = value.as_f64() else {
            continue;
        };

        let mut attributes = Attributes::new();

        // CLS webvitals are a little weird, in that they can have an arbitrary number of
        // "source" attributes (with a .N postfix, 0-based, monotonically increasing), so we
        // exhaustively look for them here.
        if web_vital.attribute_value == "browser.web_vital.cls.value" {
            for i in 0..MAX_CLS_SOURCES {
                let attr_key = format!("browser.web_vital.cls.source.{i}");
                if let Some(v) = attrs.get_attribute(&attr_key) {
                    attributes.insert(attr_key, v.value.clone());
                } else {
                    break;
                }
            }
        }

        for attr_key in web_vital.attribute_keys {
            if let Some(v) = attrs.get_attribute(*attr_key) {
                attributes.insert(*attr_key, v.value.clone());
            }
        }

        for attr_key in COMMON_ATTRIBUTES {
            if let Some(v) = attrs.get_attribute(attr_key) {
                attributes.insert(attr_key, v.value.clone());
            }
        }

        // This is for attribution: we'll be able to tell on a metric if it came from relay.
        attributes.insert("sentry.metric.source", "span");

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
