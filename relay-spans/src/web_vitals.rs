use relay_event_schema::protocol::{Attributes, SpanV2, TraceMetric};
use relay_metrics::MetricUnit;
use relay_protocol::Value;
use std::collections::BTreeMap;

const MAX_CLS_SOURCES: u32 = 128;

const WEB_VITAL_SPAN_NAMES: [&'static str; 7] = [
    "pageload",
    "ui.webvital.lcp",
    "ui.webvital.cls",
    "ui.interaction.click",
    "ui.interaction.hover",
    "ui.interaction.drag",
    "ui.interaction.press",
];

const COMMON_ATTRIBUTES: [&'static str; 9] = [
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span_v1_to_span_v2;
    use relay_event_schema::protocol::{Event, Span};
    use relay_protocol::Annotated;

    /// Returns the string value of attribute `key` on the metric, if present.
    fn attr<'a>(metric: &'a TraceMetric, key: &str) -> Option<&'a str> {
        metric
            .attributes
            .value()?
            .get_value(key)
            .and_then(Value::as_str)
    }

    /// Finds the LCP web vital metric in the produced set.
    fn lcp_metric(metrics: &[TraceMetric]) -> &TraceMetric {
        metrics
            .iter()
            .find(|m| m.name.value().map(String::as_str) == Some("browser.web_vital.lcp"))
            .expect("expected an LCP web vital metric")
    }

    /// Span V2 ingest path (`spans/mod.rs`): a browser SDK sends the SDK attributes directly on
    /// the span, so extraction must copy them onto the produced metric.
    #[test]
    fn native_v2_span_carries_sdk_attributes() {
        let json = r#"{
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "start_timestamp": 1544719859.0,
            "end_timestamp": 1544719860.0,
            "name": "pageload",
            "attributes": {
                "browser.web_vital.lcp.value": {"type": "double", "value": 2500.0},
                "sentry.release": {"type": "string", "value": "myapp@1.0.0"},
                "sentry.environment": {"type": "string", "value": "prod"},
                "sentry.sdk.name": {"type": "string", "value": "sentry.javascript.react"},
                "sentry.sdk.version": {"type": "string", "value": "9.1.0"}
            }
        }"#;
        let span = Annotated::<SpanV2>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        let metrics = extract_web_vital_metrics(&span).expect("expected metrics");
        let lcp = lcp_metric(&metrics);

        assert_eq!(attr(lcp, "sentry.release"), Some("myapp@1.0.0"));
        assert_eq!(attr(lcp, "sentry.environment"), Some("prod"));
        assert_eq!(
            attr(lcp, "sentry.sdk.name"),
            Some("sentry.javascript.react")
        );
        assert_eq!(attr(lcp, "sentry.sdk.version"), Some("9.1.0"));
    }

    /// If the SDK does not send the attributes, extraction cannot invent them. This documents the
    /// honest caveat that the Span V2 path only carries SDK info when the client provides it.
    #[test]
    fn native_v2_span_without_sdk_attributes_omits_them() {
        let json = r#"{
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "start_timestamp": 1544719859.0,
            "end_timestamp": 1544719860.0,
            "name": "pageload",
            "attributes": {
                "browser.web_vital.lcp.value": {"type": "double", "value": 2500.0}
            }
        }"#;
        let span = Annotated::<SpanV2>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        let metrics = extract_web_vital_metrics(&span).expect("expected metrics");
        let lcp = lcp_metric(&metrics);

        assert_eq!(attr(lcp, "sentry.release"), None);
        assert_eq!(attr(lcp, "sentry.environment"), None);
        assert_eq!(attr(lcp, "sentry.sdk.name"), None);
        assert_eq!(attr(lcp, "sentry.sdk.version"), None);
    }

    /// Standalone legacy span path (`legacy_spans/mod.rs`): a v1 span arrives on its own carrying
    /// the SDK info in its `data` bag. `span_v1_to_span_v2` maps those `data` fields to `sentry.*`
    /// attributes, which extraction must copy onto the metric.
    #[test]
    fn standalone_legacy_span_carries_sdk_attributes() {
        let json = r#"{
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "start_timestamp": 1544719859.0,
            "timestamp": 1544719860.0,
            "data": {
                "sentry.name": "pageload",
                "browser.web_vital.lcp.value": 2500.0,
                "sentry.release": "myapp@1.0.0",
                "sentry.environment": "prod",
                "sentry.sdk.name": "sentry.javascript.react",
                "sentry.sdk.version": "9.1.0"
            }
        }"#;
        let span_v1 = Annotated::<Span>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();
        let span_v2 = span_v1_to_span_v2(span_v1, true);

        assert_eq!(span_v2.name.value().map(String::as_str), Some("pageload"));

        let metrics = extract_web_vital_metrics(&span_v2).expect("expected metrics");
        let lcp = lcp_metric(&metrics);

        assert_eq!(attr(lcp, "sentry.release"), Some("myapp@1.0.0"));
        assert_eq!(attr(lcp, "sentry.environment"), Some("prod"));
        assert_eq!(
            attr(lcp, "sentry.sdk.name"),
            Some("sentry.javascript.react")
        );
        assert_eq!(attr(lcp, "sentry.sdk.version"), Some("9.1.0"));
    }

    /// Transaction path (`transactions/types/output.rs`): a transaction event carries
    /// release/environment/sdk at the *event* level. `Span::from(&Event)` copies them into
    /// `SpanData`, and `span_v1_to_span_v2` maps those into `sentry.*` attributes. This drives the
    /// real conversion chain and asserts the attributes reach the produced metric.
    #[test]
    fn transaction_derived_span_carries_sdk_attributes() {
        let event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "platform": "javascript",
                "sdk": {"name": "sentry.javascript.react", "version": "9.1.0"},
                "release": "myapp@1.0.0",
                "environment": "prod",
                "transaction": "pageload",
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74052",
                        "type": "trace",
                        "op": "pageload",
                        "data": {
                            "browser.web_vital.lcp.value": 2500.0
                        }
                    }
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let span_v1 = Span::from(&event);
        let span_v2 = span_v1_to_span_v2(span_v1, true);

        // The transaction name becomes the span name; "pageload" marks this as a web vital span.
        assert_eq!(span_v2.name.value().map(String::as_str), Some("pageload"));

        let metrics = extract_web_vital_metrics(&span_v2).expect("expected metrics");
        let lcp = lcp_metric(&metrics);

        assert_eq!(attr(lcp, "sentry.release"), Some("myapp@1.0.0"));
        assert_eq!(attr(lcp, "sentry.environment"), Some("prod"));
        assert_eq!(
            attr(lcp, "sentry.sdk.name"),
            Some("sentry.javascript.react")
        );
        assert_eq!(attr(lcp, "sentry.sdk.version"), Some("9.1.0"));
    }
}
