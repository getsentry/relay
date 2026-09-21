use relay_conventions::attributes::{
    BROWSER__NAME, BROWSER__NAVIGATION__ID, BROWSER__NAVIGATION__TYPE, BROWSER__VERSION,
    BROWSER__WEB_VITAL__CLS__VALUE, BROWSER__WEB_VITAL__FCP__VALUE, BROWSER__WEB_VITAL__INP__VALUE,
    BROWSER__WEB_VITAL__LCP__ELEMENT, BROWSER__WEB_VITAL__LCP__ID,
    BROWSER__WEB_VITAL__LCP__LOAD_TIME, BROWSER__WEB_VITAL__LCP__RENDER_TIME,
    BROWSER__WEB_VITAL__LCP__SIZE, BROWSER__WEB_VITAL__LCP__URL, BROWSER__WEB_VITAL__LCP__VALUE,
    BROWSER__WEB_VITAL__TTFB__REQUEST_TIME, BROWSER__WEB_VITAL__TTFB__VALUE, SENTRY__DESCRIPTION,
    SENTRY__ENVIRONMENT, SENTRY__METRIC__SOURCE, SENTRY__ORIGIN, SENTRY__PAGELOAD__SPAN_ID,
    SENTRY__PLATFORM, SENTRY__RELEASE, SENTRY__SDK__NAME, SENTRY__SDK__VERSION,
    SENTRY__SEGMENT__NAME, USER_AGENT__ORIGINAL,
};
use relay_conventions::interpolate::browser__web_vital__cls__source__key;
use relay_event_schema::protocol::{Attributes, SpanV2, TraceMetric};
use relay_metrics::MetricUnit;
use relay_protocol::Value;
use std::collections::BTreeMap;

const MAX_CLS_SOURCES: u32 = 128;

const INP_TARGET: &str = "browser.web_vital.inp.target";
const INP_TYPE: &str = "browser.web_vital.inp.type";
const INTERACTION_OP_PREFIX: &str = "ui.interaction.";

#[allow(
    deprecated,
    reason = "Spans converted from a transaction carry the browser name as a legacy span tag."
)]
const LEGACY_BROWSER_NAME: &str = relay_conventions::attributes::SENTRY__BROWSER__NAME;

const WEB_VITAL_SPAN_NAMES: [&str; 7] = [
    "pageload",
    "ui.webvital.lcp",
    "ui.webvital.cls",
    "ui.interaction.click",
    "ui.interaction.hover",
    "ui.interaction.drag",
    "ui.interaction.press",
];

const COMMON_ATTRIBUTES: [&str; 13] = [
    BROWSER__NAME,
    BROWSER__VERSION,
    BROWSER__NAVIGATION__TYPE,
    BROWSER__NAVIGATION__ID,
    SENTRY__PAGELOAD__SPAN_ID,
    SENTRY__ORIGIN,
    SENTRY__SEGMENT__NAME,
    USER_AGENT__ORIGINAL,
    SENTRY__RELEASE,
    SENTRY__ENVIRONMENT,
    SENTRY__SDK__NAME,
    SENTRY__SDK__VERSION,
    SENTRY__PLATFORM,
];

const WEB_VITAL_LOOKUPS: [WebVital; 5] = [
    WebVital {
        attribute_value: BROWSER__WEB_VITAL__LCP__VALUE,
        name: "browser.web_vital.lcp",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            BROWSER__WEB_VITAL__LCP__ELEMENT,
            BROWSER__WEB_VITAL__LCP__ID,
            BROWSER__WEB_VITAL__LCP__URL,
            BROWSER__WEB_VITAL__LCP__SIZE,
            BROWSER__WEB_VITAL__LCP__LOAD_TIME,
            BROWSER__WEB_VITAL__LCP__RENDER_TIME,
            "score.lcp",
            "score.weight.lcp",
            "score.ratio.lcp",
        ],
    },
    WebVital {
        attribute_value: BROWSER__WEB_VITAL__CLS__VALUE,
        name: "browser.web_vital.cls",
        unit: MetricUnit::None,
        attribute_keys: &["score.cls", "score.weight.cls", "score.ratio.cls"],
    },
    WebVital {
        attribute_value: BROWSER__WEB_VITAL__INP__VALUE,
        name: "browser.web_vital.inp",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            INP_TARGET,
            INP_TYPE,
            "score.inp",
            "score.weight.inp",
            "score.ratio.inp",
        ],
    },
    WebVital {
        attribute_value: BROWSER__WEB_VITAL__FCP__VALUE,
        name: "browser.web_vital.fcp",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &["score.fcp", "score.weight.fcp", "score.ratio.fcp"],
    },
    WebVital {
        attribute_value: BROWSER__WEB_VITAL__TTFB__VALUE,
        name: "browser.web_vital.ttfb",
        unit: MetricUnit::Duration(relay_metrics::DurationUnit::MilliSecond),
        attribute_keys: &[
            BROWSER__WEB_VITAL__TTFB__REQUEST_TIME,
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
    let attrs = span.attributes.0.as_ref()?;

    let op_name = attrs.get_value("sentry.op")?.as_str()?;

    if !WEB_VITAL_SPAN_NAMES.contains(&op_name) {
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
        // "source" attributes (with a .N postfix, 1-based, monotonically increasing), so we
        // exhaustively look for them here.
        if web_vital.attribute_value == BROWSER__WEB_VITAL__CLS__VALUE {
            for i in 1..MAX_CLS_SOURCES {
                let attr_key = browser__web_vital__cls__source__key(i.to_string());
                match attrs.get_attribute(&attr_key) {
                    Some(v) => attributes.insert(attr_key, v.value.clone()),
                    None => break,
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

        if !attributes.contains_key(BROWSER__NAME)
            && let Some(v) = attrs.get_attribute(LEGACY_BROWSER_NAME)
        {
            attributes.insert(BROWSER__NAME, v.value.clone());
        }

        // No SDK sends the interaction's element and type as attributes: the element is the span's
        // description, or its name for a streamed span, and the type is part of the op. A name
        // that is just the op was inferred from it and names no element.
        if web_vital.attribute_value == BROWSER__WEB_VITAL__INP__VALUE {
            if !attributes.contains_key(INP_TARGET)
                && let Some(target) = attrs
                    .get_value(SENTRY__DESCRIPTION)
                    .and_then(|v| v.as_str())
                    .or(span.name.as_str().filter(|name| *name != op_name))
            {
                attributes.insert(INP_TARGET, target.to_owned());
            }

            if !attributes.contains_key(INP_TYPE)
                && let Some(ty) = op_name.strip_prefix(INTERACTION_OP_PREFIX)
            {
                attributes.insert(INP_TYPE, ty.to_owned());
            }
        }

        // This is for attribution: we'll be able to tell on a metric if it came from relay.
        attributes.insert(SENTRY__METRIC__SOURCE, "span");

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
    use relay_event_schema::protocol::Span as SpanV1;
    use relay_protocol::{Annotated, SerializableAnnotated};

    use super::*;

    fn inp_attribute<'a>(metrics: &'a [TraceMetric], key: &str) -> Option<&'a str> {
        let metric = metrics
            .iter()
            .find(|m| m.name.as_str() == Some("browser.web_vital.inp"))?;
        metric.attributes.value()?.get_value(key)?.as_str()
    }

    #[test]
    fn test_inp_streamed_span() {
        let span = Annotated::<SpanV2>::from_json(
            r#"{
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": "968cff94913ebb07",
                "name": "body > button#submit",
                "start_timestamp": 1742921669.25,
                "end_timestamp": 1742921669.75,
                "attributes": {
                    "sentry.op": {"type": "string", "value": "ui.interaction.click"},
                    "browser.web_vital.inp.value": {"type": "double", "value": 104.0},
                    "browser.name": {"type": "string", "value": "Chrome"},
                    "browser.version": {"type": "string", "value": "140.0.0"}
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let metrics = extract_web_vital_metrics(&span).unwrap();

        assert_eq!(
            inp_attribute(&metrics, INP_TARGET),
            Some("body > button#submit")
        );
        assert_eq!(inp_attribute(&metrics, INP_TYPE), Some("click"));
        assert_eq!(inp_attribute(&metrics, BROWSER__NAME), Some("Chrome"));
        assert_eq!(inp_attribute(&metrics, BROWSER__VERSION), Some("140.0.0"));
    }

    #[test]
    fn test_inp_legacy_span() {
        let span = Annotated::<SpanV1>::from_json(
            r#"{
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": "968cff94913ebb07",
                "op": "ui.interaction.press",
                "description": "body > input#search",
                "origin": "auto.http.browser.inp",
                "start_timestamp": 1742921669.25,
                "timestamp": 1742921669.75,
                "measurements": {"inp": {"value": 104.0, "unit": "millisecond"}},
                "sentry_tags": {"browser.name": "Safari"}
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let span = crate::span_v1_to_span_v2(span, true);
        let metrics = extract_web_vital_metrics(&span).unwrap();

        assert_eq!(
            inp_attribute(&metrics, INP_TARGET),
            Some("body > input#search")
        );
        assert_eq!(inp_attribute(&metrics, INP_TYPE), Some("press"));
        assert_eq!(inp_attribute(&metrics, BROWSER__NAME), Some("Safari"));
    }

    #[test]
    fn test_inp_without_element() {
        let span = Annotated::<SpanV2>::from_json(
            r#"{
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": "968cff94913ebb07",
                "name": "ui.interaction.drag",
                "start_timestamp": 1742921669.25,
                "end_timestamp": 1742921669.75,
                "attributes": {
                    "sentry.op": {"type": "string", "value": "ui.interaction.drag"},
                    "browser.web_vital.inp.value": {"type": "double", "value": 104.0}
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let metrics = extract_web_vital_metrics(&span).unwrap();

        assert_eq!(inp_attribute(&metrics, INP_TARGET), None);
        assert_eq!(inp_attribute(&metrics, INP_TYPE), Some("drag"));
    }

    #[test]
    fn test_inp_explicit_attributes_win() {
        let span = Annotated::<SpanV2>::from_json(
            r#"{
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": "968cff94913ebb07",
                "name": "body > button#submit",
                "start_timestamp": 1742921669.25,
                "end_timestamp": 1742921669.75,
                "attributes": {
                    "sentry.op": {"type": "string", "value": "ui.interaction.click"},
                    "browser.web_vital.inp.value": {"type": "double", "value": 104.0},
                    "browser.web_vital.inp.target": {"type": "string", "value": "explicit"},
                    "browser.web_vital.inp.type": {"type": "string", "value": "keyboard"}
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let metrics = extract_web_vital_metrics(&span).unwrap();

        assert_eq!(inp_attribute(&metrics, INP_TARGET), Some("explicit"));
        assert_eq!(inp_attribute(&metrics, INP_TYPE), Some("keyboard"));
    }

    #[test]
    fn test_lcp_attributes() {
        let span = Annotated::<SpanV2>::from_json(
            r#"{
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": "968cff94913ebb07",
                "start_timestamp": 1742921669.25,
                "end_timestamp": 1742921669.75,
                "attributes": {
                    "sentry.op": {"type": "string", "value": "ui.webvital.lcp"},
                    "browser.web_vital.lcp.value": {"type": "double", "value": 400.0},
                    "browser.navigation.type": {"type": "string", "value": "soft-navigation"},
                    "browser.navigation.id": {"type": "integer", "value": 2},
                    "unrelated": {"type": "string", "value": "dropped"}
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let metrics = extract_web_vital_metrics(&span).unwrap();
        assert_eq!(metrics.len(), 1);

        insta::assert_json_snapshot!(SerializableAnnotated(&metrics[0].attributes), @r#"
        {
          "browser.navigation.id": {
            "type": "integer",
            "value": 2
          },
          "browser.navigation.type": {
            "type": "string",
            "value": "soft-navigation"
          },
          "sentry.metric.source": {
            "type": "string",
            "value": "span"
          }
        }
        "#);
    }
}
