use relay_event_schema::processor::{ProcessValue, ValueType};
use relay_event_schema::protocol::{
    Attribute, AttributeType, Attributes, Event, OurLog, Span, SpanData, SpanV2, TraceMetric,
};
use relay_protocol::{Annotated, Value};
use serde_json::json;

use crate::PiiConfig;

fn config(selector: &str) -> PiiConfig {
    serde_json::from_value(json!({
        "rules": {"redact": {"type": "anything", "redaction": {"method": "replace", "text": "[scrubbed]"}}},
        "applications": {selector: ["redact"]}
    })).unwrap()
}

fn scrub_item<T: ProcessValue>(item: T, root: ValueType, selector: &str, advanced: bool) -> T {
    let mut item = Annotated::new(item);
    let config = config(selector);
    let (advanced_config, legacy_config) = if advanced {
        (Some(&config), None)
    } else {
        (None, Some(&config))
    };
    super::scrub(root, &mut item, advanced_config, legacy_config).unwrap();
    item.into_value().unwrap()
}

fn scrub_attributes(
    attributes: Attributes,
    root: ValueType,
    selector: &str,
    advanced: bool,
) -> Attributes {
    let attributes = Annotated::new(attributes);
    match root {
        ValueType::Span => {
            scrub_item(
                SpanV2 {
                    attributes,
                    ..Default::default()
                },
                root,
                selector,
                advanced,
            )
            .attributes
        }
        ValueType::OurLog => {
            scrub_item(
                OurLog {
                    attributes,
                    ..Default::default()
                },
                root,
                selector,
                advanced,
            )
            .attributes
        }
        ValueType::TraceMetric => {
            scrub_item(
                TraceMetric {
                    attributes,
                    ..Default::default()
                },
                root,
                selector,
                advanced,
            )
            .attributes
        }
        _ => unreachable!(),
    }
    .into_value()
    .unwrap()
}

fn attributes(keys: &[&str]) -> Attributes {
    let mut attributes = Attributes::new();
    for (index, key) in keys.iter().enumerate() {
        attributes.insert(*key, format!("secret-{index}"));
    }
    attributes.insert("unrelated", "untouched");
    attributes
}

#[test]
fn compatible_attribute_selectors_scrub_both_directions_and_all_copies() {
    for (current, alias) in [
        ("db.query.text", "db.statement"),
        ("http.request.method", "http.method"),
        ("gen_ai.request.model", "ai.model_id"),
        ("sentry.segment.name", "sentry.transaction"),
        ("browser.web_vital.cls.source.1", "cls.source.1"),
        ("url.path.parameter.user", "url.path.params.user"),
    ] {
        for selector_key in [current, alias] {
            for keys in [vec![current], vec![alias], vec![current, alias]] {
                for advanced in [false, true] {
                    for root in [ValueType::Span, ValueType::OurLog, ValueType::TraceMetric] {
                        let selector = format!(
                            "${root}.attributes.'{selector_key}'{}",
                            if advanced { ".value" } else { "" }
                        );
                        let result = scrub_attributes(attributes(&keys), root, &selector, advanced);
                        assert_eq!(result.0.len(), keys.len() + 1);
                        for key in &keys {
                            let value = result.get_annotated_value(*key).unwrap();
                            assert_eq!(
                                value.value().and_then(Value::as_str),
                                Some("[scrubbed]"),
                                "{root} {advanced} {selector}: {key}"
                            );
                            assert!(
                                value
                                    .meta()
                                    .iter_remarks()
                                    .any(|remark| remark.rule_id() == "redact")
                            );
                        }
                        assert_eq!(
                            result.get_value("unrelated").and_then(Value::as_str),
                            Some("untouched")
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_case_and_camel_case_aliases_match_without_renaming() {
    for (current, alias) in [
        ("browser.web_vital.lcp.load_time", "lcp.loadTime"),
        ("gen_ai.tool.name", "mcp.tool.name"),
        ("browser.web_vital.cls.source.Main", "cls.source.Main"),
    ] {
        for (selector_key, field_key) in [(current, alias), (alias, current)] {
            for selector_key in [
                selector_key.to_owned(),
                selector_key.to_ascii_uppercase(),
                selector_key.to_ascii_lowercase(),
            ] {
                for field_key in [
                    field_key.to_owned(),
                    field_key.to_ascii_uppercase(),
                    field_key.to_ascii_lowercase(),
                ] {
                    for advanced in [false, true] {
                        let selector = format!(
                            "$span.attributes.'{selector_key}'{}",
                            if advanced { ".value" } else { "" }
                        );
                        let result = scrub_attributes(
                            attributes(&[&field_key]),
                            ValueType::Span,
                            &selector,
                            advanced,
                        );
                        assert_eq!(
                            result.get_value(&field_key).and_then(Value::as_str),
                            Some("[scrubbed]")
                        );
                        assert_eq!(result.0.len(), 2);
                    }
                }
            }
        }
    }
}

#[test]
fn different_dynamic_fragments_are_not_equivalent() {
    for advanced in [false, true] {
        let result = scrub_attributes(
            attributes(&["cls.source.1", "cls.source.2"]),
            ValueType::Span,
            &format!(
                "$span.attributes.'browser.web_vital.cls.source.1'{}",
                if advanced { ".value" } else { "" }
            ),
            advanced,
        );
        assert_eq!(
            result.get_value("cls.source.1").and_then(Value::as_str),
            Some("[scrubbed]")
        );
        assert_eq!(
            result.get_value("cls.source.2").and_then(Value::as_str),
            Some("secret-1")
        );
    }
}

#[test]
fn transformations_and_incompatible_types_are_not_matched_as_aliases() {
    for (old, current) in [
        ("gen_ai.request.messages", "gen_ai.input.messages"),
        ("gen_ai.response.text", "gen_ai.output.messages"),
        ("gen_ai.response.tool_calls", "gen_ai.output.messages"),
        ("hardwareConcurrency", "device.processor_count"),
        ("ai.tools", "gen_ai.tool.definitions"),
        ("service.version", "sentry.release"),
    ] {
        for (selector_key, field_key) in [(old, current), (current, old)] {
            for advanced in [false, true] {
                let original = attributes(&[field_key]);
                let selector = format!(
                    "$span.attributes.'{selector_key}'{}",
                    if advanced { ".value" } else { "" }
                );
                assert_eq!(
                    scrub_attributes(original.clone(), ValueType::Span, &selector, advanced),
                    original
                );
            }
        }
    }
}

#[test]
fn never_and_manual_scrubbing_policies_are_unchanged() {
    for advanced in [false, true] {
        for selector_key in ["release", "sentry.release"] {
            let original = attributes(&["release", "sentry.release"]);
            let selector = format!(
                "$span.attributes.'{selector_key}'{}",
                if advanced { ".value" } else { "" }
            );
            assert_eq!(
                scrub_attributes(original.clone(), ValueType::Span, &selector, advanced),
                original
            );
        }
        let result = scrub_attributes(
            attributes(&[
                "ai.model_id",
                "gen_ai.request.model",
                "release",
                "sentry.release",
            ]),
            ValueType::Span,
            "$string",
            advanced,
        );
        for key in [
            "ai.model_id",
            "gen_ai.request.model",
            "release",
            "sentry.release",
        ] {
            assert!(
                result
                    .get_value(key)
                    .and_then(Value::as_str)
                    .unwrap()
                    .starts_with("secret-")
            );
        }
        assert_eq!(
            result.get_value("unrelated").and_then(Value::as_str),
            Some("[scrubbed]")
        );
    }
}

#[test]
fn boolean_and_wildcard_selectors_keep_alias_context() {
    for selector in [
        "$span.*.'db.query.text'.value",
        "$span.attributes.'db.query.text'.value && !$span.attributes.'db.operation.name'.value",
        "$span.attributes.'db.query.text'.value || $span.attributes.'db.operation.name'.value",
    ] {
        let result = scrub_attributes(
            attributes(&["db.statement", "db.query.text"]),
            ValueType::Span,
            selector,
            true,
        );
        for key in ["db.statement", "db.query.text"] {
            assert_eq!(
                result.get_value(key).and_then(Value::as_str),
                Some("[scrubbed]")
            );
        }
    }
    let result = scrub_attributes(
        attributes(&["db.statement", "db.query.text", "db.operation"]),
        ValueType::Span,
        "$span.attributes.*.value && !$span.attributes.'db.query.text'.value",
        true,
    );
    assert_eq!(
        result.get_value("db.statement").and_then(Value::as_str),
        Some("secret-0")
    );
    assert_eq!(
        result.get_value("db.query.text").and_then(Value::as_str),
        Some("secret-1")
    );
    assert_eq!(
        result.get_value("db.operation").and_then(Value::as_str),
        Some("[scrubbed]")
    );
    let result = scrub_attributes(
        attributes(&["url.path.params.id"]),
        ValueType::Span,
        "$span.**.'url.path.parameter.id'.value",
        true,
    );
    assert_eq!(
        result
            .get_value("url.path.params.id")
            .and_then(Value::as_str),
        Some("[scrubbed]")
    );
}

#[test]
fn legacy_span_data_and_trace_context_are_scoped_away_from_arbitrary_user_data() {
    let event_json = json!({
        "contexts": {"trace": {"data": {"db.statement": "trace secret", "db.query.text": "trace current"}}},
        "spans": [{"data": {"db.statement": "span secret", "db.query.text": "span current"}}],
        "extra": {
            "db.statement": "extra secret",
            "db.query.text": "exact key control",
            "attributes": {"db.statement": "fake attributes"},
            "data": {"db.statement": "fake data"}
        },
        "request": {"data": {"db.statement": "request secret"}}
    });
    for advanced in [false, true] {
        let event = Annotated::<Event>::from_json(&event_json.to_string())
            .unwrap()
            .into_value()
            .unwrap();
        let event = scrub_item(event, ValueType::Event, "'db.query.text'", advanced);
        let json: serde_json::Value =
            serde_json::from_str(&Annotated::new(event).to_json().unwrap()).unwrap();
        for key in ["db.statement", "db.query.text"] {
            assert_eq!(json["contexts"]["trace"]["data"][key], "[scrubbed]");
            assert_eq!(json["spans"][0]["data"][key], "[scrubbed]");
        }
        assert_eq!(json["extra"]["db.statement"], "extra secret");
        assert_eq!(json["extra"]["db.query.text"], "[scrubbed]");
        assert_eq!(
            json["extra"]["attributes"]["db.statement"],
            "fake attributes"
        );
        assert_eq!(json["extra"]["data"]["db.statement"], "fake data");
        assert_eq!(json["request"]["data"]["db.statement"], "request secret");
    }
}

#[test]
fn aliases_do_not_apply_to_nested_attribute_values_or_attribute_metadata() {
    for advanced in [false, true] {
        let raw = json!({
            "custom": {"type": "unknown", "value": {"db.statement": "nested secret", "attributes": {"db.statement": "nested fake attributes"}}},
            "metadata": {"type": "string", "value": "untouched", "db.statement": "metadata secret"}
        });
        let original = Annotated::<Attributes>::from_json(&raw.to_string())
            .unwrap()
            .into_value()
            .unwrap();
        let result = scrub_attributes(
            original.clone(),
            ValueType::Span,
            "'db.query.text'",
            advanced,
        );
        assert_eq!(result, original);
        let selector = if advanced {
            "$span.attributes.custom.value.'db.query.text'"
        } else {
            "$span.attributes.custom.'db.query.text'"
        };
        assert_eq!(
            scrub_attributes(original.clone(), ValueType::Span, selector, advanced),
            original
        );
    }
}

#[test]
fn removal_rules_scrub_every_copy_and_original_value_metadata() {
    for advanced in [false, true] {
        let mut attributes = attributes(&["db.statement", "db.query.text"]);
        for key in ["db.statement", "db.query.text"] {
            attributes
                .get_annotated_value_mut(key)
                .unwrap()
                .meta_mut()
                .set_original_value(Some("original secret".to_owned()));
        }
        let selector = format!("'db.query.text'{}", if advanced { ".value" } else { "" });
        let config: PiiConfig = serde_json::from_value(json!({
            "rules": {"remove": {"type": "anything", "redaction": {"method": "remove"}}},
            "applications": {selector: ["remove"]}
        }))
        .unwrap();
        let mut span = Annotated::new(SpanV2 {
            attributes: Annotated::new(attributes),
            ..Default::default()
        });
        let (advanced_config, legacy_config) = if advanced {
            (Some(&config), None)
        } else {
            (None, Some(&config))
        };
        super::scrub(ValueType::Span, &mut span, advanced_config, legacy_config).unwrap();
        let result = span.value().unwrap().attributes.value().unwrap();
        for key in ["db.statement", "db.query.text"] {
            let value = result.get_annotated_value(key).unwrap();
            assert!(value.value().is_none());
            assert!(value.meta().original_value().is_none());
            assert!(
                value
                    .meta()
                    .iter_remarks()
                    .any(|remark| remark.rule_id() == "remove")
            );
        }
    }
}

#[test]
fn replacement_rules_scrub_original_metadata_in_attributes_and_legacy_data() {
    for advanced in [false, true] {
        for key in ["db.statement", "db.query.text"] {
            let mut attributes = attributes(&[key]);
            attributes
                .get_annotated_value_mut(key)
                .unwrap()
                .meta_mut()
                .set_original_value(Some("original secret".to_owned()));
            let selector = format!("'db.query.text'{}", if advanced { ".value" } else { "" });
            let attributes = scrub_attributes(attributes, ValueType::Span, &selector, advanced);
            let value = attributes.get_annotated_value(key).unwrap();
            assert_eq!(value.value().and_then(Value::as_str), Some("[scrubbed]"));
            assert_eq!(
                value.meta().original_value().and_then(Value::as_str),
                Some("[scrubbed]")
            );
            let mut data = SpanData::default();
            let mut value = Annotated::new(Value::String("visible secret".to_owned()));
            value
                .meta_mut()
                .set_original_value(Some("original secret".to_owned()));
            data.other.insert(key.to_owned(), value);
            let span = scrub_item(
                Span {
                    data: Annotated::new(data),
                    ..Default::default()
                },
                ValueType::Span,
                "'db.query.text'",
                advanced,
            );
            let value = span.data.value().unwrap().get(key).unwrap();
            assert_eq!(value.value().and_then(Value::as_str), Some("[scrubbed]"));
            assert_eq!(
                value.meta().original_value().and_then(Value::as_str),
                Some("[scrubbed]")
            );
        }
    }
}

#[test]
fn indexed_original_metadata_keeps_its_exact_path() {
    for advanced in [false, true] {
        for key in ["db.statement", "db.query.text"] {
            let mut first = Annotated::new(Value::String("visible secret".to_owned()));
            first
                .meta_mut()
                .set_original_value(Some("original secret".to_owned()));
            let value = Value::Array(vec![
                first,
                Annotated::new(Value::String("untouched".to_owned())),
            ]);
            let mut attributes = Attributes::new();
            attributes.0.insert(
                key.to_owned(),
                Annotated::new(Attribute::new(AttributeType::Array, value)),
            );
            let selector = format!("'db.query.text'{}.0", if advanced { ".value" } else { "" });
            let attributes = scrub_attributes(attributes, ValueType::Span, &selector, advanced);
            let Some(Value::Array(values)) = attributes.get_value(key) else {
                panic!("expected array")
            };
            assert_eq!(
                values[0].value().and_then(Value::as_str),
                Some("[scrubbed]")
            );
            assert_eq!(
                values[0].meta().original_value().and_then(Value::as_str),
                Some("[scrubbed]")
            );
            assert_eq!(values[1].value().and_then(Value::as_str), Some("untouched"));
        }
    }
}

#[test]
fn aliases_work_for_numeric_and_array_values_and_tolerate_missing_values() {
    let raw = json!({
        "port": {"type": "integer", "value": 5432},
        "server.port": {"type": "integer", "value": 5672},
        "db.query.summary": {"type": "string"},
        "custom.null": null,
        "db.statement": {"type": "array", "value": ["secret", "other secret"]},
        "db.query.text": {"type": "array", "value": ["another secret"]}
    });
    for advanced in [false, true] {
        let attributes = Annotated::<Attributes>::from_json(&raw.to_string())
            .unwrap()
            .into_value()
            .unwrap();
        let selector = format!("'server.port'{}", if advanced { ".value" } else { "" });
        let attributes = scrub_attributes(attributes, ValueType::Span, &selector, advanced);
        for key in ["port", "server.port"] {
            assert!(attributes.get_value(key).is_none());
        }
        assert!(attributes.get_value("db.query.summary").is_none());
        let selector = format!("'db.query.text'{}", if advanced { ".value" } else { "" });
        let attributes = scrub_attributes(attributes, ValueType::Span, &selector, advanced);
        assert!(attributes.get_value("db.statement").is_none());
        assert!(attributes.get_value("db.query.text").is_none());
    }
}

#[test]
fn legacy_span_link_attributes_are_scoped_to_the_declared_field() {
    let raw = json!({
        "links": [{
            "attributes": {"db.statement": "link secret", "db.query.text": "current link secret"},
            "extra": {"db.statement": "extra secret", "attributes": {"db.statement": "fake attributes"}},
            "db.statement": "link extra"
        }],
        "data": {"db.statement": "span secret"},
        "db.statement": "span extra"
    });
    for advanced in [false, true] {
        let span = Annotated::<Span>::from_json(&raw.to_string())
            .unwrap()
            .into_value()
            .unwrap();
        let span = scrub_item(span, ValueType::Span, "'db.query.text'", advanced);
        let json: serde_json::Value =
            serde_json::from_str(&Annotated::new(span).to_json().unwrap()).unwrap();
        assert_eq!(json["links"][0]["attributes"]["db.statement"], "[scrubbed]");
        assert_eq!(
            json["links"][0]["attributes"]["db.query.text"],
            "[scrubbed]"
        );
        assert_eq!(json["links"][0]["extra"]["db.statement"], "extra secret");
        assert_eq!(
            json["links"][0]["extra"]["attributes"]["db.statement"],
            "fake attributes"
        );
        assert_eq!(json["links"][0]["db.statement"], "link extra");
        assert_eq!(json["data"]["db.statement"], "[scrubbed]");
        assert_eq!(json["db.statement"], "span extra");
    }
}

#[test]
fn span_link_attributes_are_scrubbed_independently_of_siblings() {
    let span_json = json!({
        "attributes": {"db.statement": {"type": "string", "value": "span secret"}},
        "links": [{"attributes": {"db.statement": {"type": "string", "value": "link secret"}}}],
        "db.statement": "span extra"
    });
    for advanced in [false, true] {
        let span = Annotated::<SpanV2>::from_json(&span_json.to_string())
            .unwrap()
            .into_value()
            .unwrap();
        let selector = format!("'db.query.text'{}", if advanced { ".value" } else { "" });
        let span = scrub_item(span, ValueType::Span, &selector, advanced);
        let json: serde_json::Value =
            serde_json::from_str(&Annotated::new(span).to_json().unwrap()).unwrap();
        assert_eq!(json["attributes"]["db.statement"]["value"], "[scrubbed]");
        assert_eq!(
            json["links"][0]["attributes"]["db.statement"]["value"],
            "[scrubbed]"
        );
        assert_eq!(json["db.statement"], "span extra");
    }
}
