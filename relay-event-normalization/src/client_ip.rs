//! Resolve the client address and finish its derived identity before span tag extraction.
use std::net::IpAddr;

use relay_conventions::attributes::{
    CLIENT__ADDRESS, CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION, USER__IP_ADDRESS,
};
use relay_event_schema::processor::{ProcessingAction, ProcessingResult, ValueType};
use relay_event_schema::protocol::{
    Attributes, AutoInferSetting, ClientSdkInfo, Context, Event, EventType, IpAddr as ProtocolIp,
    ParsedIp, Request, SpanData, SpanV2, Tags, User,
};
use relay_pii::{PiiProcessor, ProjectPii, user_needs_id};
use relay_protocol::{Annotated, Object, RemarkType, Value};

use self::Unavailable::{Missing, Redacted};
use crate::{AsnLookup, CityLookup, NormalizationConfig};

const ASN_KEYS: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().next().is_some()
}

pub fn unredacted_value<T>(field: &Annotated<T>) -> Option<&T> {
    field.value().filter(|_| !redacted(field))
}

/// Use the original address for enrichment, then publish only identity allowed by both
/// the source and destination PII rules.
pub fn process_client_ip(
    event: &mut Event,
    config: &NormalizationConfig<'_>,
    rules: ProjectPii<'_>,
) -> ProcessingResult {
    let lookup = process_user_ip(
        &mut event.request,
        &mut event.user,
        ClientIpConfig {
            platform: event.platform.as_str(),
            sdk: event.client_sdk.value(),
            client_ip: config.client_ip,
            geoip_lookup: config.geoip_lookup,
            infer_ip_address: config.infer_ip_address,
            security_report: event.csp.value().is_some(),
        },
        rules,
    )?;
    if let Some(db) = config.asn_lookup {
        enrich_event_asn(event, lookup, db, rules);
    }
    Ok(())
}

/// Inputs used to resolve a user IP on events and replays.
pub struct ClientIpConfig<'a> {
    pub platform: Option<&'a str>,
    pub sdk: Option<&'a ClientSdkInfo>,
    pub client_ip: Option<IpAddr>,
    pub geoip_lookup: Option<&'a CityLookup>,
    pub infer_ip_address: bool,
    pub security_report: bool,
}

/// A missing field permits fallback. An upstream redaction stops it, even on a parent.
enum Unavailable {
    Missing,
    Redacted,
}

fn field_value_mut<T>(field: &mut Annotated<T>) -> Result<&mut T, Unavailable> {
    if redacted(field) {
        return Err(Redacted);
    }
    field.value_mut().as_mut().ok_or(Missing)
}

/// Expand a request marker even when the user already has an IP. Leave literals unparsed.
fn expand_remote_addr(env: &mut Object<Value>, transport: Option<IpAddr>) -> Option<IpAddr> {
    let field = field_value_mut(env.get_mut("REMOTE_ADDR")?).ok()?;
    if field.as_str() != Some(ProtocolIp::AUTO_MARKER) {
        return None;
    }
    if let Some(ip) = transport {
        *field = Value::String(ip.to_string());
    } else {
        env.remove("REMOTE_ADDR");
    }
    transport
}

/// Normalize the request and user in place and return the address eligible for lookup.
pub fn process_user_ip(
    request: &mut Annotated<Request>,
    user: &mut Annotated<User>,
    config: ClientIpConfig<'_>,
    rules: ProjectPii<'_>,
) -> Result<Option<IpAddr>, ProcessingAction> {
    let infer = config
        .sdk
        .and_then(|sdk| sdk.settings.value())
        .map(|settings| settings.infer_ip())
        .unwrap_or_default();
    let transport = config.client_ip.filter(|_| {
        infer != AutoInferSetting::Never
            && (config.infer_ip_address
                || rules.advanced.is_some_and(|r| !r.applications.is_empty()))
    });
    let mut env = field_value_mut(request).and_then(|r| field_value_mut(&mut r.env));
    let expanded_ip = env
        .as_deref_mut()
        .ok()
        .and_then(|env| expand_remote_addr(env, transport));
    // Only a raw address taken from the request must wait for request PII before it can be copied.
    let (lookup, request_backfill, hash_only) = 'select: {
        let supplied = field_value_mut(user).and_then(|u| {
            let parsed = field_value_mut(&mut u.ip_address)?.parsed();
            Ok((parsed, &mut u.ip_address))
        });
        let fallback_connection = match supplied {
            Ok((ParsedIp::Address(ip), _)) => break 'select (Some(ip), None, false),
            Ok((ParsedIp::Replacement, _)) | Err(Redacted) => break 'select (None, None, false),
            Ok((ParsedIp::Auto, field)) => {
                // Consume the marker even if no eligible address is available.
                field.set_value(None);
                transport
            }
            // Legacy JavaScript and Apple SDKs infer a missing user IP from the connection.
            Err(Missing) => transport.filter(|_| {
                infer == AutoInferSetting::Auto
                    || matches!(config.platform, Some("javascript" | "cocoa" | "objc"))
            }),
        };

        // Browser CSP reports prefer the connection. Other events prefer a supplied REMOTE_ADDR.
        let ip = if let Some(ip) =
            transport.filter(|_| config.security_report && config.infer_ip_address)
        {
            ip
        } else {
            let request_ip = if let Some(ip) = expanded_ip {
                Some(ProtocolIp::from(ip))
            } else {
                match env
                    .and_then(|env| field_value_mut(env.get_mut("REMOTE_ADDR").ok_or(Missing)?))
                {
                    Err(Redacted) => break 'select (None, None, false),
                    Ok(Value::String(text)) if infer != AutoInferSetting::Never => {
                        ProtocolIp::parse(text.as_str()).ok()
                    }
                    _ => None,
                }
            };
            if let Some(ip) = request_ip {
                break 'select (ip.address(), Some(ip), false);
            }
            let Some(ip) = fallback_connection else {
                // City and ASN may still use the connection without storing a user IP.
                break 'select (config.client_ip, None, false);
            };
            ip
        };
        user.get_or_insert_with(User::default)
            .ip_address
            .set_value(Some(ProtocolIp::from(ip)));
        // With IP storage disabled, only a PII hash may become a user identifier.
        (Some(ip), None, !config.infer_ip_address)
    };
    rules.scrub_request_env(request)?;
    if let Some(ip) = request_backfill
        && request
            .value()
            .and_then(|r| r.env.value()?.get("REMOTE_ADDR"))
            .and_then(unredacted_value)
            .and_then(Value::as_str)
            .is_some()
    {
        user.get_or_insert_with(User::default)
            .ip_address
            .set_value(Some(ip));
    }
    let lookup_city = || config.geoip_lookup?.lookup(lookup?);
    let user = match user.value_mut() {
        Some(user) => {
            if user.geo.value().is_none() && !redacted(&user.geo) {
                user.geo.set_value(lookup_city());
            }
            user
        }
        slot @ None => {
            let Some(geo) = lookup_city() else {
                return Ok(lookup);
            };
            slot.insert(User {
                geo: Annotated::new(geo),
                ..Default::default()
            })
        }
    };

    let advanced_replaced = if let Some(advanced) = rules.advanced {
        PiiProcessor::new(advanced.compiled()).scrub_event_user_fields(user)?
    } else {
        false
    };
    let (mut saved_ip, last_replaced) = if let Some(legacy) = rules.legacy {
        // Legacy rules may remove the advanced replacement or a competing username/email.
        let saved_ip = advanced_replaced.then(|| user.ip_address.clone());
        let replaced = PiiProcessor::new(legacy.compiled()).scrub_event_user_fields(user)?;
        (saved_ip, replaced)
    } else {
        (None, advanced_replaced)
    };
    let replacement = if user_needs_id(user) {
        saved_ip
            .as_mut()
            .or_else(|| last_replaced.then_some(&mut user.ip_address))
    } else {
        None
    };
    let id = if hash_only {
        let hash = replacement
            .filter(|ip| {
                ip.meta()
                    .iter_remarks()
                    .any(|r| r.ty == RemarkType::Pseudonymized)
            })
            .map(std::mem::take);
        user.ip_address.set_value(None);
        hash
    } else {
        replacement.map(std::mem::take)
    };
    if let Some(ip) = id {
        rules.set_derived_user_id(user, ip)?;
    }
    Ok(lookup)
}

fn enrich_event_asn(event: &mut Event, ip: Option<IpAddr>, db: &AsnLookup, rules: ProjectPii<'_>) {
    let trace_data = field_value_mut(&mut event.contexts).and_then(|contexts| {
        let trace = field_value_mut(contexts.0.get_mut("trace").ok_or(Missing)?)?;
        match &mut trace.0 {
            Context::Trace(trace) => Ok(&mut trace.data),
            _ => Err(Missing),
        }
    });
    let segment_has_asn = has_asn(&event.tags, |tags, k| tags.0.contains(k))
        || match &trace_data {
            Ok(data) => has_asn(data, SpanData::contains),
            Err(reason) => matches!(reason, Redacted),
        };
    if event.ty.value() != Some(&EventType::Transaction) {
        if !segment_has_asn && let Some((number, org)) = ip.and_then(|ip| db.lookup(ip)) {
            let tags = &mut event.tags.get_or_insert_with(Tags::default).0;
            tags.insert(
                CLIENT__AS__NUMBER.into(),
                Annotated::new(number.to_string()),
            );
            if let Some(org) = org {
                tags.insert(
                    CLIENT__AS__ORGANIZATION.into(),
                    Annotated::new(org.to_owned()),
                );
            }
        }
        return;
    }
    let mut fields = [rules.advanced, rules.legacy]
        .into_iter()
        .flatten()
        .any(|config| !config.applications.is_empty())
        .then(Attributes::new);
    if !segment_has_asn
        && !has_asn(&event.measurements, |m, k| m.contains_key(k))
        && let Ok(data) = trace_data
        && let Some(ip) = span_ip(data, |key| event.tags.value()?.0.get(key), ip)
    {
        insert_span_asn(data, ip, db, rules, &mut fields);
    }
    for span in event
        .spans
        .value_mut()
        .iter_mut()
        .flatten()
        .filter_map(|s| s.value_mut().as_mut())
    {
        if !has_asn(&span.data, SpanData::contains)
            && !has_asn(&span.measurements, |m, k| m.contains_key(k))
            && !has_asn(&span.tags, |tags, k| tags.contains_key(k))
            && let Some(ip) = span_ip(&span.data, |key| span.tags.value()?.get(key), None)
        {
            insert_span_asn(&mut span.data, ip, db, rules, &mut fields);
        }
    }
}

/// Preserve a scrubbed container or either ASN attribute supplied by the SDK.
fn has_asn<T>(field: &Annotated<T>, contains: impl Fn(&T, &str) -> bool) -> bool {
    redacted(field)
        || field
            .value()
            .is_some_and(|value| ASN_KEYS.into_iter().any(|key| contains(value, key)))
}

fn set_asn_fields(fields: &mut Attributes, number: u32, organization: Option<&str>) {
    fields.insert(CLIENT__AS__NUMBER, i64::from(number));
    if let Some(org) = organization {
        fields.insert(CLIENT__AS__ORGANIZATION, org);
    }
}

/// Enrich a standalone span using its normalized client address or user IP.
pub fn enrich_span_asn(attrs: &mut Annotated<Attributes>, db: &AsnLookup) {
    if has_asn(attrs, |attrs, k| attrs.contains_key(k)) {
        return;
    }
    let Some(attrs) = attrs.value_mut() else {
        return;
    };
    let ip = [CLIENT__ADDRESS, USER__IP_ADDRESS]
        .into_iter()
        .find_map(|key| attrs.0.get(key))
        .and_then(unredacted_value)
        .and_then(|attr| unredacted_value(&attr.value.value))
        .and_then(Value::as_str)
        .and_then(|text| text.parse().ok());
    if let Some((number, organization)) = ip.and_then(|ip| db.lookup(ip)) {
        set_asn_fields(attrs, number, organization);
    }
}

fn insert_span_asn(
    data: &mut Annotated<SpanData>,
    ip: IpAddr,
    db: &AsnLookup,
    rules: ProjectPii<'_>,
    fields: &mut Option<Attributes>,
) {
    let Some((number, organization)) = db.lookup(ip) else {
        return;
    };
    let Some(fields) = fields else {
        let data = data.get_or_insert_with(Default::default);
        data.insert_value(CLIENT__AS__NUMBER, i64::from(number));
        if let Some(org) = organization {
            data.insert_value(CLIENT__AS__ORGANIZATION, org.to_owned());
        }
        return;
    };
    set_asn_fields(fields, number, organization);
    // Apply span-format destination rules before extracting transaction spans.
    let mut span = Annotated::new(SpanV2 {
        attributes: Annotated::new(std::mem::take(fields)),
        ..Default::default()
    });
    if relay_pii::eap::scrub(ValueType::Span, &mut span, rules.advanced, rules.legacy).is_err() {
        return;
    }
    let Some(attrs) = span.into_value().and_then(|s| s.attributes.into_value()) else {
        return;
    };
    *fields = attrs;
    let data = data.get_or_insert_with(Default::default);
    // Move the owned keys and values into span data, reusing the map for the next span.
    for key in ASN_KEYS {
        if let Some((key, attr)) = fields.0.remove_entry(key)
            && let Some(attr) = attr.into_value()
        {
            data.other.insert(key, attr.value.value);
        }
    }
}

fn span_ip<'a, T: AsRef<str> + 'a>(
    data: &Annotated<SpanData>,
    tag: impl Fn(&str) -> Option<&'a Annotated<T>>,
    fallback: Option<IpAddr>,
) -> Option<IpAddr> {
    for key in [CLIENT__ADDRESS, USER__IP_ADDRESS] {
        let text = if let Some(v) = data.value().and_then(|d| d.get(key)) {
            unredacted_value(v)?.as_str()?
        } else if let Some(v) = tag(key) {
            unredacted_value(v)?.as_ref()
        } else {
            continue;
        };
        return text.parse().ok();
    }
    fallback
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_transport_fallback_with_upstream_redaction() {
        let removed = json!({"": {"rem": [["@anything:remove", "x"]]}});
        let ip = Some("1.0.0.1");
        let asn = Some("15169");
        let db = AsnLookup::open("../tests/fixtures/GeoLite2-ASN-Test.mmdb").unwrap();
        let config = NormalizationConfig {
            client_ip: Some("1.0.0.1".parse().unwrap()),
            infer_ip_address: true,
            asn_lookup: Some(&db),
            ..Default::default()
        };
        for (meta, expected_ip, expected_asn) in [
            (json!({}), ip, asn), // Missing values permit transport fallback.
            (json!({"user": removed}), None, None), // A removed user blocks lookup.
            (json!({"user": {"ip_address": removed}}), None, None), // So does its IP.
            (json!({"request": removed}), None, None), // A removed request blocks fallback.
            (json!({"request": {"env": removed}}), None, None), // So does its environment.
            (json!({"contexts": removed}), ip, None), // The ASN destination is blocked.
            (json!({"contexts": {"trace": removed}}), ip, None), // So is a removed trace.
        ] {
            let payload = json!({
                "platform": "javascript", "user": {"ip_address": null},
                "request": {"env": {}}, "contexts": {"trace": null}, "_meta": meta,
            });
            let mut event = Annotated::<Event>::from_json(&payload.to_string())
                .unwrap()
                .into_value()
                .unwrap();
            process_client_ip(&mut event, &config, Default::default()).unwrap();
            let ip = event.user.value().and_then(|u| u.ip_address.as_str());
            let asn = event.tags.value().and_then(|t| t.get(CLIENT__AS__NUMBER));
            assert_eq!((ip, asn), (expected_ip, expected_asn), "{meta}");
        }
    }

    #[test]
    fn test_ip_fallback_after_both_rule_sets() {
        let advanced = serde_json::from_value(json!({
            "applications": {"$user.ip_address": ["@ip:hash"]}
        }))
        .unwrap();
        let legacy = serde_json::from_value(json!({
            "applications": {
                "$user.email": ["@anything:remove"],
                "$user.ip_address": ["@anything:remove"]
            }
        }))
        .unwrap();
        let rules = ProjectPii {
            advanced: Some(&advanced),
            legacy: Some(&legacy),
        };
        let mut event = Annotated::<Event>::from_json(
            r#"{"user":{"email":"sdk@example.org","ip_address":"127.0.0.1"}}"#,
        )
        .unwrap()
        .into_value()
        .unwrap();
        process_client_ip(&mut event, &NormalizationConfig::default(), rules).unwrap();
        let user = event.user.value().unwrap();
        assert!(user.email.value().is_none());
        assert!(user.ip_address.value().is_none());
        assert_eq!(
            user.id.as_str(),
            Some("AE12FE3B5F129B5CC4CDD2B136B7B7947C4D2741")
        );
    }
}
