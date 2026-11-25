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
use relay_pii::{ProjectPii, user_needs_id};
use relay_protocol::{Annotated, RemarkType, Value};

use crate::{AsnLookup, CityLookup, NormalizationConfig};

const ASN_KEYS: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

pub fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().next().is_some()
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
    let existing = user.value();
    let supplied = existing
        .and_then(|u| u.ip_address.value())
        .map(ProtocolIp::parsed);
    let user_ip_redacted = redacted(user) || existing.is_some_and(|u| redacted(&u.ip_address));
    let auto = supplied == Some(ParsedIp::Auto);
    let csp_transport = config.security_report && config.infer_ip_address && transport.is_some();
    let use_remote = !user_ip_redacted
        && (supplied.is_none() || auto)
        && infer != AutoInferSetting::Never
        && !csp_transport;
    let mut request_redacted = redacted(request);
    let remote = request.0.as_mut().and_then(|request| {
        request_redacted |= redacted(&request.env);
        let env = request.env.0.as_mut()?;
        let field = env.get_mut("REMOTE_ADDR")?;
        request_redacted |= redacted(field);
        if request_redacted {
            return None;
        }
        let text = field.as_str()?;
        if text == "{{auto}}" {
            let Some(ip) = transport else {
                env.remove("REMOTE_ADDR");
                return None;
            };
            field.set_value(Some(Value::String(ip.to_string())));
            return use_remote.then(|| ProtocolIp::from(ip));
        }
        if !use_remote {
            return None;
        }
        ProtocolIp::parse(text).ok()
    });

    // Keep the original request IP for lookup. Only a surviving source can become user.ip_address.
    rules.scrub_request_env(request)?;
    let from_remote = remote.is_some();
    let (lookup, backfill) = match supplied {
        _ if user_ip_redacted => (None, None),
        Some(ParsedIp::Address(ip)) => (Some(ip), None),
        Some(ParsedIp::Replacement) => (None, None),
        _ if let Some(remote) = remote => {
            let ip = remote.address();
            let surviving = request
                .value()
                .and_then(|r| r.env.value()?.get("REMOTE_ADDR"))
                .is_some_and(|v| v.as_str().is_some() && !redacted(v));
            (ip, surviving.then_some(remote))
        }
        _ if csp_transport || !request_redacted => {
            let infer_transport = csp_transport
                || auto
                || infer == AutoInferSetting::Auto
                || matches!(config.platform, Some("javascript" | "cocoa" | "objc"));
            (
                config.client_ip,
                transport.filter(|_| infer_transport).map(ProtocolIp::from),
            )
        }
        _ => (None, None),
    };
    let geo = match existing {
        Some(u) if u.geo.value().is_some() || redacted(&u.geo) => None,
        _ => config.geoip_lookup.and_then(|db| db.lookup(lookup?)),
    };
    let user = match user.value_mut() {
        Some(user) => user,
        missing if backfill.is_some() || geo.is_some() => missing.insert(User::default()),
        None => return Ok(lookup),
    };
    if let Some(geo) = geo {
        user.geo.set_value(Some(geo));
    }

    let hash_only = backfill.is_some() && !from_remote && !config.infer_ip_address;
    if let Some(ip) = backfill {
        user.ip_address.set_value(Some(ip));
    } else if auto && !user_ip_redacted {
        user.ip_address.set_value(None);
    }
    let (early, mut last) = match rules.processors() {
        [early, Some(last)] => (early, last),
        [Some(last), None] => (None, last),
        [None, None] => return Ok(lookup),
    };
    let id_unset = |u: &User| u.id.value().is_none() && u.id.meta().is_empty();
    let eligible = id_unset(user)
        && user
            .ip_address
            .value()
            .is_some_and(|ip| ip.parsed() != ParsedIp::Replacement);
    let allowed = |ip: &Annotated<ProtocolIp>| {
        !hash_only
            || ip
                .meta()
                .iter_remarks()
                .any(|r| r.ty == RemarkType::Pseudonymized)
    };

    // Only projects with both rule sets need to preserve an intermediate replacement.
    let (eligible, saved) = match early {
        None => (eligible, None),
        Some(mut first) => {
            first.scrub_event_user_fields(user)?;
            match user.ip_address.value().map(ProtocolIp::parsed) {
                Some(ParsedIp::Address(_) | ParsedIp::Auto) => (id_unset(user), None),
                Some(ParsedIp::Replacement) if eligible && allowed(&user.ip_address) => {
                    (false, Some(user.ip_address.clone()))
                }
                _ => (false, None),
            }
        }
    };

    last.scrub_event_user_fields(user)?;
    if user_needs_id(user) {
        let replacement = saved.or_else(|| {
            (eligible
                && user
                    .ip_address
                    .value()
                    .is_some_and(|ip| ip.parsed() == ParsedIp::Replacement)
                && allowed(&user.ip_address))
            .then(|| std::mem::take(&mut user.ip_address))
        });
        if let Some(ip) = replacement {
            rules.set_derived_user_id(user, ip)?;
        }
    }
    if hash_only {
        user.ip_address.set_value(None);
    }
    Ok(lookup)
}

fn enrich_event_asn(event: &mut Event, ip: Option<IpAddr>, db: &AsnLookup, rules: ProjectPii<'_>) {
    let contexts_redacted = redacted(&event.contexts);
    let trace = event.contexts.0.as_mut().and_then(|c| c.0.get_mut("trace"));
    let trace_redacted = trace.as_deref().is_some_and(redacted);
    let trace_data = trace.and_then(|entry| match &mut entry.0.as_mut()?.0 {
        Context::Trace(trace) => Some(&mut trace.data),
        _ => None,
    });
    let segment_has_asn = contexts_redacted
        || trace_redacted
        || has_asn(&event.tags, |tags, k| tags.0.contains(k))
        || trace_data
            .as_deref()
            .is_some_and(|d| has_asn(d, SpanData::contains));
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
                    Annotated::new(org.chars().take(200).collect()),
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
        && let Some(data) = trace_data
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
    let source = [CLIENT__ADDRESS, USER__IP_ADDRESS]
        .into_iter()
        .find_map(|key| attrs.0.get(key));
    let Some(entry) = source.filter(|a| !redacted(a)).and_then(|a| a.value()) else {
        return;
    };
    let value = &entry.value.value;
    if !redacted(value)
        && let Some(ip) = value.as_str().and_then(|s| s.parse().ok())
        && let Some((number, organization)) = db.lookup(ip)
    {
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
            if redacted(v) {
                return None;
            }
            v.as_str()
        } else if let Some(v) = tag(key) {
            if redacted(v) {
                return None;
            }
            v.as_str()
        } else {
            continue;
        };
        return text?.parse().ok();
    }
    fallback
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
