//! Resolve the client address and finish its derived identity before span tag extraction.
use std::net::IpAddr;

use relay_conventions::attributes::{
    CLIENT__ADDRESS, CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION, USER__IP_ADDRESS,
};
use relay_event_schema::processor::{ProcessingAction, ProcessingResult, ValueType};
use relay_event_schema::protocol::{
    Attributes, AutoInferSetting, ClientSdkInfo, Event, EventType, IpAddr as ProtocolIp,
    Measurements, ParsedIp, Request, SpanData, SpanV2, Tags, TraceContext, User,
};
use relay_pii::{PiiProcessor, ProjectPii};
use relay_protocol::{Annotated, Remark, RemarkType, Value};

use crate::{AsnLookup, CityLookup, NormalizationConfig};

pub fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().next().is_some()
}

fn remote_addr(request: &Annotated<Request>) -> Option<&Annotated<Value>> {
    request.value()?.env.value()?.get("REMOTE_ADDR")
}

fn remote_redacted(request: &Annotated<Request>) -> bool {
    redacted(request)
        || request.value().is_some_and(|r| redacted(&r.env))
        || remote_addr(request).is_some_and(redacted)
}

/// Resolve the request marker in place. Keep SDK-supplied address spelling for IP hash rules.
fn normalize_remote(
    request: &mut Annotated<Request>,
    client: Option<(&ProtocolIp, IpAddr)>,
) -> Option<IpAddr> {
    let env = request.value_mut().as_mut()?.env.value_mut().as_mut()?;
    let remote = env.get_mut("REMOTE_ADDR")?;
    match remote.as_str()?.parse::<ParsedIp>().ok()? {
        ParsedIp::Address(ip) => Some(ip),
        ParsedIp::Auto => match client {
            Some((text, ip)) => {
                remote.set_value(Some(Value::String(text.to_string())));
                Some(ip)
            }
            None => {
                env.remove("REMOTE_ADDR");
                None
            }
        },
    }
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
    pub client_ip: Option<&'a ProtocolIp>,
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
    let connection = config
        .client_ip
        .and_then(|text| Some((text, text.as_str().parse::<IpAddr>().ok()?)));
    let transport = connection.filter(|_| {
        infer != AutoInferSetting::Never
            && (config.infer_ip_address
                || rules.advanced.is_some_and(|r| !r.applications.is_empty()))
    });
    let request_redacted = remote_redacted(request);
    let remote = if request_redacted {
        None
    } else {
        normalize_remote(request, transport)
    };
    let csp_transport = config.security_report && config.infer_ip_address && transport.is_some();
    let remote = remote.filter(|_| infer != AutoInferSetting::Never && !csp_transport);

    // Keep the original request IP for lookup. Only its surviving text can become user.ip_address.
    rules.scrub_request_env(request)?;
    let field = user.value().map(|u| &u.ip_address);
    let user_ip_redacted = redacted(user) || field.is_some_and(redacted);
    let supplied = field.and_then(|f| f.as_str()).map(str::parse::<ParsedIp>);
    let auto = matches!(supplied, Some(Ok(ParsedIp::Auto)));
    let (lookup, backfill) = match (supplied, remote) {
        _ if user_ip_redacted => (None, None),
        (Some(Ok(ParsedIp::Address(ip))), _) => (Some(ip), None),
        (Some(Err(_)), _) => (None, None),
        (_, Some(ip)) => (
            Some(ip),
            remote_addr(request)
                .filter(|v| !redacted(v))
                .and_then(|v| v.as_str()),
        ),
        _ if csp_transport || !request_redacted => {
            let infer_transport = csp_transport
                || auto
                || infer == AutoInferSetting::Auto
                || matches!(config.platform, Some("javascript" | "cocoa" | "objc"));
            (
                connection.map(|(_, ip)| ip),
                transport
                    .filter(|_| infer_transport)
                    .map(|(text, _)| text.as_str()),
            )
        }
        _ => (None, None),
    };

    let hash_only = backfill.is_some() && remote.is_none() && !config.infer_ip_address;
    if let Some(text) = backfill {
        user.get_or_insert_with(User::default)
            .ip_address
            .set_value(Some(ProtocolIp(text.to_owned())));
    } else if auto
        && !user_ip_redacted
        && let Some(user) = user.value_mut()
    {
        user.ip_address.set_value(None);
    }
    if let (Some(db), Some(ip)) = (config.geoip_lookup, lookup)
        && user
            .value()
            .is_none_or(|u| u.geo.value().is_none() && !redacted(&u.geo))
        && let Some(geo) = db.lookup(ip)
    {
        user.get_or_insert_with(User::default)
            .geo
            .set_value(Some(geo));
    }
    scrub_user_identity(user, rules, hash_only)?;
    Ok(lookup)
}

/// A rule can hash the IP before a later rule removes the IP or another identifier.
fn scrub_user_identity(
    user: &mut Annotated<User>,
    rules: ProjectPii<'_>,
    hash_only: bool,
) -> ProcessingResult {
    if rules.is_empty() || user.value().is_none() {
        return Ok(());
    }
    let configs = [rules.advanced, rules.legacy].into_iter().flatten();
    let mut earlier_replacement = None;
    let mut replaced = false;
    for config in configs.clone() {
        // Only save a copy if another rule set is about to process the replacement.
        if replaced {
            earlier_replacement = user.value().map(|u| u.ip_address.clone());
        }
        let can_replace = user.value().is_some_and(|u| {
            u.id.value().is_none()
                && u.id.meta().is_empty()
                && u.ip_address
                    .as_str()
                    .is_some_and(|s| s.parse::<ParsedIp>().is_ok())
        });
        PiiProcessor::new(config.compiled()).scrub_event_user_fields(user)?;
        replaced = can_replace
            && user
                .value()
                .and_then(|u| u.ip_address.as_str())
                .is_some_and(|s| s.parse::<ParsedIp>().is_err());
    }
    let Some(user) = user.value_mut() else {
        return Ok(());
    };
    let replacement = if replaced {
        Some(&mut user.ip_address)
    } else {
        earlier_replacement.as_mut()
    };
    if let Some(ip) = replacement
        && user.id.value().is_none()
        && user.id.meta().is_empty()
        && user.email.value().is_none()
        && user.username.value().is_none()
        && (!hash_only
            || ip
                .meta()
                .iter_remarks()
                .any(|r| r.ty == RemarkType::Pseudonymized))
    {
        let mut destination = Annotated::new(User {
            id: std::mem::take(ip).map_value(|ip| ip.into_inner().into()),
            ..Default::default()
        });
        for config in configs {
            PiiProcessor::new(config.compiled()).scrub_event_user_fields(&mut destination)?;
        }
        user.id = destination.into_value().map(|u| u.id).unwrap_or_default();
        user.ip_address = Annotated::empty();
        user.ip_address
            .meta_mut()
            .add_remark(Remark::new(RemarkType::Removed, "pii:ip_address"));
    }
    if hash_only {
        user.ip_address.set_value(None);
    }
    Ok(())
}

fn enrich_event_asn(event: &mut Event, ip: Option<IpAddr>, db: &AsnLookup, rules: ProjectPii<'_>) {
    let has_tags = redacted(&event.tags)
        || event
            .tags
            .value()
            .is_some_and(|t| ASN.iter().any(|k| t.0.contains(k)));
    let has_trace = redacted(&event.contexts)
        || event
            .contexts
            .value()
            .and_then(|c| c.0.get("trace"))
            .is_some_and(redacted)
        || event
            .context::<TraceContext>()
            .is_some_and(|t| has_data(&t.data));
    if event.ty.value() != Some(&EventType::Transaction) {
        if !has_tags
            && !has_trace
            && let Some((number, org)) = ip.and_then(|ip| db.lookup(ip))
        {
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
    if !has_tags
        && !has_trace
        && !has_measurement(&event.measurements)
        && let Some(trace) = event.context::<TraceContext>()
        && let Some(ip) = span_ip(&trace.data, |key| event.tags.value()?.0.get(key), ip)
        && let Some(trace) = event.context_mut::<TraceContext>()
    {
        insert_span_asn(&mut trace.data, ip, db, rules, &mut fields);
    }
    for span in event
        .spans
        .value_mut()
        .iter_mut()
        .flatten()
        .filter_map(|s| s.value_mut().as_mut())
    {
        if !has_data(&span.data)
            && !has_measurement(&span.measurements)
            && !redacted(&span.tags)
            && !span
                .tags
                .value()
                .is_some_and(|tags| ASN.iter().any(|k| tags.contains_key(*k)))
            && let Some(ip) = span_ip(&span.data, |key| span.tags.value()?.get(key), None)
        {
            insert_span_asn(&mut span.data, ip, db, rules, &mut fields);
        }
    }
}

const ASN: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

fn has_data(data: &Annotated<SpanData>) -> bool {
    redacted(data)
        || data
            .value()
            .is_some_and(|d| ASN.iter().any(|k| d.contains(k)))
}

fn has_measurement(m: &Annotated<Measurements>) -> bool {
    redacted(m)
        || m.value()
            .is_some_and(|m| ASN.iter().any(|k| m.contains_key(*k)))
}

fn set_asn_fields(fields: &mut Attributes, number: u32, organization: Option<&str>) {
    fields.insert(CLIENT__AS__NUMBER, i64::from(number));
    if let Some(org) = organization {
        fields.insert(CLIENT__AS__ORGANIZATION, org);
    }
}

/// Enrich a standalone span using its normalized client address or user IP.
pub fn enrich_span_asn(attrs: &mut Annotated<Attributes>, db: &AsnLookup) {
    if redacted(attrs) {
        return;
    }
    let Some(attrs) = attrs.value_mut() else {
        return;
    };
    if ASN.iter().any(|k| attrs.contains_key(*k)) {
        return;
    }
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
    for key in ASN {
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
