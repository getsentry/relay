//! Client IP invariants:
//!
//! ## Envelope item coverage
//!
//! ASN lookup requires `projects:relay-client-asn-lookup` and a local ASN database.
//!
//! | Payload | IP source for enrichment | Derived ASN |
//! | --- | --- | --- |
//! | Errors, messages, security reports, feedback events | Event IP | Event tags |
//! | Transaction segment | Span address or event IP | Span attributes |
//! | Child or standalone span | Span address | Span attributes |
//! | Replay event | Event IP for City | None |
//! | Logs, trace metrics, sessions | Resolve IP markers | None |
//! | Other items | No lookup on the item itself | None |
//!
//! Crash attachments can produce an error event, which follows the event rules.
//!
//! ## Request and connection
//!
//! - A concrete `user.ip_address` wins. For backfill, prefer `REMOTE_ADDR`. CSP reports
//!   prefer the connection IP when inference is enabled. Resolve request `{{auto}}` independently.
//! - An explicit user `{{auto}}` permits connection backfill when inference is enabled.
//!   With no user IP, connection backfill requires SDK opt-in or legacy JavaScript, Cocoa, or ObjC.
//! - SDK `infer_ip: "never"` disables user IP backfill. City and opt-in ASN may still
//!   use the connection IP.
//!
//! ## City and ASN
//!
//! - City and opt-in ASN may use an IP even when storage is disabled or scrubbing removes it.
//!   If `REMOTE_ADDR` is the only source and is scrubbed, keep eligible enrichment
//!   without copying the IP to the user or deriving a user ID from it.
//! - Skip City lookup when `user.geo` is supplied or redacted. Skip ASN lookup at a
//!   destination when either ASN field is supplied or redacted. Honor destination PII rules.
//!
//! City lookup with IP storage disabled: <https://github.com/getsentry/relay/pull/4586>.
//!
//! ## User identity
//!
//! - A PII pass can supply a fallback ID when `user.id` starts empty with no metadata
//!   and the pass turns the IP into non-IP text, such as a hash or `[ip]`.
//! - Prefer the advanced replacement, even if legacy rules later remove the IP.
//!   After both passes, any ID, ID metadata, email, or username blocks the fallback.
//! - Apply `user.id` rules to the fallback and mark `user.ip_address` as removed.
//!   Always clear a connection IP admitted only for hashing. Only its hash may supply the ID.
//! - Later Relays preserve existing enrichment and honor upstream redactions.
//!
//! ## Span enrichment
//!
//! - For ASN, use the span's `client.address`, then `user.ip_address` if the first is absent.
//!   An invalid or redacted address blocks fallback.
//! - An explicit span address marker (`{{auto}}`) requests the connection IP, including
//!   on transaction segments and children. Store the resolved address when IP inference is
//!   enabled. A missing child address stays missing.
//! - A transaction segment may use the event IP when both span addresses are absent.
//!   Child spans need their own address for ASN, even when they inherit event user tags.
//! - Standalone spans use `client.address` for City lookup. Only ASN can fall back to
//!   `user.ip_address`. If scrubbing replaces `client.address` with a valid IP, City can use
//!   the replacement and ASN skips lookup. Span PII rules apply to the derived ASN attributes.

use std::net::IpAddr;

use relay_conventions::attributes::{
    CLIENT__ADDRESS, CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION, USER__IP_ADDRESS,
};
use relay_event_schema::processor::{ProcessingAction, ProcessingResult, ValueType};
use relay_event_schema::protocol::{
    Attributes, AutoInferSetting, ClientSdkInfo, Context, Event, EventType, IpAddr as ProtocolIp,
    ParsedIp, Request, SpanData, SpanV2, Tags, User,
};
use relay_pii::ProjectPii;
use relay_pii::UserIpRetention::{ApplyRules, HashOnly};
use relay_protocol::{Annotated, Object, RemarkType, Value};

use self::Unavailable::{Missing, Redacted};
use crate::{AsnLookup, CityLookup, NormalizationConfig};

const ADDRESS_KEYS: [&str; 2] = [CLIENT__ADDRESS, USER__IP_ADDRESS];
const ASN_KEYS: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

pub fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().any(|remark| match remark.ty {
        RemarkType::Annotated => false,
        RemarkType::Removed
        | RemarkType::Substituted
        | RemarkType::Masked
        | RemarkType::Pseudonymized
        | RemarkType::Encrypted => true,
    })
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
            // A previous normalizer already decided whether to backfill the event user.
            client_ip: config.client_ip.filter(|_| !config.is_renormalize),
            geoip_lookup: config.geoip_lookup,
            infer_ip_address: config.infer_ip_address,
            security_report: event.csp.value().is_some(),
        },
        rules,
    )?;
    process_span_addresses_and_asn(event, lookup, config, rules);
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

/// Update the request payload's marker and read a concrete address only when needed for fallback.
fn normalize_request_ip(
    request: &mut Annotated<Request>,
    transport: Option<IpAddr>,
    use_request: bool,
) -> Result<ProtocolIp, Unavailable> {
    let env = field_value_mut(&mut field_value_mut(request)?.env)?;
    let field = field_value_mut(env.get_mut("REMOTE_ADDR").ok_or(Missing)?)?;
    match field.as_str() {
        // Resolve the marker in the request payload even if another source supplies the user IP.
        Some(ProtocolIp::AUTO_MARKER) => {
            let Some(ip) = transport else {
                env.remove("REMOTE_ADDR");
                return Err(Missing);
            };
            *field = Value::String(ip.to_string());
            use_request.then(|| ProtocolIp::from(ip)).ok_or(Missing)
        }
        // Parse a literal only when the request can supply the user IP.
        Some(text) if use_request => ProtocolIp::parse(text).map_err(|_| Missing),
        // Leave the payload as received when there is no request candidate to read.
        _ => Err(Missing),
    }
}

fn infer_ip_setting(sdk: Option<&ClientSdkInfo>) -> AutoInferSetting {
    sdk.and_then(|sdk| sdk.settings.value())
        .map(|settings| settings.infer_ip())
        .unwrap_or_default()
}

/// Normalize the request and user in place and return the address eligible for lookup.
pub fn process_user_ip(
    request: &mut Annotated<Request>,
    user: &mut Annotated<User>,
    config: ClientIpConfig<'_>,
    rules: ProjectPii<'_>,
) -> Result<Option<IpAddr>, ProcessingAction> {
    let infer = infer_ip_setting(config.sdk);
    let transport = config.client_ip.filter(|_| {
        infer != AutoInferSetting::Never
            && (config.infer_ip_address
                || rules.advanced.is_some_and(|r| !r.applications.is_empty()))
    });
    let supplied = field_value_mut(user).and_then(|user| {
        let ip = field_value_mut(&mut user.ip_address)?.parsed();
        if ip == ParsedIp::Auto {
            user.ip_address.set_value(None);
        }
        Ok(ip)
    });
    let csp_connection = transport.filter(|_| config.security_report && config.infer_ip_address);
    let use_request = matches!(supplied, Ok(ParsedIp::Auto) | Err(Missing))
        && csp_connection.is_none()
        && infer != AutoInferSetting::Never;
    let remote = normalize_request_ip(request, transport, use_request);
    rules.scrub_request_env(request)?;

    let connection_retention = if config.infer_ip_address {
        ApplyRules
    } else {
        HashOnly
    };
    let (lookup, backfill, ip_retention) = match (supplied, remote) {
        // Use the concrete IP already on the user for lookup and leave the field for user PII.
        (Ok(ParsedIp::Address(ip)), _) => (Some(ip), None, ApplyRules),
        // A replaced or redacted user IP blocks lookup and fallback to another source.
        (Ok(ParsedIp::Replacement) | Err(Redacted), _) => (None, None, ApplyRules),

        // Security reports prefer the connection, even if the request IP was redacted.
        (_, _) if let Some(ip) = csp_connection => (Some(ip), Some(ip.into()), ApplyRules),
        // Redaction on the incoming request, environment, or address blocks connection fallback.
        (_, Err(Redacted)) => (None, None, ApplyRules),
        // Look up the original request IP. Copy it to the user only if it survives request PII.
        (_, Ok(ip)) => {
            let survives = request
                .value()
                .and_then(|r| r.env.value()?.get("REMOTE_ADDR"))
                .and_then(unredacted_value)
                .and_then(Value::as_str)
                .is_some();
            (ip.address(), survives.then_some(ip), ApplyRules)
        }

        // An explicit user marker requests a connection backfill on any platform.
        (Ok(ParsedIp::Auto), Err(Missing)) if let Some(ip) = transport => {
            (Some(ip), Some(ip.into()), connection_retention)
        }
        // A missing user IP uses the connection for SDK opt-in or legacy client platforms.
        (Err(Missing), Err(Missing))
            if let Some(ip) = transport
                && infer.allows_missing_user_ip_from_connection(config.platform) =>
        {
            (Some(ip), Some(ip.into()), connection_retention)
        }
        // The envelope IP can still supply a lookup when it cannot backfill the user.
        (Ok(ParsedIp::Auto) | Err(Missing), Err(Missing)) => (config.client_ip, None, ApplyRules),
    };
    let lookup_city = || config.geoip_lookup?.lookup(lookup?);
    let user = match user.value_mut() {
        // Fill eligible fields on the existing user, preserving supplied or redacted geo.
        Some(user) => {
            if let Some(ip) = backfill {
                user.ip_address.set_value(Some(ip));
            }
            if user.geo.value().is_none() && !redacted(&user.geo) {
                user.geo.set_value(lookup_city());
            }
            user
        }
        // Create a user only when an IP or City lookup result needs to be stored.
        slot @ None => {
            let geo = lookup_city();
            if backfill.is_none() && geo.is_none() {
                return Ok(lookup);
            }
            slot.insert(User {
                ip_address: backfill.into(),
                geo: geo.into(),
                ..Default::default()
            })
        }
    };

    rules.scrub_event_user(user, ip_retention)?;
    Ok(lookup)
}

fn process_span_addresses_and_asn(
    event: &mut Event,
    ip: Option<IpAddr>,
    config: &NormalizationConfig<'_>,
    rules: ProjectPii<'_>,
) {
    let is_transaction = event.ty.value() == Some(&EventType::Transaction);
    if !is_transaction && config.asn_lookup.is_none() {
        return;
    }
    let trace_data = field_value_mut(&mut event.contexts).and_then(|contexts| {
        let trace = field_value_mut(contexts.0.get_mut("trace").ok_or(Missing)?)?;
        match &mut trace.0 {
            Context::Trace(trace) => Ok(&mut trace.data),
            _ => Err(Missing),
        }
    });
    let segment_db = config.asn_lookup.filter(|_| {
        !has_asn(&event.tags, |tags, k| tags.0.contains(k))
            && match &trace_data {
                Ok(data) => !has_asn(data, SpanData::contains),
                Err(reason) => matches!(reason, Missing),
            }
    });
    if !is_transaction {
        if let Some((number, org)) = segment_db.and_then(|db| db.lookup(ip?)) {
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
    let marker_ip = config
        .client_ip
        .filter(|_| infer_ip_setting(event.client_sdk.value()) != AutoInferSetting::Never);
    let stored_ip = marker_ip.filter(|_| config.infer_ip_address);
    let mut fields = [rules.advanced, rules.legacy]
        .into_iter()
        .flatten()
        .any(|config| !config.applications.is_empty())
        .then(Attributes::new);
    if let Ok(data) = trace_data {
        if let Some(db) = segment_db
            && !has_asn(&event.measurements, |m, k| m.contains_key(k))
            && let Some(ip) = span_ip(data, |key| event.tags.value()?.0.get(key), marker_ip, ip)
        {
            insert_span_asn(data, ip, db, rules, &mut fields);
        }
        normalize_data_markers(data, stored_ip);
    }
    if let Ok(tags) = field_value_mut(&mut event.tags) {
        tags.retain_mut(|entry| {
            let Ok(entry) = field_value_mut(entry) else {
                return true;
            };
            !matches!(entry.0.as_str(), Some(CLIENT__ADDRESS | USER__IP_ADDRESS))
                || keep_address(&mut entry.1, stored_ip, |v| Some(v.as_str()))
        });
    }
    let Ok(spans) = field_value_mut(&mut event.spans) else {
        return;
    };
    for span in spans.iter_mut().filter_map(|s| field_value_mut(s).ok()) {
        if let Some(db) = config.asn_lookup
            && !has_asn(&span.data, SpanData::contains)
            && !has_asn(&span.measurements, |m, k| m.contains_key(k))
            && !has_asn(&span.tags, |tags, k| tags.contains_key(k))
            && let Some(ip) = span_ip(
                &span.data,
                |key| span.tags.value()?.get(key),
                marker_ip,
                None,
            )
        {
            insert_span_asn(&mut span.data, ip, db, rules, &mut fields);
        }
        normalize_data_markers(&mut span.data, stored_ip);
        if let Ok(tags) = field_value_mut(&mut span.tags) {
            normalize_map_markers(tags, stored_ip, |v| Some(v.as_str()));
        }
    }
}

/// Preserve a scrubbed container or either ASN attribute supplied by the SDK.
pub fn has_asn<T>(field: &Annotated<T>, contains: impl Fn(&T, &str) -> bool) -> bool {
    redacted(field)
        || field
            .value()
            .is_some_and(|value| ASN_KEYS.into_iter().any(|key| contains(value, key)))
}

pub fn set_asn_fields(fields: &mut Attributes, number: u32, organization: Option<&str>) {
    fields.insert(CLIENT__AS__NUMBER, i64::from(number));
    if let Some(org) = organization {
        fields.insert(CLIENT__AS__ORGANIZATION, org);
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
    marker_ip: Option<IpAddr>,
    fallback: Option<IpAddr>,
) -> Option<IpAddr> {
    for key in ADDRESS_KEYS {
        let text = if let Some(v) = data.value().and_then(|d| d.get(key)) {
            unredacted_value(v)?.as_str()?
        } else if let Some(v) = tag(key) {
            unredacted_value(v)?.as_ref()
        } else {
            continue;
        };
        return match text {
            ProtocolIp::AUTO_MARKER => marker_ip,
            literal => literal.parse().ok(),
        };
    }
    fallback
}

/// Resolve markers after the lookup so removing them for storage preserves eligible ASN.
fn normalize_data_markers(data: &mut Annotated<SpanData>, ip: Option<IpAddr>) {
    if let Ok(data) = field_value_mut(data) {
        normalize_map_markers(&mut data.other, ip, Value::as_str);
    }
}

fn normalize_map_markers<T: From<String>>(
    fields: &mut Object<T>,
    ip: Option<IpAddr>,
    text: fn(&T) -> Option<&str>,
) {
    for key in ADDRESS_KEYS {
        if let Some(field) = fields.get_mut(key)
            && !keep_address(field, ip, text)
        {
            fields.remove(key);
        }
    }
}

fn keep_address<T: From<String>>(
    field: &mut Annotated<T>,
    ip: Option<IpAddr>,
    text: fn(&T) -> Option<&str>,
) -> bool {
    if unredacted_value(field).and_then(text) != Some(ProtocolIp::AUTO_MARKER) {
        return true;
    }
    let Some(ip) = ip else {
        return false;
    };
    field.set_value(Some(ip.to_string().into()));
    true
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
    fn test_previously_normalized_span_ip_markers() {
        let db = AsnLookup::open("../tests/fixtures/GeoLite2-ASN-Test.mmdb").unwrap();
        let ip = "1.0.0.1";
        let marker = ProtocolIp::AUTO_MARKER;
        for (connection, store, sdk, lookup, expected_ip, expected_asn) in [
            (Some(ip), true, "auto", None, Some(ip), None),
            (Some(ip), false, "auto", Some(&db), None, Some(15169)),
            (Some(ip), true, "never", Some(&db), None, None),
            (None, true, "auto", Some(&db), None, None),
        ] {
            let payload = json!({
                "type": "transaction", "platform": "javascript",
                "sdk": {"settings": {"infer_ip": sdk}},
                "tags": {"client.address": marker},
                "spans": [{"data": {"user.ip_address": marker}}],
            });
            let mut event = Annotated::<Event>::from_json(&payload.to_string())
                .unwrap()
                .into_value()
                .unwrap();
            let config = NormalizationConfig {
                is_renormalize: true,
                client_ip: connection.map(|ip| ip.parse().unwrap()),
                infer_ip_address: store,
                asn_lookup: lookup,
                ..Default::default()
            };
            process_client_ip(&mut event, &config, Default::default()).unwrap();
            assert!(event.user.value().is_none());
            assert_eq!(
                event.tags.value().unwrap().get(CLIENT__ADDRESS),
                expected_ip
            );
            let span = event.spans.value().unwrap()[0].value().unwrap();
            let data = span.data.value().unwrap();
            assert_eq!(data.get_str(USER__IP_ADDRESS), expected_ip);
            assert_eq!(
                data.get_value(CLIENT__AS__NUMBER),
                expected_asn.map(Value::I64).as_ref()
            );
        }
    }

    #[test]
    fn test_user_ip_lookup_with_remarks() {
        let db = AsnLookup::open("../tests/fixtures/GeoLite2-ASN-Test.mmdb").unwrap();
        let config = NormalizationConfig {
            asn_lookup: Some(&db),
            ..Default::default()
        };
        for (remarks, expected) in [
            (json!([["note", "a"]]), Some("15169")),
            (json!([["note", "a"], ["replace", "s"]]), None),
            (json!([["remove", "x"]]), None),
            (json!([["mask", "m"]]), None),
            (json!([["hash", "p"]]), None),
            (json!([["encrypt", "e"]]), None),
        ] {
            let payload = json!({
                "user": {"ip_address": "1.0.0.1"},
                "_meta": {"user": {"ip_address": {"": {"rem": remarks}}}},
            });
            let mut event = Annotated::<Event>::from_json(&payload.to_string())
                .unwrap()
                .into_value()
                .unwrap();
            process_client_ip(&mut event, &config, Default::default()).unwrap();
            assert_eq!(
                event.tags.value().and_then(|t| t.get(CLIENT__AS__NUMBER)),
                expected
            );
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
