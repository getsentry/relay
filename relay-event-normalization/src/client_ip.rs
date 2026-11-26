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
//! - Keep a concrete `user.ip_address`. When backfilling, CSP prefers the connection IP
//!   and other events prefer `REMOTE_ADDR`. Resolve request `{{auto}}` independently.
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
//! - Skip City lookup when `user.geo` is supplied or marked as modified. Skip ASN lookup
//!   when either destination field is supplied or marked as modified. Honor destination PII rules.
//!
//! City lookup with IP storage disabled: <https://github.com/getsentry/relay/pull/4586>.
//!
//! ## User identity
//!
//! - A PII pass can supply a fallback ID by hashing a concrete IP when `user.id` starts
//!   empty with no metadata. A mask or fixed replacement cannot identify a user.
//! - Prefer the advanced hash, even if legacy rules later remove the IP.
//!   After both passes, any ID, ID metadata, email, or username blocks the fallback.
//! - Apply `user.id` rules to the fallback and mark `user.ip_address` as removed.
//!   Always clear a connection IP admitted only for hashing. Only its hash may supply the ID.
//! - A downstream processor can fill missing City or ASN from the forwarded client IP.
//!   An earlier normalization decision still governs IP storage. A modifying remark on the
//!   source blocks fallback.
//!
//! ## Span enrichment
//!
//! - For ASN, use the span's `client.address`, then `user.ip_address` if the first is absent.
//!   An invalid address or one marked as modified blocks fallback.
//! - An explicit span address marker (`{{auto}}`) requests the connection IP, including
//!   on transaction segments and children. Store the resolved address when IP inference is
//!   enabled, subject to the span attribute’s PII rules. A missing child address stays missing.
//! - A transaction segment may use the event IP when both span addresses are absent.
//!   Child spans need their own address for ASN, even when they inherit event user tags.
//! - Standalone spans use the same eligible address for City and ASN. Span PII rules
//!   apply to the derived attributes.

use std::cell::LazyCell;
use std::mem;
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

use self::Unavailable::{MarkedModified, Missing};
use crate::{AsnLookup, CityLookup, NormalizationConfig};

const ADDRESS_KEYS: [&str; 2] = [CLIENT__ADDRESS, USER__IP_ADDRESS];
const ASN_KEYS: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

/// Whether a remark records a change to the field, including a change to only part of its value.
pub fn has_modifying_remark<T>(field: &Annotated<T>) -> bool {
    field.meta().iter_remarks().any(|remark| match remark.ty {
        RemarkType::Annotated => false,
        RemarkType::Removed
        | RemarkType::Substituted
        | RemarkType::Masked
        | RemarkType::Pseudonymized
        | RemarkType::Encrypted => true,
    })
}

/// Read a present value when its remarks permit using it as a source.
pub fn value_without_modifying_remark<T>(field: &Annotated<T>) -> Option<&T> {
    field.value().filter(|_| !has_modifying_remark(field))
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
            backfill_from_connection: !config.is_renormalize,
            geoip_lookup: config.geoip_lookup,
            infer_ip_address: config.infer_ip_address,
            security_report: event.csp.value().is_some(),
        },
        rules,
    )?;
    let tag_db = config
        .asn_lookup
        .filter(|_| !has_asn(&event.tags, |tags, k| tags.0.contains(k)));
    if event.ty.value() == Some(&EventType::Transaction) {
        process_transaction_ips(event, lookup, tag_db, config, rules);
    } else if let Some((number, org)) = tag_db.and_then(|db| db.lookup(lookup?)) {
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
    Ok(())
}

/// Inputs used to resolve a user IP on events and replays.
pub struct ClientIpConfig<'a> {
    pub platform: Option<&'a str>,
    pub sdk: Option<&'a ClientSdkInfo>,
    pub client_ip: Option<IpAddr>,
    /// A fully normalized event may use the original connection for lookup only.
    pub backfill_from_connection: bool,
    pub geoip_lookup: Option<&'a CityLookup>,
    pub infer_ip_address: bool,
    pub security_report: bool,
}

/// A missing field permits fallback. A modifying remark on the field or a parent stops it.
enum Unavailable {
    Missing,
    MarkedModified,
}

fn field_value_mut<T>(field: &mut Annotated<T>) -> Result<&mut T, Unavailable> {
    if has_modifying_remark(field) {
        return Err(MarkedModified);
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
        config.backfill_from_connection
            && infer != AutoInferSetting::Never
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
        // A supplied IP also wins on a CSP event forwarded by another Relay.
        (Ok(ParsedIp::Address(ip)), _) => (Some(ip), None, ApplyRules),
        // A replacement value or a modifying remark blocks lookup and fallback.
        (Ok(ParsedIp::Replacement) | Err(MarkedModified), _) => (None, None, ApplyRules),

        // A browser report without a user IP uses the connection for backfill.
        (_, _) if let Some(ip) = csp_connection => (Some(ip), Some(ip.into()), ApplyRules),
        // A modifying remark on the request, environment, or address blocks connection fallback.
        (_, Err(MarkedModified)) => (None, None, ApplyRules),
        // Look up the original request IP. Copy it to the user only if it survives request PII.
        (_, Ok(ip)) => {
            let survives = request
                .value()
                .and_then(|r| r.env.value()?.get("REMOTE_ADDR"))
                .and_then(value_without_modifying_remark)
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
            if user.geo.value().is_none() && !has_modifying_remark(&user.geo) {
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

fn process_transaction_ips(
    event: &mut Event,
    ip: Option<IpAddr>,
    segment_db: Option<&AsnLookup>,
    config: &NormalizationConfig<'_>,
    rules: ProjectPii<'_>,
) {
    let marker_ip = config
        .client_ip
        .filter(|_| infer_ip_setting(event.client_sdk.value()) != AutoInferSetting::Never);
    // All markers in this transaction use the same connection, but each destination
    // can have its own PII rule. Compute their publishable values on first use.
    let stored_addresses = LazyCell::new(|| {
        let Some(ip) = marker_ip.filter(|_| config.infer_ip_address) else {
            return Object::new();
        };
        let mut fields = Attributes::new();
        for key in ADDRESS_KEYS {
            fields.insert(key, ip.to_string());
        }
        scrubbed_span_fields(fields, rules).collect()
    });
    let stored = |key: &str| {
        stored_addresses
            .get(key)
            .cloned()
            .unwrap_or_default()
            .and_then(Value::into_string)
    };
    let trace_data = field_value_mut(&mut event.contexts).and_then(|contexts| {
        let trace = field_value_mut(contexts.0.get_mut("trace").ok_or(Missing)?)?;
        match &mut trace.0 {
            Context::Trace(trace) => Ok(&mut trace.data),
            _ => Err(Missing),
        }
    });
    // Resolve address markers even when ASN lookup is disabled or already supplied.
    let mut segment_tags = [None; 2];
    if let Ok(tags) = field_value_mut(&mut event.tags) {
        tags.retain_mut(|entry| {
            let Ok(entry) = field_value_mut(entry) else {
                return true;
            };
            let Some(i) = ADDRESS_KEYS
                .iter()
                .position(|key| Some(*key) == entry.0.as_str())
            else {
                return true;
            };
            let (address, keep) = resolve_address(
                &mut entry.1,
                || stored(ADDRESS_KEYS[i]),
                |s| Some(s.as_str()),
            );
            segment_tags[i].get_or_insert(address);
            keep
        });
    }
    let process_span = |data: &mut Annotated<SpanData>, tags, fallback, db: Option<&AsnLookup>| {
        let addresses = resolve_address_map(
            field_value_mut(data).ok().map(|d| &mut d.other),
            stored,
            Value::as_str,
        );
        let Some(db) = db.filter(|_| !has_asn(data, SpanData::contains)) else {
            return;
        };
        // For each key, data precedes tags. Only a wholly absent address permits fallback.
        let source = addresses
            .into_iter()
            .zip(tags)
            .find_map(|(data, tag)| data.or(tag));
        let ip = match source {
            Some(ParsedIp::Address(ip)) => Some(ip),
            Some(ParsedIp::Auto) => marker_ip,
            Some(ParsedIp::Replacement) => None,
            None => fallback,
        };
        if let Some((number, org)) = ip.and_then(|ip| db.lookup(ip)) {
            let mut fields = Attributes::new();
            set_asn_fields(&mut fields, number, org);
            data.get_or_insert_with(Default::default)
                .other
                .extend(scrubbed_span_fields(fields, rules));
        }
    };
    if let Ok(data) = trace_data {
        let db = segment_db.filter(|_| !has_asn(&event.measurements, |m, k| m.contains_key(k)));
        process_span(data, segment_tags, ip, db);
    }
    let Ok(spans) = field_value_mut(&mut event.spans) else {
        return;
    };
    for span in spans.iter_mut().filter_map(|s| field_value_mut(s).ok()) {
        let db = config.asn_lookup.filter(|_| {
            !has_asn(&span.measurements, |m, k| m.contains_key(k))
                && !has_asn(&span.tags, |tags, k| tags.contains_key(k))
        });
        let tags = resolve_address_map(field_value_mut(&mut span.tags).ok(), stored, |s| {
            Some(s.as_str())
        });
        process_span(&mut span.data, tags, None, db);
    }
}

/// Preserve a scrubbed container or either ASN attribute supplied by the SDK.
pub fn has_asn<T>(field: &Annotated<T>, contains: impl Fn(&T, &str) -> bool) -> bool {
    has_modifying_remark(field)
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

/// Apply span-attribute rules before generated fields are written to the transaction.
fn scrubbed_span_fields(
    fields: Attributes,
    rules: ProjectPii<'_>,
) -> impl Iterator<Item = (String, Annotated<Value>)> {
    let attrs = if rules.is_empty() {
        fields
    } else {
        let mut span = Annotated::new(SpanV2 {
            attributes: fields.into(),
            ..Default::default()
        });
        relay_pii::eap::scrub(ValueType::Span, &mut span, rules.advanced, rules.legacy)
            .ok()
            .and_then(|_| span.into_value()?.attributes.into_value())
            .unwrap_or_default()
    };
    attrs
        .0
        .into_iter()
        .map(|(key, attr)| (key, attr.and_then(|a| a.value.value)))
}

/// Return each original address while applying marker storage to the wire fields.
fn resolve_address_map<T: From<String>>(
    mut fields: Option<&mut Object<T>>,
    stored: impl Fn(&str) -> Annotated<String>,
    text: fn(&T) -> Option<&str>,
) -> [Option<ParsedIp>; 2] {
    ADDRESS_KEYS.map(|key| {
        let fields = fields.as_deref_mut()?;
        let (address, keep) = resolve_address(fields.get_mut(key)?, || stored(key), text);
        if !keep {
            fields.remove(key);
        }
        Some(address)
    })
}

fn resolve_address<T: From<String>>(
    field: &mut Annotated<T>,
    stored: impl FnOnce() -> Annotated<String>,
    text: fn(&T) -> Option<&str>,
) -> (ParsedIp, bool) {
    let address = value_without_modifying_remark(field)
        .and_then(text)
        .and_then(|v| v.parse().ok())
        .unwrap_or(ParsedIp::Replacement);
    if address == ParsedIp::Auto {
        let replacement = stored();
        let keep = replacement.value().is_some() || !replacement.meta().is_empty();
        *field = mem::take(field).and_then(|_| replacement.map_value(T::from));
        (address, keep)
    } else {
        (address, true)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_event_ip_sources_without_connection() {
        // An HTTP integration test always has at least the loopback peer.
        let config = NormalizationConfig {
            infer_ip_address: true,
            ..Default::default()
        };
        for csp in [None, Some(json!({"effective_directive": "default-src"}))] {
            for user in [None, Some("192.0.2.1")] {
                for remote in [None, Some("192.0.2.2")] {
                    let payload = json!({
                        "platform": "javascript", "csp": csp,
                        "user": {"ip_address": user}, "request": {"env": {"REMOTE_ADDR": remote}},
                    });
                    let mut event = Annotated::<Event>::from_json(&payload.to_string())
                        .unwrap()
                        .into_value()
                        .unwrap();
                    process_client_ip(&mut event, &config, Default::default()).unwrap();
                    assert_eq!(
                        event.user.value().and_then(|u| u.ip_address.as_str()),
                        user.or(remote),
                        "{payload}"
                    );
                }
            }
        }
    }

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
            (json!({"tags": removed}), ip, None), // Removed tags block error ASN enrichment.
            (json!({"contexts": removed}), ip, asn), // Errors write ASN to tags.
            (json!({"contexts": {"trace": removed}}), ip, asn),
        ] {
            let payload = json!({
                "platform": "javascript", "user": {"ip_address": null}, "tags": null,
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
