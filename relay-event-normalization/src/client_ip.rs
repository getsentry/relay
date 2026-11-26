//! Client IP invariants:
//!
//! ## Envelope item coverage
//!
//! ASN lookup requires `projects:relay-client-asn-lookup` and a local ASN database.
//!
//! | Payload | IP source for enrichment | Derived ASN |
//! | --- | --- | --- |
//! | Raw CSP report | Connection at ingestion | Event tags after conversion |
//! | Errors, messages, classified CSP, feedback v2 | Event IP | Event tags |
//! | Transaction segment | Span address or event IP | Span attributes |
//! | Child or standalone span | Span address | Span attributes |
//! | Replay event | Event IP for City | None |
//! | Logs, trace metrics | Address attributes | None |
//! | Sessions, session aggregates | `attrs.ip_address` marker | None |
//! | Other items | No lookup on the item itself | None |
//!
//! A raw CSP report has no Sentry user or request environment. Relay forwards the
//! converted event as a `security` item, which can already carry `user.ip_address`.
//! Crash attachments can produce an error event. Legacy user reports have no IP field.
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
//! - With the default normalization level, a processing Relay skips City and ASN on events
//!   already fully normalized by a trusted Relay.
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
//!
//! ## Span enrichment
//!
//! - Read `client.address`, then `user.ip_address`, from transaction span data or standalone span attributes.
//!   An invalid address or one marked as modified blocks fallback.
//! - Standalone spans can request the connection IP with an explicit address marker (`{{auto}}`).
//! - A transaction segment may use the event IP when both span addresses are absent.
//!   Child spans need their own address for ASN, even when they inherit event user tags.
//! - Standalone spans use the same eligible address for City and ASN. Span PII rules
//!   apply to the derived attributes.

use std::net::IpAddr;

use relay_conventions::attributes::{
    CLIENT__ADDRESS, CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION, USER__GEO__CITY,
    USER__GEO__COUNTRY_CODE, USER__GEO__REGION, USER__GEO__SUBDIVISION, USER__GEO__SUBREGION,
    USER__IP_ADDRESS,
};
use relay_event_schema::processor::{ProcessingAction, ProcessingResult, ValueType};
use relay_event_schema::protocol::{
    Attributes, AutoInferSetting, ClientSdkInfo, Context, Event, EventType, Geo,
    IpAddr as ProtocolIp, ParsedIp, Request, SpanData, SpanV2, Tags, User,
};
use relay_pii::ProjectPii;
use relay_pii::UserIpRetention::{ApplyRules, HashOnly};
use relay_protocol::{Annotated, RemarkType, Value};

use self::Unavailable::{MarkedModified, Missing};
use crate::span::country_subregion::Subregion;
use crate::{AsnLookup, CityLookup, NormalizationConfig};

const ADDRESS_KEYS: [&str; 2] = [CLIENT__ADDRESS, USER__IP_ADDRESS];
const ASN_KEYS: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

/// Whether a remark records a change to the field, including a change to only part of its value.
fn has_modifying_remark<T>(field: &Annotated<T>) -> bool {
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
            geoip_lookup: config.geoip_lookup,
            infer_ip_address: config.infer_ip_address,
            security_report: event.csp.value().is_some(),
        },
        rules,
    )?;
    let Some(db) = config.asn_lookup else {
        return Ok(());
    };
    let tags_have_asn = has_asn(&event.tags, |tags, k| tags.0.contains(k));
    if event.ty.value() == Some(&EventType::Transaction) {
        enrich_transaction_asn(event, lookup, tags_have_asn, db, rules);
    } else if !tags_have_asn && let Some((number, org)) = lookup.and_then(|ip| db.lookup(ip)) {
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
        // Keep a concrete event IP, including one filled by an upstream Relay.
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

/// Normalizes the user's geographical information into [`Attributes`].
///
/// Does not modify the attributes if there is already user geo information present,
/// to preserve original values.
///
/// This uses [`CLIENT__ADDRESS`] or, when absent, [`USER__IP_ADDRESS`]. Run
/// [`crate::eap::normalize_client_address`] before [`normalize_user_geo`].
pub fn normalize_user_geo(
    attributes: &mut Annotated<Attributes>,
    info: impl FnOnce(IpAddr) -> Option<Geo>,
) {
    enrich_span_ip(attributes, Some(info), None);
}

/// Enrich missing City and ASN fields from the same eligible address.
pub fn normalize_span_ip(
    attributes: &mut Annotated<Attributes>,
    city: Option<&CityLookup>,
    asn: Option<&AsnLookup>,
) {
    enrich_span_ip(attributes, city.map(|db| move |ip| db.lookup(ip)), asn);
}

fn enrich_span_ip(
    attributes: &mut Annotated<Attributes>,
    city: Option<impl FnOnce(IpAddr) -> Option<Geo>>,
    asn: Option<&AsnLookup>,
) {
    if (city.is_none() && asn.is_none()) || has_modifying_remark(attributes) {
        return;
    }
    let asn = asn.filter(|_| !has_asn(attributes, |a, k| a.contains_key(k)));
    let Some(attributes) = attributes.value_mut() else {
        return;
    };
    let city = city.filter(|_| {
        ![
            USER__GEO__COUNTRY_CODE,
            USER__GEO__CITY,
            USER__GEO__SUBDIVISION,
            USER__GEO__REGION,
        ]
        .into_iter()
        .any(|key| attributes.contains_key(key))
    });
    if city.is_none() && asn.is_none() {
        return;
    }
    let Some(source @ Annotated(Some(attr), _)) = attributes
        .0
        .get(CLIENT__ADDRESS)
        .or_else(|| attributes.0.get(USER__IP_ADDRESS))
    else {
        return;
    };
    let value = &attr.value.value;
    if has_modifying_remark(source) || has_modifying_remark(value) {
        return;
    }
    let Some(ip) = value.as_str().and_then(|text| text.parse().ok()) else {
        return;
    };
    if let Some((number, org)) = asn.and_then(|db| db.lookup(ip)) {
        set_asn_fields(attributes, number, org);
    }
    if let Some(geo) = city.and_then(|lookup| lookup(ip)) {
        let subregion = geo
            .country_code
            .value()
            .and_then(|cc| Subregion::from_iso2(cc));

        attributes.insert(USER__GEO__COUNTRY_CODE, geo.country_code);
        attributes.insert(USER__GEO__CITY, geo.city);
        attributes.insert(USER__GEO__SUBDIVISION, geo.subdivision);
        attributes.insert(USER__GEO__REGION, geo.region);
        if let Some(subregion) = subregion {
            attributes.insert_if_missing(USER__GEO__SUBREGION, || (subregion as u8).to_string());
        }
    }
}

fn enrich_transaction_asn(
    event: &mut Event,
    ip: Option<IpAddr>,
    tags_have_asn: bool,
    db: &AsnLookup,
    rules: ProjectPii<'_>,
) {
    if !tags_have_asn
        && !has_asn(&event.measurements, |m, k| m.contains_key(k))
        && let Ok(contexts) = field_value_mut(&mut event.contexts)
        && let Some(trace) = contexts.0.get_mut("trace")
        && let Ok(context) = field_value_mut(trace)
        && let Context::Trace(trace) = &mut context.0
    {
        enrich_span_asn(&mut trace.data, ip, db, rules);
    }
    if let Ok(spans) = field_value_mut(&mut event.spans) {
        for span in spans.iter_mut().filter_map(|s| field_value_mut(s).ok()) {
            if !has_asn(&span.measurements, |m, k| m.contains_key(k))
                && !has_asn(&span.tags, |tags, k| tags.contains_key(k))
            {
                enrich_span_asn(&mut span.data, None, db, rules);
            }
        }
    }
}

fn enrich_span_asn(
    data: &mut Annotated<SpanData>,
    fallback: Option<IpAddr>,
    db: &AsnLookup,
    rules: ProjectPii<'_>,
) {
    if has_asn(data, SpanData::contains) {
        return;
    }
    let source = data
        .value()
        .and_then(|d| ADDRESS_KEYS.iter().find_map(|key| d.get(key)));
    let ip = match source {
        Some(field) => value_without_modifying_remark(field)
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok()),
        None => fallback,
    };
    if let Some((number, org)) = ip.and_then(|ip| db.lookup(ip)) {
        let mut fields = Attributes::new();
        set_asn_fields(&mut fields, number, org);
        data.get_or_insert_with(Default::default)
            .other
            .extend(scrubbed_span_fields(fields, rules));
    }
}

/// Preserve a scrubbed container or either ASN attribute supplied by the SDK.
fn has_asn<T>(field: &Annotated<T>, contains: impl Fn(&T, &str) -> bool) -> bool {
    has_modifying_remark(field)
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

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::{ProcessingState, process_value};
    use relay_pii::{PiiConfig, PiiProcessor};
    use relay_protocol::Remark;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_span_ip_source_eligibility() {
        let city = CityLookup::open("../tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        let db = AsnLookup::open("../tests/fixtures/GeoLite2-ASN-Test.mmdb").unwrap();
        let mut source = Attributes::new();
        source.insert(USER__IP_ADDRESS, "89.160.20.112");
        let mut user_only = Annotated::new(source.clone());
        normalize_span_ip(&mut user_only, Some(&city), Some(&db));
        assert!(user_only.value().unwrap().contains_key(USER__GEO__CITY));
        let mut city_only = Annotated::new(source.clone());
        normalize_span_ip(&mut city_only, Some(&city), None);
        assert!(city_only.value().unwrap().contains_key(USER__GEO__CITY));
        assert!(user_only.value().unwrap().contains_key(CLIENT__AS__NUMBER));

        // A redacted client address blocks both lookups, including fallback to the user IP.
        source.insert(CLIENT__ADDRESS, "2600::1");
        source
            .get_annotated_value_mut(CLIENT__ADDRESS)
            .unwrap()
            .meta_mut()
            .add_remark(Remark::new(RemarkType::Substituted, "test"));
        let mut attrs = Annotated::new(source);
        normalize_span_ip(&mut attrs, Some(&city), Some(&db));
        assert!(!attrs.value().unwrap().contains_key(CLIENT__AS__NUMBER));
        assert!(!attrs.value().unwrap().contains_key(USER__GEO__CITY));
    }

    #[test]
    fn test_event_ip_sources_without_connection() {
        // An HTTP integration test always has at least the loopback peer.
        let config = NormalizationConfig {
            infer_ip_address: true,
            ..Default::default()
        };
        for user in [None, Some("192.0.2.1")] {
            for remote in [None, Some("192.0.2.2")] {
                let payload = json!({
                    "platform": "javascript",
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

    #[test]
    fn test_transport_fallback_with_upstream_redaction() {
        let db = AsnLookup::open("../tests/fixtures/GeoLite2-ASN-Test.mmdb").unwrap();
        let config = NormalizationConfig {
            client_ip: Some("1.0.0.1".parse().unwrap()),
            infer_ip_address: true,
            asn_lookup: Some(&db),
            ..Default::default()
        };
        for selector in ["$request.env", "$request.env.REMOTE_ADDR"] {
            let rules: PiiConfig = serde_json::from_value(json!({
                "applications": { (selector): ["@anything:remove"] }
            }))
            .unwrap();
            let mut event = Annotated::<Event>::from_json(
                r#"{"platform":"javascript","request":{"env":{"REMOTE_ADDR":"2600::1"}}}"#,
            )
            .unwrap();
            process_value(
                &mut event,
                &mut PiiProcessor::new(rules.compiled()),
                ProcessingState::root(),
            )
            .unwrap();
            let mut event = event.into_value().unwrap();
            process_client_ip(&mut event, &config, Default::default()).unwrap();
            assert_eq!(
                event.user.value().and_then(|u| u.ip_address.as_str()),
                None,
                "{selector}"
            );
            assert_eq!(
                event.tags.value().and_then(|t| t.get(CLIENT__AS__NUMBER)),
                None,
                "{selector}"
            );
        }
    }

    #[test]
    fn test_has_modifying_remark() {
        let mut field = Annotated::<String>::empty();
        field
            .meta_mut()
            .add_remark(Remark::new(RemarkType::Annotated, "note"));
        assert!(!has_modifying_remark(&field));
        for ty in [
            RemarkType::Removed,
            RemarkType::Substituted,
            RemarkType::Masked,
            RemarkType::Pseudonymized,
            RemarkType::Encrypted,
        ] {
            let mut field = field.clone();
            field.meta_mut().add_remark(Remark::new(ty, "rule"));
            assert!(has_modifying_remark(&field), "{ty:?}");
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
