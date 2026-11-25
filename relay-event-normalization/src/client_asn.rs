//! Write client ASN fields on errors and spans.
use crate::AsnLookup;
use crate::client_ip::redacted;
use relay_conventions::attributes::{
    CLIENT__ADDRESS, CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION, USER__IP_ADDRESS,
};
use relay_event_schema::processor::ValueType;
use relay_event_schema::protocol::{
    Attributes, Event, EventType, Measurements, SpanData, SpanV2, Tags, TraceContext,
};
use relay_pii::ProjectPii;
use relay_protocol::Annotated;
use std::net::IpAddr;

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

fn lookup_fields(ip: IpAddr, db: &AsnLookup) -> Option<Attributes> {
    let (number, organization) = db.lookup(ip)?;
    let mut fields = Attributes::new();
    fields.insert(CLIENT__AS__NUMBER, i64::from(number));
    if let Some(org) = organization {
        fields.insert(CLIENT__AS__ORGANIZATION, org);
    }
    Some(fields)
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
        && let Some(fields) = lookup_fields(ip, db)
    {
        attrs.0.extend(fields.0);
    }
}

fn insert_span_asn(
    data: &mut Annotated<SpanData>,
    ip: IpAddr,
    db: &AsnLookup,
    rules: ProjectPii<'_>,
) {
    let Some(mut fields) = lookup_fields(ip, db) else {
        return;
    };
    // Transaction spans will be extracted after event-format scrubbing. Apply
    // span-format destination rules while this hop still has its project rules.
    if !rules.is_empty() {
        let mut span = Annotated::new(SpanV2 {
            attributes: Annotated::new(fields),
            ..Default::default()
        });
        if relay_pii::eap::scrub(ValueType::Span, &mut span, rules.advanced, rules.legacy).is_err()
        {
            return;
        }
        let Some(attrs) = span.into_value().and_then(|s| s.attributes.into_value()) else {
            return;
        };
        fields = attrs;
    }
    let data = data.get_or_insert_with(Default::default);
    for (key, attr) in fields {
        if let Some(attr) = attr.into_value() {
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

pub fn enrich_event_asn(
    event: &mut Event,
    ip: Option<IpAddr>,
    db: &AsnLookup,
    rules: ProjectPii<'_>,
) {
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
    if !has_tags
        && !has_trace
        && !has_measurement(&event.measurements)
        && let Some(trace) = event.context::<TraceContext>()
        && let Some(ip) = span_ip(&trace.data, |key| event.tags.value()?.0.get(key), ip)
        && let Some(trace) = event.context_mut::<TraceContext>()
    {
        insert_span_asn(&mut trace.data, ip, db, rules);
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
            insert_span_asn(&mut span.data, ip, db, rules);
        }
    }
}
