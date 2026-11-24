//! Insert generated fields before scrubbing so rules targeting the ASN can act on them.
//! A rule targeting only the source IP can leave those fields intact. After scrubbing,
//! discard the generated fields if the original IP was removed, replaced, or masked.

use std::net::IpAddr;

use relay_conventions::attributes::{
    CLIENT__ADDRESS, CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION, USER__IP_ADDRESS,
};
use relay_dynamic_config::Feature;
use relay_event_normalization::AsnLookup;
use relay_event_schema::processor::{ProcessingAction, ValueType};
use relay_event_schema::protocol::{
    Attributes, Event, Measurements, Span, SpanData, SpanV2, TraceContext,
};
use relay_protocol::{Annotated, Empty, Value};

use crate::processing::Context;
use crate::services::processor::ProcessingError;

const ASN_KEYS: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

fn database(ctx: Context<'_>) -> Option<&AsnLookup> {
    let db = ctx.asn_lookup?;
    (ctx.is_processing()
        && ctx.project_info.has_feature(Feature::ClientAsnLookup)
        && !ctx
            .project_info
            .config
            .datascrubbing_settings
            .scrub_ip_addresses)
        .then_some(db)
}

// Only a configured PII processor can change these source fields during scrubbing.
fn ip_to_recheck(ip: IpAddr, ctx: Context<'_>) -> Option<IpAddr> {
    let config = &ctx.project_info.config;
    (config.pii_config.is_some() || config.datascrubbing_settings.pii_config().is_some())
        .then_some(ip)
}

fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().next().is_some()
}

fn user_ip(event: &Event) -> Option<&str> {
    // Normalization can backfill user.ip_address from request.env.REMOTE_ADDR.
    if redacted(&event.request)
        || event.request.value().is_some_and(|request| {
            redacted(&request.env)
                || request
                    .env
                    .value()
                    .and_then(|env| env.get("REMOTE_ADDR"))
                    .is_some_and(redacted)
        })
    {
        return None;
    }
    let user = event.user.value()?;
    if redacted(&event.user) || redacted(&user.ip_address) {
        return None;
    }
    Some(user.ip_address.value()?.as_str())
}

enum SpanSource<'a> {
    Standalone(&'a Attributes),
    Segment(&'a Event),
    Child(&'a Span),
}

struct SpanAddress<'a> {
    key: &'static str,
    text: &'a str,
    ip: IpAddr,
}

/// Selects the same source for lookup and for the check after scrubbing.
fn span_address(source: SpanSource<'_>) -> Option<SpanAddress<'_>> {
    let data = match source {
        SpanSource::Standalone(_) => None,
        SpanSource::Segment(event) => {
            let data = &event.context::<TraceContext>()?.data;
            if redacted(data) || redacted(&event.tags) {
                return None;
            }
            data.value()
        }
        SpanSource::Child(span) => {
            if redacted(&span.data) || redacted(&span.tags) {
                return None;
            }
            span.data.value()
        }
    };

    let mut selected = None;
    for key in [CLIENT__ADDRESS, USER__IP_ADDRESS] {
        let (text, scrubbed) = if let Some(value) = data.and_then(|data| data.get(key)) {
            (value.as_str(), redacted(value))
        } else {
            match source {
                SpanSource::Standalone(attrs) => {
                    let Some(entry) = attrs.0.get(key) else {
                        continue;
                    };
                    let value = &entry.value()?.value.value;
                    (value.as_str(), redacted(entry) || redacted(value))
                }
                SpanSource::Segment(event) => {
                    let Some(tag) = event.tags.value().and_then(|tags| tags.0.get(key)) else {
                        continue;
                    };
                    (tag.as_str(), redacted(tag))
                }
                SpanSource::Child(span) => {
                    let Some(tag) = span.tags.value().and_then(|tags| tags.get(key)) else {
                        continue;
                    };
                    (tag.as_str(), redacted(tag))
                }
            }
        };
        // A redaction on either alias vetoes lookup, even if we already selected an IP.
        if scrubbed {
            return None;
        }
        if selected.is_none() {
            selected = Some((key, text?));
        }
    }

    let (key, text) = match (selected, source) {
        (Some(address), _) => address,
        (None, SpanSource::Segment(event)) => (USER__IP_ADDRESS, user_ip(event)?),
        _ => return None,
    };
    Some(SpanAddress {
        key,
        text,
        ip: text.parse().ok()?,
    })
}

pub fn enrich_span(span: &mut Annotated<SpanV2>, ctx: Context<'_>) -> Option<IpAddr> {
    let db = database(ctx)?;
    let attrs = span.value_mut().as_mut()?.attributes.value_mut().as_mut()?;
    if ASN_KEYS.iter().any(|key| attrs.contains_key(*key)) {
        return None;
    }
    let ip = span_address(SpanSource::Standalone(attrs))?.ip;
    let (number, organization) = db.lookup(ip)?;
    attrs.insert(CLIENT__AS__NUMBER, i64::from(number));
    if let Some(organization) = organization {
        attrs.insert(CLIENT__AS__ORGANIZATION, organization);
    }
    ip_to_recheck(ip, ctx)
}

pub fn remove_from_span_if_ip_scrubbed(span: &mut Annotated<SpanV2>, source: Option<IpAddr>) {
    if let Some(ip) = source
        && let Some(attrs) = span
            .value_mut()
            .as_mut()
            .and_then(|s| s.attributes.value_mut().as_mut())
        && span_address(SpanSource::Standalone(attrs)).map(|address| address.ip) != Some(ip)
    {
        for key in ASN_KEYS {
            attrs.remove(key);
        }
    }
}

pub fn enrich_error(event: &mut Annotated<Event>, ctx: Context<'_>) -> Option<IpAddr> {
    let db = database(ctx)?;
    let event = event.value_mut().as_mut()?;
    if has_asn_tags(event)
        || event
            .context::<TraceContext>()
            .and_then(|trace| trace.data.value())
            .is_some_and(has_asn_data)
    {
        return None;
    }
    let ip = user_ip(event)?.parse().ok()?;
    let (number, organization) = db.lookup(ip)?;
    let tags = event.tags.get_or_insert_with(Default::default);
    tags.0.insert(
        CLIENT__AS__NUMBER.into(),
        Annotated::new(number.to_string()),
    );
    if let Some(organization) = organization {
        tags.0.insert(
            CLIENT__AS__ORGANIZATION.into(),
            Annotated::new(organization.chars().take(200).collect()),
        );
    }
    ip_to_recheck(ip, ctx)
}

pub fn remove_from_error_if_ip_scrubbed(event: &mut Annotated<Event>, source: Option<IpAddr>) {
    if let Some(ip) = source
        && let Some(event) = event.value_mut()
        && user_ip(event).and_then(|s| s.parse().ok()) != Some(ip)
        && let Some(tags) = event.tags.value_mut()
    {
        for key in ASN_KEYS {
            tags.0.remove(key);
        }
    }
}

fn has_asn_tags(event: &Event) -> bool {
    redacted(&event.tags)
        || event
            .tags
            .value()
            .is_some_and(|tags| ASN_KEYS.iter().any(|key| tags.0.contains(key)))
}

fn has_asn_data(data: &SpanData) -> bool {
    ASN_KEYS.iter().any(|key| data.contains(key))
}

fn has_asn_measurement(measurements: &Annotated<Measurements>) -> bool {
    measurements
        .value()
        .is_some_and(|m| ASN_KEYS.iter().any(|key| m.contains_key(*key)))
}

struct TransactionAsn {
    ip_to_recheck: Option<IpAddr>,
    number: Annotated<Value>,
    organization: Annotated<Value>,
}

impl TransactionAsn {
    fn insert_into(self, target: &mut Annotated<SpanData>) -> Option<IpAddr> {
        let data = target.get_or_insert_with(Default::default);
        if !self.number.is_empty() {
            data.insert(CLIENT__AS__NUMBER, self.number);
        }
        if !self.organization.is_empty() {
            data.insert(CLIENT__AS__ORGANIZATION, self.organization);
        }
        self.ip_to_recheck
    }
}

// Extracted transaction spans bypass the standalone span scrubber. Only the
// source and the two generated fields need to see the V2 selectors as well.
fn transaction_fields(
    db: &AsnLookup,
    source: SpanSource<'_>,
    ctx: Context<'_>,
) -> Result<Option<TransactionAsn>, ProcessingAction> {
    let Some(SpanAddress { key, text, ip }) = span_address(source) else {
        return Ok(None);
    };
    let Some((number, organization)) = db.lookup(ip) else {
        return Ok(None);
    };
    let config = &ctx.project_info.config;
    let legacy = config.datascrubbing_settings.pii_config();
    if config.pii_config.is_none() && legacy.is_none() {
        return Ok(Some(TransactionAsn {
            ip_to_recheck: None,
            number: Annotated::new(i64::from(number).into()),
            organization: Annotated::from(organization.map(|s| Value::String(s.to_owned()))),
        }));
    }

    let mut attrs = Attributes::new();
    attrs.insert(key, text);
    attrs.insert(CLIENT__AS__NUMBER, i64::from(number));
    if let Some(organization) = organization {
        attrs.insert(CLIENT__AS__ORGANIZATION, organization);
    }
    let mut projection = Annotated::new(SpanV2 {
        attributes: Annotated::new(attrs),
        ..Default::default()
    });
    relay_pii::eap::scrub(
        ValueType::Span,
        &mut projection,
        config.pii_config.as_ref(),
        legacy.as_ref(),
    )?;
    let Some(mut attrs) = projection
        .into_value()
        .and_then(|s| s.attributes.into_value())
    else {
        return Ok(None);
    };
    if attrs
        .get_annotated_value(key)
        .is_none_or(|v| redacted(v) || v.value().is_none())
    {
        return Ok(None);
    }
    let [number, organization] = ASN_KEYS.map(|key| {
        attrs
            .remove(key)
            .and_then(Annotated::into_value)
            .map(|a| a.value.value)
            .unwrap_or_default()
    });
    Ok(
        (!number.is_empty() || !organization.is_empty()).then_some(TransactionAsn {
            ip_to_recheck: Some(ip),
            number,
            organization,
        }),
    )
}

/// Tracks generated fields only when a PII processor runs. Scrubbing can clear an
/// annotated child, but keeps its array index until serialization.
#[derive(Default)]
pub struct TransactionSources {
    segment: Option<IpAddr>,
    children: Vec<(usize, IpAddr)>,
}

pub fn enrich_transaction(
    event: &mut Annotated<Event>,
    ctx: Context<'_>,
) -> Result<TransactionSources, ProcessingError> {
    let mut sources = TransactionSources::default();
    let Some(db) = database(ctx) else {
        return Ok(sources);
    };
    let Some(event) = event.value_mut() else {
        return Ok(sources);
    };
    if !has_asn_tags(event)
        && !has_asn_measurement(&event.measurements)
        && let Some(trace) = event.context::<TraceContext>()
        && !trace.data.value().is_some_and(has_asn_data)
        && let Some(asn) = transaction_fields(db, SpanSource::Segment(event), ctx)?
        && let Some(trace) = event.context_mut::<TraceContext>()
    {
        sources.segment = asn.insert_into(&mut trace.data);
    }
    for (index, span) in event.spans.value_mut().iter_mut().flatten().enumerate() {
        let Some(span) = span.value_mut() else {
            continue;
        };
        if !span
            .tags
            .value()
            .is_some_and(|t| ASN_KEYS.iter().any(|key| t.contains_key(*key)))
            && !has_asn_measurement(&span.measurements)
            && !span.data.value().is_some_and(has_asn_data)
            && let Some(asn) = transaction_fields(db, SpanSource::Child(span), ctx)?
            && let Some(ip) = asn.insert_into(&mut span.data)
        {
            sources.children.push((index, ip));
        }
    }
    Ok(sources)
}

fn remove_asn(data: &mut Annotated<SpanData>) {
    if let Some(data) = data.value_mut() {
        for key in ASN_KEYS {
            data.remove(key);
        }
    }
}

impl TransactionSources {
    pub fn remove_if_ip_scrubbed(self, event: &mut Annotated<Event>) {
        let Some(event) = event.value_mut() else {
            return;
        };
        if let Some(ip) = self.segment
            && span_address(SpanSource::Segment(event)).map(|address| address.ip) != Some(ip)
            && let Some(trace) = event.context_mut::<TraceContext>()
        {
            remove_asn(&mut trace.data);
        }
        if let Some(spans) = event.spans.value_mut() {
            for (index, ip) in self.children {
                if let Some(span) = spans.get_mut(index).and_then(|s| s.value_mut().as_mut())
                    && span_address(SpanSource::Child(span)).map(|address| address.ip) != Some(ip)
                {
                    remove_asn(&mut span.data);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_conventions::attributes::{CLIENT__ADDRESS, USER__IP_ADDRESS};
    use relay_event_schema::protocol::Attributes;
    use relay_protocol::{Remark, RemarkType};

    use super::{SpanSource, span_address};

    #[test]
    fn test_span_address_precedence() {
        let mut attrs = Attributes::new();
        attrs.insert(USER__IP_ADDRESS, "192.0.2.1");
        attrs.insert(CLIENT__ADDRESS, "2600::1");
        let address = span_address(SpanSource::Standalone(&attrs)).unwrap();
        assert_eq!((address.key, address.text), (CLIENT__ADDRESS, "2600::1"));

        attrs.insert(CLIENT__ADDRESS, "invalid");
        assert!(span_address(SpanSource::Standalone(&attrs)).is_none());

        attrs.remove(CLIENT__ADDRESS);
        let address = span_address(SpanSource::Standalone(&attrs)).unwrap();
        assert_eq!((address.key, address.text), (USER__IP_ADDRESS, "192.0.2.1"));
    }

    #[test]
    fn test_span_address_redacted_alias() {
        let mut attrs = Attributes::new();
        attrs.insert(CLIENT__ADDRESS, "192.0.2.1");
        attrs.insert(USER__IP_ADDRESS, "192.0.2.2");
        let alias = attrs.0.get_mut(USER__IP_ADDRESS).unwrap();
        alias
            .meta_mut()
            .add_remark(Remark::new(RemarkType::Substituted, "@ip"));
        assert!(span_address(SpanSource::Standalone(&attrs)).is_none());

        attrs.0.get_mut(USER__IP_ADDRESS).unwrap().set_value(None);
        assert!(span_address(SpanSource::Standalone(&attrs)).is_none());
    }
}
